/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.heron.common.basics;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;

/**
 * A WakeableLooper is a class that could:
 * Block the thread when doWait() is called and unblock
 * when the wakeUp() is called or the waiting time exceeds the timeout.
 * It could execute timer event
 * <p>
 * The WakeableLooper will execute in a while loop unless the exitLoop() is called. And in every
 * execution, it will execute runOnce(), which will:
 * 1. doWait(), which in fact is implemented by selector.select(timeout), and it will be wake up if other threads
 * wake it up, it meets the timeout, one channel is selected, or the current thread is interrupted.
 * 2. run executeTasksOnWakeup(), which is a list of Runnable, e.g. tasks added to run every time. We could add tasks
 * even during executionTasksOnWakeup, but the task added will be executed next time we run executeTasksOnwakeup().
 * Notice: you could just add tasks into it but not remove tasks from it.
 * 3. trigger the timers, which is a priority queue of {@code TimerTask}, the {@code TimerTask}
 * will be removed after execution.
 * <p>
 * So to use this class, user could add the persistent tasks, one time tasks and timer tasks as many
 * as they want.
 * Most methods except the onExit() are not thread-safe.
 * People should handle the concurrent scenarios in their business logic rather than in this class.
 */
public abstract class WakeableLooper {
  // The tasks could only be added but not removed
  private final List<Runnable> tasksOnWakeup;
  private final PriorityQueue<TimerTask> timers;

  // The tasks would be invoked before exit
  private final List<Runnable> exitTasks;

  // For selector since there is bug in selector.select(timeout): we could not
  // use a timeout > 10 * Integer.MAX_VALUE
  // So here we set Integer.MAX_VALUE as the infinite future
  // We will also multiple 1000*1000 to convert mill-seconds to nano-seconds
  private static final Duration INFINITE_FUTURE = Duration.ofMillis(Integer.MAX_VALUE);
  private volatile boolean exitLoop;
  // this boolean is set when the tasksOnWakeup list is cleared.
  // this boolean is need if it is one of the tasks in taskOnWakeup that clears the list
  private boolean terminateAllTasksOnWakeup;
  // this boolean is set when the exitTasks list is cleared.
  // this boolean is need if it is one of the tasks in exitTask that clears the list
  private boolean terminateAllExitTasks;

  public WakeableLooper() {
    exitLoop = false;
    tasksOnWakeup = new ArrayList<>();
    timers = new PriorityQueue<TimerTask>();
    exitTasks = new ArrayList<>();
    terminateAllTasksOnWakeup = false;
    terminateAllExitTasks = false;
  }

  public void clear() {
    clearTasksOnWakeup();
    clearTimers();
    clearExitTasks();
  }

  public void clearTasksOnWakeup() {
    tasksOnWakeup.clear();
    terminateAllTasksOnWakeup = true;
  }

  public void clearTimers() {
    timers.clear();
  }

  public void clearExitTasks() {
    exitTasks.clear();
    terminateAllExitTasks = true;
  }

  public void loop() {
    while (!exitLoop) {
      runOnce();
    }

    // Invoke the exit tasks
    onExit();
  }

  private void runOnce() {
    doWait();

    executeTasksOnWakeup();

    triggerExpiredTimers(System.nanoTime());
  }

  private void onExit() {
    int s = exitTasks.size();
    for (int i = 0; i < s; i++) {
      // this flag is not thread safe!
      if (terminateAllExitTasks) {
        break;
      }
      exitTasks.get(i).run();
    }
    terminateAllExitTasks = false;
  }

  protected abstract void doWait();

  public abstract void wakeUp();

  public void addTasksOnWakeup(Runnable task) {
    tasksOnWakeup.add(task);
    // We need to wake up the looper itself when we add a new task, otherwise, it is possible
    // this task will never be executed due to the looper will never be wake up.
    wakeUp();
  }

  public void addTasksOnExit(Runnable task) {
    exitTasks.add(task);
  }

  public void registerTimerEvent(Duration timerDuration, Runnable task) {
    assert timerDuration.getSeconds() >= 0;
    assert task != null;
    Duration expiration = timerDuration.plusNanos(System.nanoTime());
    timers.add(new TimerTask(expiration, task));
  }

  public void registerPeriodicEvent(Duration frequency, Runnable task) {
    registerTimerEvent(frequency, new Runnable() {
      @Override
      public void run() {
        task.run();
        registerPeriodicEvent(frequency, task);
      }
    });
  }

  public void exitLoop() {
    exitLoop = true;
    wakeUp();
  }

  /**
   * Get the timeout which should be used in doWait().
   *
   * @return INFINITE_FUTURE : if there are no timer events
   * or the time to next timer event in milli-second
   */
  protected Duration getNextTimeoutInterval() {
    Duration nextTimeoutInterval = INFINITE_FUTURE;
    if (!timers.isEmpty()) {
      // The time recorded in timer is in nano-seconds. We have to convert it to milli-seconds
      // We need to ceil the result to avoid early wake up
      nextTimeoutInterval = timers.peek().expirationTime.minusNanos(System.nanoTime());
    }
    return nextTimeoutInterval;
  }

  private void executeTasksOnWakeup() {
    // Be careful here we could not use iterator, since it is possible that we may
    // add some items into this list during the iteration, which may cause
    // ConcurrentModificationException
    // We pre-get the size to avoid execute the tasks added during execution
    int s = tasksOnWakeup.size();
    for (int i = 0; i < s; i++) {
      // this flag is not thread safe
      if (terminateAllTasksOnWakeup) {
        break;
      }
      tasksOnWakeup.get(i).run();
    }
    terminateAllTasksOnWakeup = false;
  }

  private void triggerExpiredTimers(long currentTime) {
    // Executes the task should be executed no later than current time
    while (!timers.isEmpty()) {
      long nextExpiredTime = timers.peek().expirationTime.toNanos();
      if (nextExpiredTime - currentTime <= 0) {
        timers.poll().handler.run();
      } else {
        return;
      }
    }
  }

  /**
   * A TimerTask will has the runnable, and expirationTime to indicate when it will be executed.
   * The expirationTime is the Duration until expiry should occur.
   */
  private static class TimerTask implements Comparable<TimerTask> {
    private final Duration expirationTime;
    private final Runnable handler;

    TimerTask(Duration expirationTime, Runnable handler) {
      this.expirationTime = expirationTime;
      this.handler = handler;
    }

    @Override
    public int compareTo(TimerTask other) {
      return this.expirationTime.compareTo(other.expirationTime);
    }

    @Override
    public boolean equals(Object other) {
      throw new RuntimeException("TODO: implement");
    }

    @Override
    public int hashCode() {
      throw new RuntimeException("TODO: implement");
    }
  }
}
