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

/**
 * A ExecutorLooper, implementing WakeableLooper, is a class wrapping object wait()/notify() to await/unblock a thread.
 * It extends WakeableLooper, so it will execute in a while loop unless the exitLoop() is called.
 * And in every execution, in tasksOnWakeup(), it will do nothing by default
 * The ExecutorLooper should start by calling {@code loop()}
 */

public class ExecutorLooper extends WakeableLooper {
  // The lock to implement the await/unblock
  private final RunnableLock lock;

  public ExecutorLooper() {
    this.lock = new RunnableLock();
  }

  @Override
  protected void doWait() {
    synchronized (lock.proceedLock) {
      while (!lock.isToProceed) {

        // If timer task exists, the doWait() should wait not later than the time timer to execute
        // It no timer exists, we consider it will wait forever until other threads call wakeUp()
        // The nextTimeoutIntervalMs is in milli-seconds
        Duration nextTimeoutInterval = getNextTimeoutInterval();

        // In fact, only when the timeout > 0 (no timer should be executed before now)
        // or no wakeUp() is called during the thread's run, will the thread wait().
        if (nextTimeoutInterval.toMillis() > 0) {
          try {
            lock.isWaiting = true;
            // The wait will take the timeout in unit of milli-seconds
            lock.proceedLock.wait(nextTimeoutInterval.toMillis());
          } catch (InterruptedException e) {
            e.printStackTrace();
          } finally {
            lock.isWaiting = false;
          }
        } else {
          // break the loop if timeout happens
          break;
        }
      }
      lock.isToProceed = false;
    }
  }

  @Override
  public void wakeUp() {
    // In fact, we are using the wait()/notify() to implement the blocking thread here
    if (!lock.isToProceed) {
      synchronized (lock.proceedLock) {
        lock.isToProceed = true;
        if (lock.isWaiting) {
          lock.proceedLock.notify();
        }
      }
    }
  }

  //The lock used to await/unblock the thread
  private static final class RunnableLock {
    private Object proceedLock;
    private volatile boolean isToProceed;

    // Are we doing a wait() on proceedLock.
    private volatile boolean isWaiting;

    RunnableLock() {
      this.proceedLock = new Object();
      isToProceed = false;
      isWaiting = false;
    }
  }
}
