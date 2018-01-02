// Copyright 2016 Twitter. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.twitter.heron.common.basics;

import java.lang.Thread;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * WakeableLooper Tester.
 */
public class WakeableLooperTest {
  private volatile static int globalValue;
  private WakeableLooper slaveLooper;

  @Before
  public void before() {
    slaveLooper = new SlaveLooper();
    globalValue = 6;
  }

  @After
  public void after() {
    slaveLooper = null;
  }

  class LooperThread extends Thread {
    private WakeableLooper looper;

    LooperThread(WakeableLooper looper) {
      super();
      this.looper = looper;
    }

    public void run() {
      looper.loop();
    }
  }

  /**
   * Method: loop()
   */
  @Test
  public void testLoop() {
    Runnable r = new Runnable() {
      private int i = 3;

      @Override
      public void run() {
        globalValue += 10;
        slaveLooper.wakeUp();
        i--;
        if (i == 0) {
          slaveLooper.exitLoop();
        }
      }
    };
    slaveLooper.addTasksOnWakeup(r);
    slaveLooper.loop();
    Assert.assertEquals(36, globalValue);
  }

  /**
   * Method: addTasksOnWakeup(Runnable task)
   */
  @Test
  public void testAddTasksOnWakeup() {
    Runnable r = new Runnable() {
      @Override
      public void run() {
        slaveLooper.exitLoop();
        globalValue = 10;
      }
    };
    slaveLooper.addTasksOnWakeup(r);
    slaveLooper.loop();
    Assert.assertEquals(10, globalValue);
  }

  /**
   * Method: registerTimerEventInSeconds(long timerInSeconds, Runnable task)
   */
  @Test
  public void testRegisterTimerEventInSeconds() {
    Runnable r = new Runnable() {
      @Override
      public void run() {
        slaveLooper.exitLoop();
        globalValue = 10;
      }
    };

    long startTime = System.nanoTime();
    Duration interval = Duration.ofSeconds(1);
    slaveLooper.registerTimerEvent(interval, r);
    slaveLooper.loop();
    long endTime = System.nanoTime();
    Assert.assertTrue(endTime - startTime - interval.toNanos() >= 0);
    Assert.assertEquals(10, globalValue);
  }

  /**
   * Method: registerTimerEventInNanoSeconds(long timerInNanoSecnods, Runnable task)
   */
  @Test
  public void testRegisterTimerEventInNanoSeconds() {
    Runnable r = new Runnable() {
      @Override
      public void run() {
        slaveLooper.exitLoop();
        globalValue = 10;
      }
    };

    long startTime = System.nanoTime();
    Duration interval = Duration.ofMillis(6);
    slaveLooper.registerTimerEvent(interval, r);
    slaveLooper.loop();
    long endTime = System.nanoTime();
    Assert.assertTrue(endTime - startTime - interval.toNanos() >= 0);
    Assert.assertEquals(10, globalValue);
  }

  /**
   * Method: exitLoop()
   */
  @Test
  public void testExitLoop() {
    Runnable r = new Runnable() {
      @Override
      public void run() {
        slaveLooper.exitLoop();
        globalValue = 10;
      }
    };
    slaveLooper.addTasksOnWakeup(r);
    slaveLooper.loop();
    Assert.assertEquals(10, globalValue);
  }


  /**
   * Method: waitForExit()
   */
  @Test
  public void testWaitForExit() {
    int sleepTimeMS = 200;
    Runnable r = new Runnable() {
      @Override
      public void run() {
        try {
          slaveLooper.exitLoop();  // Exit after the first wake up
          Thread.sleep(sleepTimeMS);
          globalValue = 10;
        } catch (InterruptedException e) {
          return;
        }
      }
    };
    LooperThread looperThread = new LooperThread(slaveLooper);
    looperThread.start();
    long startTime = System.nanoTime();
    slaveLooper.addTasksOnWakeup(r);
    // Wait for it to finish.
    boolean ret = slaveLooper.waitForExit(sleepTimeMS * 2, TimeUnit.MILLISECONDS);
    long endTime = System.nanoTime();

    Assert.assertTrue(ret);
    Assert.assertTrue(endTime - startTime >= sleepTimeMS * 1000);
    Assert.assertEquals(10, globalValue);
  }

  @Test
  public void testWaitForExitTimeout() {
    int sleepTimeMS = 200;
    Runnable r = new Runnable() {
      @Override
      public void run() {
        try {
          slaveLooper.exitLoop(); // Exit after the first wake up
          Thread.sleep(sleepTimeMS);
          globalValue = 10;
        } catch (InterruptedException e) {
          return;
        }
      }
    };
    LooperThread looperThread = new LooperThread(slaveLooper);
    looperThread.start();
    long startTime = System.nanoTime();
    slaveLooper.addTasksOnWakeup(r);
    // Wait for it to finish.
    boolean ret = slaveLooper.waitForExit(sleepTimeMS / 10, TimeUnit.MILLISECONDS);
    long endTime = System.nanoTime();

    Assert.assertFalse(ret);
    Assert.assertEquals(6, globalValue);
  }

  /**
   * Method: getNextTimeoutInterval()
   */
  @Test
  public void testGetNextTimeoutIntervalMs()
      throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
    Runnable r = new Runnable() {
      @Override
      public void run() {
        slaveLooper.exitLoop();
        globalValue = 10;
      }
    };

    Duration interval = Duration.ofSeconds(6);
    slaveLooper.registerTimerEvent(interval, r);

    Method method =
        slaveLooper.getClass().getSuperclass().getDeclaredMethod("getNextTimeoutInterval");
    method.setAccessible(true);
    Duration res = (Duration) method.invoke(slaveLooper);

    Assert.assertNotNull(res);

    Assert.assertTrue(res.compareTo(interval) <= 0 && res.compareTo(interval.dividedBy(2)) > 0);
  }

  /**
   * Method: runOnce()
   */
  @Test
  public void testRunOnce()
      throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
    Runnable r = new Runnable() {
      @Override
      public void run() {
        globalValue = 10;
      }
    };
    slaveLooper.addTasksOnWakeup(r);

    Method method = slaveLooper.getClass().getSuperclass().getDeclaredMethod("runOnce");
    method.setAccessible(true);
    method.invoke(slaveLooper);

    Assert.assertEquals(10, globalValue);
  }

  /**
   * Method: executeTasksOnWakeup()
   */
  @Test
  public void testExecuteTasksOnWakeup()
      throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
    Runnable r = new Runnable() {
      @Override
      public void run() {
        globalValue = 10;
      }
    };
    slaveLooper.addTasksOnWakeup(r);

    Method method =
        slaveLooper.getClass().getSuperclass().getDeclaredMethod("executeTasksOnWakeup");
    method.setAccessible(true);
    method.invoke(slaveLooper);

    Assert.assertEquals(10, globalValue);
  }

  /**
   * Method: triggerExpiredTimers(long currentTime)
   */
  @Test
  public void testTriggerExpiredTimers()
      throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
    Runnable r = new Runnable() {
      @Override
      public void run() {
        globalValue = 10;
      }
    };

    Duration interval = Duration.ofNanos(1);
    slaveLooper.registerTimerEvent(interval, r);

    Method method =
        slaveLooper.getClass().getSuperclass().getDeclaredMethod(
            "triggerExpiredTimers", long.class);
    long current = System.nanoTime();
    method.setAccessible(true);
    method.invoke(slaveLooper, current);

    Assert.assertEquals(10, globalValue);
  }
}

