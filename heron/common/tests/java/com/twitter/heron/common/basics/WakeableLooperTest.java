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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * WakeableLooper Tester.
 */
public class WakeableLooperTest {
  private static int globalValue;
  private static final long SECONDS_TO_NANOSECONDS = 1000 * 1000 * 1000;
  private WakeableLooper slaveLooper;

  @Before
  public void before() throws Exception {
    slaveLooper = new SlaveLooper();
    globalValue = 6;
  }

  @After
  public void after() throws Exception {
    slaveLooper = null;
  }

  /**
   * Method: loop()
   */
  @Test
  public void testLoop() throws Exception {
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
  public void testAddTasksOnWakeup() throws Exception {
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
  public void testRegisterTimerEventInSeconds() throws Exception {
    Runnable r = new Runnable() {
      @Override
      public void run() {
        slaveLooper.exitLoop();
        globalValue = 10;
      }
    };

    long startTime = System.nanoTime();
    int intervalSeconds = 1;
    slaveLooper.registerTimerEventInSeconds(intervalSeconds, r);
    slaveLooper.loop();
    long endTime = System.nanoTime();
    Assert.assertTrue(endTime - startTime - (long) intervalSeconds * SECONDS_TO_NANOSECONDS >= 0);
    Assert.assertEquals(10, globalValue);
  }

  /**
   * Method: registerTimerEventInNanoSeconds(long timerInNanoSecnods, Runnable task)
   */
  @Test
  public void testRegisterTimerEventInNanoSeconds() throws Exception {
    Runnable r = new Runnable() {
      @Override
      public void run() {
        slaveLooper.exitLoop();
        globalValue = 10;
      }
    };

    long startTime = System.nanoTime();
    int intervalNs = 6 * 1000 * 1000;
    slaveLooper.registerTimerEventInNanoSeconds(intervalNs, r);
    slaveLooper.loop();
    long endTime = System.nanoTime();
    Assert.assertTrue(endTime - startTime - intervalNs >= 0);
    Assert.assertEquals(10, globalValue);
  }

  /**
   * Method: exitLoop()
   */
  @Test
  public void testExitLoop() throws Exception {
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
   * Method: getNextTimeoutIntervalMs()
   */
  @Test
  public void testGetNextTimeoutIntervalMs() throws Exception {
    Runnable r = new Runnable() {
      @Override
      public void run() {
        slaveLooper.exitLoop();
        globalValue = 10;
      }
    };

    long intervalNs = 6L * 1000 * 1000 * 1000;
    slaveLooper.registerTimerEventInNanoSeconds(intervalNs, r);

    long res = 1000;

    try {
      Method method =
          slaveLooper.getClass().getSuperclass().getDeclaredMethod("getNextTimeoutIntervalMs");
      method.setAccessible(true);
      res = (Long) method.invoke(slaveLooper) * 1000 * 1000;
    } catch (NoSuchMethodException e) {
      e.printStackTrace();
    } catch (IllegalAccessException e) {
      e.printStackTrace();
    } catch (InvocationTargetException e) {
      e.printStackTrace();
    }

    Assert.assertNotNull(res);

    Assert.assertTrue(res <= intervalNs && res > intervalNs / 2);
  }

  /**
   * Method: runOnce()
   */
  @Test
  public void testRunOnce() throws Exception {
    Runnable r = new Runnable() {
      @Override
      public void run() {
        globalValue = 10;
      }
    };
    slaveLooper.addTasksOnWakeup(r);

    try {
      Method method = slaveLooper.getClass().getSuperclass().getDeclaredMethod("runOnce");
      method.setAccessible(true);
      method.invoke(slaveLooper);
    } catch (NoSuchMethodException e) {
    } catch (IllegalAccessException e) {
    } catch (InvocationTargetException e) {
    }

    Assert.assertEquals(10, globalValue);
  }

  /**
   * Method: executeTasksOnWakeup()
   */
  @Test
  public void testExecuteTasksOnWakeup() throws Exception {
    Runnable r = new Runnable() {
      @Override
      public void run() {
        globalValue = 10;
      }
    };
    slaveLooper.addTasksOnWakeup(r);

    try {
      Method method =
          slaveLooper.getClass().getSuperclass().getDeclaredMethod("executeTasksOnWakeup");
      method.setAccessible(true);
      method.invoke(slaveLooper);
    } catch (NoSuchMethodException e) {
    } catch (IllegalAccessException e) {
    } catch (InvocationTargetException e) {
    }

    Assert.assertEquals(10, globalValue);
  }

  /**
   * Method: triggerExpiredTimers(long currentTime)
   */
  @Test
  public void testTriggerExpiredTimers() throws Exception {
    Runnable r = new Runnable() {
      @Override
      public void run() {
        globalValue = 10;
      }
    };

    int intervalNs = 1;
    slaveLooper.registerTimerEventInNanoSeconds(intervalNs, r);

    try {
      Method method =
          slaveLooper.getClass().getSuperclass().getDeclaredMethod(
              "triggerExpiredTimers", long.class);
      long current = System.nanoTime();
      method.setAccessible(true);
      method.invoke(slaveLooper, current);
    } catch (NoSuchMethodException e) {
    } catch (IllegalAccessException e) {
    } catch (InvocationTargetException e) {
    }

    Assert.assertEquals(10, globalValue);
  }
}

