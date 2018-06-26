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

package org.apache.heron.spi.metricsmgr.metrics;

import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ExceptionInfoTest {
  private static final int N = 100;
  private static final String STACK_TRACE = "stackTrace";
  private static final String LAST_TIME = "lastTime";
  private static final String FIRST_TIME = "firstTime";
  private static final String LOGGING = "logging";
  private final List<ExceptionInfo> exceptionInfos = new ArrayList<ExceptionInfo>();


  @Before
  public void before() throws Exception {
    exceptionInfos.clear();
    for (int i = 0; i < N; i++) {
      String stackTrace = STACK_TRACE + i;
      String lastTime = LAST_TIME + i;
      String firstTime = FIRST_TIME + i;
      int count = i;
      String logging = LOGGING + i;
      ExceptionInfo info = new ExceptionInfo(stackTrace, lastTime, firstTime, count, logging);
      exceptionInfos.add(info);
    }
  }

  @After
  public void after() throws Exception {
    exceptionInfos.clear();
  }

  /**
   * Method: getStackTrace()
   */
  @Test
  public void testGetStackTrace() throws Exception {
    for (int i = 0; i < N; i++) {
      Assert.assertTrue(exceptionInfos.get(i).getStackTrace().equals(STACK_TRACE + i));
    }
  }

  /**
   * Method: getLastTime()
   */
  @Test
  public void testGetLastTime() throws Exception {
    for (int i = 0; i < N; i++) {
      Assert.assertTrue(exceptionInfos.get(i).getLastTime().equals(LAST_TIME + i));
    }
  }

  /**
   * Method: getFirstTime()
   */
  @Test
  public void testGetFirstTime() throws Exception {
    for (int i = 0; i < N; i++) {
      Assert.assertTrue(exceptionInfos.get(i).getFirstTime().equals(FIRST_TIME + i));
    }
  }

  /**
   * Method: getCount()
   */
  @Test
  public void testGetCount() throws Exception {
    for (int i = 0; i < N; i++) {
      Assert.assertTrue(exceptionInfos.get(i).getCount() == i);
    }
  }

  /**
   * Method: getLogging()
   */
  @Test
  public void testGetLogging() throws Exception {
    for (int i = 0; i < N; i++) {
      Assert.assertTrue(exceptionInfos.get(i).getLogging().equals(LOGGING + i));
    }
  }
}
