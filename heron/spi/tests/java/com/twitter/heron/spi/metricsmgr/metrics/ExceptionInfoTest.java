package com.twitter.heron.metricsmgr.api.metrics;

import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.twitter.heron.metricsmgr.api.metrics.ExceptionInfo;

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
