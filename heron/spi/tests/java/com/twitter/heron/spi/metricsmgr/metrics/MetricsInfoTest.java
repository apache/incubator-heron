package com.twitter.heron.metricsmgr.api.metrics;

import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.twitter.heron.metricsmgr.api.metrics.MetricsInfo;

public class MetricsInfoTest {
  private static final int N = 100;
  private static final String NAME = "name";
  private static final String VALUE = "value";
  private final List<MetricsInfo> metricsInfos = new ArrayList<MetricsInfo>();

  @Before
  public void before() throws Exception {
    metricsInfos.clear();
    for (int i = 0; i < N; i++) {
      MetricsInfo metricsInfo = new MetricsInfo(NAME + i, VALUE + i);
      metricsInfos.add(metricsInfo);
    }
  }

  @After
  public void after() throws Exception {
    metricsInfos.clear();
  }

  /**
   * Method: getName()
   */
  @Test
  public void testGetName() throws Exception {
    for (int i = 0; i < N; i++) {
      Assert.assertTrue(metricsInfos.get(i).getName().equals(NAME + i));
    }
  }

  /**
   * Method: getValue()
   */
  @Test
  public void testGetValue() throws Exception {
    for (int i = 0; i < N; i++) {
      Assert.assertTrue(metricsInfos.get(i).getValue().equals(VALUE + i));
    }
  }
}
