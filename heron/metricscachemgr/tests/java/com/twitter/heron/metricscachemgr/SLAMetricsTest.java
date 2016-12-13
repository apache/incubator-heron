//  Copyright 2016 Twitter. All rights reserved.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License
package com.twitter.heron.metricscachemgr;

import java.util.HashMap;
import java.util.Map;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

// test SLAMetrics
public class SLAMetricsTest {

  SLAMetrics slam = null;

  @Before
  public void before() throws Exception {
    slam = new SLAMetrics("");

    // mock config for prototype
    Map<String, String> metric = new HashMap<>();
    metric.put("__emit-count", "SUM");
    metric.put("__execute-count", "SUM");
    metric.put("__fail-count", "SUM");
    metric.put("__ack-count", "SUM");
    metric.put("__complete-latency", "AVG");
    metric.put("__execute-latency", "AVG");
    metric.put("__process-latency", "AVG");
    metric.put("__jvm-uptime-secs", "LAST");
    metric.put("__jvm-process-cpu-load", "LAST");
    metric.put("__jvm-memory-used-mb", "LAST");
    metric.put("__jvm-memory-mb-total", "LAST");
    metric.put("__jvm-gc-collection-time-ms", "LAST");
    metric.put("__server/__time_spent_back_pressure_initiated", "SUM");
    metric.put("__time_spent_back_pressure_by_compid", "SUM");

    slam.InitSLAMetrics(metric);
  }

  @After
  public void after() {
    // nop
  }

  @Test
  public void testGetAggregationType() {
    Assert.assertEquals(slam.GetAggregationType("__jvm-uptime-secs"), SLAMetrics.MetricAggregationType.LAST);
    Assert.assertEquals(slam.GetAggregationType("__fail-count"), SLAMetrics.MetricAggregationType.SUM);
  }

  @Test
  public void testIsSLAMetric() {
    Assert.assertTrue(slam.IsSLAMetric("__time_spent_back_pressure_by_compid"));
    Assert.assertFalse(slam.IsSLAMetric("1__time_spent_back_pressure_by_compid"));
  }
}
