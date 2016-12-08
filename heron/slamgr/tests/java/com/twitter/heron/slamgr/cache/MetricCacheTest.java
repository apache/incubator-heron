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


package com.twitter.heron.slamgr.cache;

import java.util.HashMap;
import java.util.Map;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.twitter.heron.proto.tmaster.TopologyMaster;

public class MetricCacheTest {


  MetricCache mc = null;

  @Before
  public void before() throws Exception {
    mc = new MetricCache(180 * 60, "");

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

    mc.getSLAMetrics().InitSLAMetrics(metric);
  }

  @After
  public void after() throws Exception {
  }

  @Test
  public void testAddMetric() {
    TopologyMaster.PublishMetrics.Builder _metrics_builder = TopologyMaster.PublishMetrics.newBuilder();

    _metrics_builder.addMetrics(TopologyMaster.MetricDatum.newBuilder().
        setComponentName("c1").setInstanceId("i1").setName("__emit-count").setValue("1").build());
    _metrics_builder.addMetrics(TopologyMaster.MetricDatum.newBuilder().
        setComponentName("c1").setInstanceId("i1").setName("__emit-count").setValue("2").build());
    _metrics_builder.addMetrics(TopologyMaster.MetricDatum.newBuilder().
        setComponentName("c1").setInstanceId("i1").setName("__emit-count").setValue("3").build());

    mc.AddMetric(_metrics_builder.build());

    mc.Purge();

    TopologyMaster.PublishMetrics.Builder _metrics_builder2 = TopologyMaster.PublishMetrics.newBuilder();

    _metrics_builder2.addMetrics(TopologyMaster.MetricDatum.newBuilder().
        setComponentName("c1").setInstanceId("i1").setName("__emit-count").setValue("4").build());
    _metrics_builder2.addMetrics(TopologyMaster.MetricDatum.newBuilder().
        setComponentName("c1").setInstanceId("i1").setName("__emit-count").setValue("5").build());
    _metrics_builder2.addMetrics(TopologyMaster.MetricDatum.newBuilder().
        setComponentName("c1").setInstanceId("i1").setName("__emit-count").setValue("6").build());

    mc.AddMetric(_metrics_builder.build());

    // builder
    TopologyMaster.MetricRequest.Builder request_builder = TopologyMaster.MetricRequest.newBuilder();

    TopologyMaster.MetricResponse res = mc.GetMetrics(request_builder.setComponentName("c1").addInstanceId("i1").addMetric("__emit-count").build());

    // assertion
    Assert.assertEquals(res.getMetricCount(), 1);
    Assert.assertEquals(res.getMetric(0).getMetric(0).getValue(), "21");
  }
}