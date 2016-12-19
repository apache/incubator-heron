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
package com.twitter.heron.metricscachemgr.metriccache;


import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.twitter.heron.metricscachemgr.SLAMetrics;
import com.twitter.heron.proto.tmaster.TopologyMaster;

public class ComponentMetricsTest {
  private static String debugFilePath = "/tmp/" + ComponentMetricsTest.class.getSimpleName() + ".debug.txt";

  private Path file = null;
  private List<String> lines = null;

  @Before
  public void before() {
    file = Paths.get(debugFilePath);
    lines = new ArrayList<>();
  }

  @After
  public void after() throws IOException {
    Files.write(file, lines, Charset.forName("UTF-8"));
  }

  @Test
  public void test() {
    ComponentMetrics cm = new ComponentMetrics("c_name", 10, 60);

    cm.AddMetricForInstance("i1", "__jvm-gc-collection-time-ms", SLAMetrics.MetricAggregationType.LAST, "1");
    cm.AddMetricForInstance("i1", "__jvm-gc-collection-time-ms", SLAMetrics.MetricAggregationType.LAST, "2");
    cm.AddMetricForInstance("i2", "__jvm-gc-collection-time-ms", SLAMetrics.MetricAggregationType.LAST, "3");

    cm.Purge();

    cm.AddMetricForInstance("i1", "__jvm-gc-collection-time-ms", SLAMetrics.MetricAggregationType.LAST, "4");
    cm.AddMetricForInstance("i1", "__jvm-gc-collection-time-ms", SLAMetrics.MetricAggregationType.LAST, "5");
    cm.AddMetricForInstance("i2", "__jvm-gc-collection-time-ms", SLAMetrics.MetricAggregationType.LAST, "6");

    lines.add(cm.toString());

    // build request
    TopologyMaster.MetricRequest.Builder request_builder = TopologyMaster.MetricRequest.newBuilder();
    request_builder.setComponentName("c1"); // bypass
    request_builder.addMetric("__jvm-gc-collection-time-ms");
    request_builder.addInstanceId("i1");

    TopologyMaster.MetricResponse.Builder response_builder = TopologyMaster.MetricResponse.newBuilder();
    cm.GetMetrics(request_builder.build(), 0, -1, response_builder);

    // assertion
    Assert.assertEquals(response_builder.getMetricCount(), 1);
    Assert.assertEquals(response_builder.getMetric(0).getMetric(0).getValue(), "5.0");

    // build request 2
    TopologyMaster.MetricRequest.Builder request_builder2 = TopologyMaster.MetricRequest.newBuilder();
    request_builder2.setComponentName("c2"); // bypass
    request_builder2.addMetric("__jvm-gc-collection-time-ms");
    request_builder2.addInstanceId("i2");

    TopologyMaster.MetricResponse.Builder response_builder2 = TopologyMaster.MetricResponse.newBuilder();
    cm.GetMetrics(request_builder2.build(), 0, -1, response_builder2);

    // assertion
    Assert.assertEquals(response_builder2.getMetricCount(), 1);
    Assert.assertEquals(response_builder2.getMetric(0).getMetric(0).getValue(), "6.0");

  }
}
