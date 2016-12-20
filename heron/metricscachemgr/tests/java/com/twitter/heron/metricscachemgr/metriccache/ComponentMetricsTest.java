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

import com.twitter.heron.proto.tmaster.TopologyMaster;
import com.twitter.heron.spi.metricsmgr.metrics.MetricsFilter.MetricAggregationType;

public class ComponentMetricsTest {
  private static String debugFilePath =
      "/tmp/" + ComponentMetricsTest.class.getSimpleName() + ".debug.txt";

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

  private void prepareTestData(ComponentMetrics cm) {
    cm.AddMetricForInstance("i1", "__jvm-gc-collection-time-ms", MetricAggregationType.LAST, "1");
    cm.AddMetricForInstance("i1", "__jvm-gc-collection-time-ms", MetricAggregationType.LAST, "2");
    cm.AddMetricForInstance("i2", "__jvm-gc-collection-time-ms", MetricAggregationType.LAST, "3");

    cm.Purge();

    cm.AddMetricForInstance("i1", "__jvm-gc-collection-time-ms", MetricAggregationType.LAST, "4");
    cm.AddMetricForInstance("i1", "__jvm-gc-collection-time-ms", MetricAggregationType.LAST, "5");
    cm.AddMetricForInstance("i2", "__jvm-gc-collection-time-ms", MetricAggregationType.LAST, "6");

    lines.add(cm.toString());
  }

  @Test
  public void test1() {
    lines.add("test1");
    ComponentMetrics cm = new ComponentMetrics("c_name", 10, 60);
    prepareTestData(cm);

    // build request
    TopologyMaster.MetricRequest.Builder requestBuilder =
        TopologyMaster.MetricRequest.newBuilder();
    requestBuilder.setComponentName("c1"); // bypass
    requestBuilder.addMetric("__jvm-gc-collection-time-ms");
    requestBuilder.addInstanceId("i1");

    TopologyMaster.MetricResponse.Builder responseBuilder =
        TopologyMaster.MetricResponse.newBuilder();
    cm.GetMetrics(requestBuilder.build(), 0, -1, responseBuilder);

    // assertion
    Assert.assertEquals(responseBuilder.getMetricCount(), 1);
    Assert.assertEquals(responseBuilder.getMetric(0).getMetric(0).getValue(), "5.0");

  }


  @Test
  public void test2() {
    lines.add("test2");
    ComponentMetrics cm = new ComponentMetrics("c_name", 10, 60);
    prepareTestData(cm);

    // build request 2
    TopologyMaster.MetricRequest.Builder requestBuilder2 =
        TopologyMaster.MetricRequest.newBuilder();
    requestBuilder2.setComponentName("c2"); // bypass
    requestBuilder2.addMetric("__jvm-gc-collection-time-ms");
    requestBuilder2.addInstanceId("i2");

    TopologyMaster.MetricResponse.Builder responseBuilder2 =
        TopologyMaster.MetricResponse.newBuilder();
    cm.GetMetrics(requestBuilder2.build(), 0, -1, responseBuilder2);

    // assertion
    Assert.assertEquals(responseBuilder2.getMetricCount(), 1);
    Assert.assertEquals(responseBuilder2.getMetric(0).getMetric(0).getValue(), "6.0");

  }
}
