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
package com.twitter.heron.metricscachemgr.metricscache;

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

public class InstanceMetricsTest {
  private static String debugFilePath =
      "/tmp/" + InstanceMetricsTest.class.getSimpleName() + ".debug.txt";

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

  private void prepareTestData(InstanceMetrics im) {
    im.AddMetricWithName("__emit-count", MetricAggregationType.SUM, "1");
    im.AddMetricWithName("__emit-count", MetricAggregationType.SUM, "2");
    im.AddMetricWithName("__emit-count", MetricAggregationType.SUM, "3");

    im.AddMetricWithName("__jvm-uptime-secs", MetricAggregationType.LAST, "10");
    im.AddMetricWithName("__jvm-uptime-secs", MetricAggregationType.LAST, "11");
    im.AddMetricWithName("__jvm-uptime-secs", MetricAggregationType.LAST, "12");

    im.Purge();

    im.AddMetricWithName("__emit-count", MetricAggregationType.SUM, "4");
    im.AddMetricWithName("__emit-count", MetricAggregationType.SUM, "5");
    im.AddMetricWithName("__emit-count", MetricAggregationType.SUM, "6");

    im.AddMetricWithName("__jvm-uptime-secs", MetricAggregationType.LAST, "13");
    im.AddMetricWithName("__jvm-uptime-secs", MetricAggregationType.LAST, "14");
    im.AddMetricWithName("__jvm-uptime-secs", MetricAggregationType.LAST, "15");

    lines.add(im.toString());
  }

  @Test
  public void test1() {
    lines.add("test1");
    InstanceMetrics im = new InstanceMetrics("i1", 10, 60);
    prepareTestData(im);

    // build request
    TopologyMaster.MetricRequest.Builder requestBuilder = TopologyMaster.MetricRequest.newBuilder();
    requestBuilder.setComponentName("c1"); // bypass
    requestBuilder.addMetric("__emit-count");

    TopologyMaster.MetricResponse.Builder responseBuilder =
        TopologyMaster.MetricResponse.newBuilder();
    im.GetMetrics(requestBuilder.build(), 0, -1, responseBuilder);

    // assertion
    Assert.assertEquals(responseBuilder.getMetricCount(), 1);
    Assert.assertEquals(responseBuilder.getMetric(0).getMetric(0).getValue(), "21.0");
  }

  @Test
  public void test2() {
    lines.add("test2");
    InstanceMetrics im = new InstanceMetrics("i1", 10, 60);
    prepareTestData(im);

    // build request 2
    TopologyMaster.MetricRequest.Builder requestBuilder2 =
        TopologyMaster.MetricRequest.newBuilder();
    requestBuilder2.setComponentName("c2"); // bypass
    requestBuilder2.addMetric("__jvm-uptime-secs");

    TopologyMaster.MetricResponse.Builder responseBuilder2 =
        TopologyMaster.MetricResponse.newBuilder();
    im.GetMetrics(requestBuilder2.build(), 0, -1, responseBuilder2);

    // assertion
    Assert.assertEquals(responseBuilder2.getMetricCount(), 1);
    Assert.assertEquals(responseBuilder2.getMetric(0).getMetric(0).getValue(), "15.0");
  }

  @Test
  public void test3() {
    lines.add("test3");
    InstanceMetrics im = new InstanceMetrics("i1", 10, 60);
    prepareTestData(im);

    // build request 3
    TopologyMaster.MetricRequest.Builder requestBuilder3 =
        TopologyMaster.MetricRequest.newBuilder();
    requestBuilder3.setComponentName("c3"); // bypass
    requestBuilder3.setMinutely(true);
    requestBuilder3.addMetric("__emit-count");

    TopologyMaster.MetricResponse.Builder responseBuilder3 =
        TopologyMaster.MetricResponse.newBuilder();
    im.GetMetrics(requestBuilder3.build(), 0, Integer.MAX_VALUE, responseBuilder3);

    // assertion
    Assert.assertEquals(responseBuilder3.getMetricCount(), 1);
    Assert.assertEquals(responseBuilder3.getMetric(0).getInstanceId(), "i1");
    Assert.assertEquals(responseBuilder3.getMetric(0).getMetric(0).getName(), "__emit-count");
    Assert.assertEquals(responseBuilder3.getMetric(0).getMetric(0).getIntervalValuesCount(), 10);
    Assert.assertEquals(
        responseBuilder3.getMetric(0).getMetric(0).getIntervalValues(0).getValue(), "15.0");
    Assert.assertEquals(
        responseBuilder3.getMetric(0).getMetric(0).getIntervalValues(1).getValue(), "6.0");
    Assert.assertEquals(
        responseBuilder3.getMetric(0).getMetric(0).getIntervalValues(2).getValue(), "0.0");

  }
}
