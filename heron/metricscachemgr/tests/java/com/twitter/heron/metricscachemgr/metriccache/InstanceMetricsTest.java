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

public class InstanceMetricsTest {
  private static String debugFilePath = "/tmp/" + InstanceMetricsTest.class.getSimpleName() + ".debug.txt";

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
    InstanceMetrics im = new InstanceMetrics("i1", 10, 60);

    im.AddMetricWithName("__emit-count", SLAMetrics.MetricAggregationType.SUM, "1");
    im.AddMetricWithName("__emit-count", SLAMetrics.MetricAggregationType.SUM, "2");
    im.AddMetricWithName("__emit-count", SLAMetrics.MetricAggregationType.SUM, "3");

    im.AddMetricWithName("__jvm-uptime-secs", SLAMetrics.MetricAggregationType.LAST, "10");
    im.AddMetricWithName("__jvm-uptime-secs", SLAMetrics.MetricAggregationType.LAST, "11");
    im.AddMetricWithName("__jvm-uptime-secs", SLAMetrics.MetricAggregationType.LAST, "12");

    im.Purge();

    im.AddMetricWithName("__emit-count", SLAMetrics.MetricAggregationType.SUM, "4");
    im.AddMetricWithName("__emit-count", SLAMetrics.MetricAggregationType.SUM, "5");
    im.AddMetricWithName("__emit-count", SLAMetrics.MetricAggregationType.SUM, "6");

    im.AddMetricWithName("__jvm-uptime-secs", SLAMetrics.MetricAggregationType.LAST, "13");
    im.AddMetricWithName("__jvm-uptime-secs", SLAMetrics.MetricAggregationType.LAST, "14");
    im.AddMetricWithName("__jvm-uptime-secs", SLAMetrics.MetricAggregationType.LAST, "15");

    lines.add(im.toString());

    // build request
    TopologyMaster.MetricRequest.Builder request_builder = TopologyMaster.MetricRequest.newBuilder();
    request_builder.setComponentName("c1"); // bypass
    request_builder.addMetric("__emit-count");

    TopologyMaster.MetricResponse.Builder response_builder = TopologyMaster.MetricResponse.newBuilder();
    im.GetMetrics(request_builder.build(), 0, -1, response_builder);

    // assertion
    Assert.assertEquals(response_builder.getMetricCount(), 1);
    Assert.assertEquals(response_builder.getMetric(0).getMetric(0).getValue(), "21.0");

    // build request 2
    TopologyMaster.MetricRequest.Builder request_builder2 = TopologyMaster.MetricRequest.newBuilder();
    request_builder2.setComponentName("c2"); // bypass
    request_builder2.addMetric("__jvm-uptime-secs");

    TopologyMaster.MetricResponse.Builder response_builder2 = TopologyMaster.MetricResponse.newBuilder();
    im.GetMetrics(request_builder2.build(), 0, -1, response_builder2);

    // assertion
    Assert.assertEquals(response_builder2.getMetricCount(), 1);
    Assert.assertEquals(response_builder2.getMetric(0).getMetric(0).getValue(), "15.0");

    // build request 3
    TopologyMaster.MetricRequest.Builder request_builder3 = TopologyMaster.MetricRequest.newBuilder();
    request_builder3.setComponentName("c3"); // bypass
    request_builder3.setMinutely(true);
    request_builder3.addMetric("__emit-count");

    TopologyMaster.MetricResponse.Builder response_builder3 = TopologyMaster.MetricResponse.newBuilder();
    im.GetMetrics(request_builder3.build(), 0, Integer.MAX_VALUE, response_builder3);

    // assertion
    Assert.assertEquals(response_builder3.getMetricCount(), 1);
    Assert.assertEquals(response_builder3.getMetric(0).getInstanceId(), "i1");
    Assert.assertEquals(response_builder3.getMetric(0).getMetric(0).getName(), "__emit-count");
    Assert.assertEquals(response_builder3.getMetric(0).getMetric(0).getIntervalValuesCount(), 10);
    Assert.assertEquals(response_builder3.getMetric(0).getMetric(0).getIntervalValues(0).getValue(), "15.0");
    Assert.assertEquals(response_builder3.getMetric(0).getMetric(0).getIntervalValues(1).getValue(), "6.0");
    Assert.assertEquals(response_builder3.getMetric(0).getMetric(0).getIntervalValues(2).getValue(), "0.0");

  }
}
