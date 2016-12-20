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
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.twitter.heron.proto.tmaster.TopologyMaster;

public class MetricCacheTest {
  public static final String CONFIG_PATH =
      "/Users/huijunw/workspace/heron/heron/config/src/yaml/conf/examples/metrics_sinks.yaml";
  private static String debugFilePath =
      "/tmp/" + MetricCacheTest.class.getSimpleName() + ".debug.txt";
  private MetricCache mc = null;
  private Path file = null;
  private List<String> lines = null;

  @Before
  public void before() throws Exception {
    file = Paths.get(debugFilePath);
    lines = new ArrayList<>();

    mc = new MetricCache(15, CONFIG_PATH);
  }

  @After
  public void after() throws IOException {
    Files.write(file, lines, Charset.forName("UTF-8"));
  }

  @Test
  public void testMetricCache() {
    long ts = Instant.now().getEpochSecond();

    TopologyMaster.PublishMetrics.Builder metricsBuilder =
        TopologyMaster.PublishMetrics.newBuilder();

    metricsBuilder.addMetrics(TopologyMaster.MetricDatum.newBuilder().setTimestamp(ts).
        setComponentName("c1").setInstanceId("i1").setName("__emit-count").setValue("1").build());
    metricsBuilder.addMetrics(TopologyMaster.MetricDatum.newBuilder().setTimestamp(ts).
        setComponentName("c1").setInstanceId("i1").setName("__emit-count").setValue("2").build());
    metricsBuilder.addMetrics(TopologyMaster.MetricDatum.newBuilder().setTimestamp(ts).
        setComponentName("c1").setInstanceId("i1").setName("__emit-count").setValue("3").build());

    mc.AddMetric(metricsBuilder.build());

    mc.Purge();

    TopologyMaster.PublishMetrics.Builder metricsBuilder2 =
        TopologyMaster.PublishMetrics.newBuilder();

    metricsBuilder2.addMetrics(TopologyMaster.MetricDatum.newBuilder().setTimestamp(ts).
        setComponentName("c1").setInstanceId("i1").setName("__emit-count").setValue("4").build());
    metricsBuilder2.addMetrics(TopologyMaster.MetricDatum.newBuilder().setTimestamp(ts).
        setComponentName("c1").setInstanceId("i1").setName("__emit-count").setValue("5").build());
    metricsBuilder2.addMetrics(TopologyMaster.MetricDatum.newBuilder().setTimestamp(ts).
        setComponentName("c1").setInstanceId("i1").setName("__emit-count").setValue("6").build());

    mc.AddMetric(metricsBuilder2.build());

    lines.add(mc.toString());

    // builder
    TopologyMaster.MetricRequest.Builder requestBuilder = TopologyMaster.MetricRequest.newBuilder();
    requestBuilder.setExplicitInterval(
        TopologyMaster.MetricInterval.newBuilder().setStart(ts - 10).setEnd(ts + 10).build());
    requestBuilder.setMinutely(true);

    TopologyMaster.MetricResponse res = mc.GetMetrics(requestBuilder
        .setComponentName("c1").addInstanceId("i1").addMetric("__emit-count").build());

    // assertion
    Assert.assertEquals(res.getMetricCount(), 1);
    Assert.assertEquals(res.getMetric(0).getInstanceId(), "i1");
    Assert.assertEquals(res.getMetric(0).getMetricCount(), 1);
    Assert.assertEquals(res.getMetric(0).getMetric(0).getName(), "__emit-count");
    Assert.assertEquals(res.getMetric(0).getMetric(0).getIntervalValuesCount(), 3);
    Assert.assertEquals(res.getMetric(0).getMetric(0).getIntervalValues(1).getValue(), "6.0");
  }
}
