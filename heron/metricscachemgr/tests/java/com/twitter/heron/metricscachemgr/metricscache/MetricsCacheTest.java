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

import org.junit.Test;

import com.twitter.heron.common.basics.NIOLooper;
import com.twitter.heron.common.config.SystemConfig;
import com.twitter.heron.metricsmgr.MetricsSinksConfig;
import com.twitter.heron.proto.tmaster.TopologyMaster;

import static org.junit.Assert.assertEquals;

public class MetricsCacheTest {
  public static final String CONFIG_SYSTEM_PATH =
      "../../../../../../../../../heron/config/src/yaml/conf/examples/heron_internals.yaml";
  public static final String CONFIG_SINK_PATH =
      "../../../../../../../../../heron/config/src/yaml/conf/examples/metrics_sinks.yaml";

  @Test
  public void testMetricCache() throws IOException {
    // prepare config files
    SystemConfig systemConfig = SystemConfig.newBuilder(true)
        .putAll(CONFIG_SYSTEM_PATH, true)
        .build();
    MetricsSinksConfig sinksConfig = new MetricsSinksConfig(CONFIG_SINK_PATH);

    // initialize metric cache, except looper
    MetricsCache mc = new MetricsCache(systemConfig, sinksConfig, new NIOLooper());

    mc.addMetrics(TopologyMaster.PublishMetrics.newBuilder()
        .addMetrics(TopologyMaster.MetricDatum.newBuilder()
            .setComponentName("c1").setInstanceId("i1").setName("__jvm-uptime-secs")
            .setTimestamp(System.currentTimeMillis()).setValue("0.1"))
        .addExceptions(TopologyMaster.TmasterExceptionLog.newBuilder()
            .setComponentName("c1").setHostname("h1").setInstanceId("i1")
            .setStacktrace("s1").setLogging("l1")
            .setCount(1)
            .setFirsttime(String.valueOf(System.currentTimeMillis()))
            .setLasttime(String.valueOf(System.currentTimeMillis())))
        .build());

    // query last 10 seconds
    TopologyMaster.MetricResponse response = mc.getMetrics(TopologyMaster.MetricRequest.newBuilder()
        .setComponentName("c1").addInstanceId("i1")
        .setInterval(10).addMetric("__jvm-uptime-secs")
        .build());

    assertEquals(response.getMetricCount(), 1);
    assertEquals(response.getMetric(0).getInstanceId(), "i1");
    assertEquals(response.getMetric(0).getMetricCount(), 1);
    assertEquals(response.getMetric(0).getMetric(0).getName(), "__jvm-uptime-secs");
    assertEquals(response.getMetric(0).getMetric(0).getValue(), "0.1");
  }
}
