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

import org.junit.Assert;
import org.junit.Test;

import com.twitter.heron.proto.tmaster.TopologyMaster;
import com.twitter.heron.slamgr.SLAMetrics;

public class InstanceMetricsTest {
  @Test
  public void test() {
    InstanceMetrics im = new InstanceMetrics("instance_id", 180 * 60, 60);

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

    // build request
    TopologyMaster.MetricRequest.Builder request_builder = TopologyMaster.MetricRequest.newBuilder();
    request_builder.addMetric("__emit-count");

    TopologyMaster.MetricResponse.Builder response_builder = TopologyMaster.MetricResponse.newBuilder();
    im.GetMetrics(request_builder.build(), 0, -1, response_builder);

    // assertion
    Assert.assertEquals(response_builder.getMetricCount(), 2);
    Assert.assertEquals(response_builder.getMetric(0).getMetric(0).getValue(), "15");

    // build request 2
    TopologyMaster.MetricRequest.Builder request_builder2 = TopologyMaster.MetricRequest.newBuilder();
    request_builder2.addMetric("__jvm-uptime-secs");

    TopologyMaster.MetricResponse.Builder response_builder2 = TopologyMaster.MetricResponse.newBuilder();
    im.GetMetrics(request_builder2.build(), 0, -1, response_builder2);

    // assertion
    Assert.assertEquals(response_builder2.getMetricCount(), 1);
    Assert.assertEquals(response_builder2.getMetric(0).getMetric(0).getValue(), "15");

  }
}
