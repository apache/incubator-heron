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

package com.twitter.heron.healthmgr.common;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import com.microsoft.dhalion.metrics.ComponentMetrics;
import com.microsoft.dhalion.metrics.InstanceMetrics;

import org.junit.Test;

import static com.twitter.heron.healthmgr.sensors.BaseSensor.MetricName.METRIC_BUFFER_SIZE;
import static com.twitter.heron.healthmgr.sensors.BaseSensor.MetricName.METRIC_WAIT_Q_GROWTH_RATE;
import static org.junit.Assert.assertEquals;

public class ComponentMetricsHelperTest {

  @Test
  public void detectsMultipleCompIncreasingBuffer() {
    ComponentMetrics compMetrics;
    InstanceMetrics instanceMetrics;
    Map<Instant, Double> bufferSizes;

    compMetrics = new ComponentMetrics("bolt");

    instanceMetrics = new InstanceMetrics("i1");
    bufferSizes = new HashMap<>();
    bufferSizes.put(Instant.ofEpochSecond(1497892210), 0.0);
    bufferSizes.put(Instant.ofEpochSecond(1497892270), 300.0);
    bufferSizes.put(Instant.ofEpochSecond(1497892330), 600.0);
    bufferSizes.put(Instant.ofEpochSecond(1497892390), 900.0);
    bufferSizes.put(Instant.ofEpochSecond(1497892450), 1200.0);
    instanceMetrics.addMetric(METRIC_BUFFER_SIZE.text(), bufferSizes);

    compMetrics.addInstanceMetric(instanceMetrics);

    instanceMetrics = new InstanceMetrics("i2");
    bufferSizes = new HashMap<>();
    bufferSizes.put(Instant.ofEpochSecond(1497892270), 0.0);
    bufferSizes.put(Instant.ofEpochSecond(1497892330), 180.0);
    bufferSizes.put(Instant.ofEpochSecond(1497892390), 360.0);
    bufferSizes.put(Instant.ofEpochSecond(1497892450), 540.0);
    instanceMetrics.addMetric(METRIC_BUFFER_SIZE.text(), bufferSizes);

    compMetrics.addInstanceMetric(instanceMetrics);

    ComponentMetricsHelper helper = new ComponentMetricsHelper(compMetrics);
    helper.computeBufferSizeTrend();
    assertEquals(5, helper.getMaxBufferChangeRate(), 0.1);

    HashMap<String, InstanceMetrics> metrics = compMetrics.getMetrics();
    assertEquals(1, metrics.get("i1").getMetrics().get(METRIC_WAIT_Q_GROWTH_RATE.text()).size());
    assertEquals(5, metrics.get("i1").getMetricValueSum(METRIC_WAIT_Q_GROWTH_RATE.text()), 0.1);
    assertEquals(3, metrics.get("i2").getMetricValueSum(METRIC_WAIT_Q_GROWTH_RATE.text()), 0.1);
  }
}
