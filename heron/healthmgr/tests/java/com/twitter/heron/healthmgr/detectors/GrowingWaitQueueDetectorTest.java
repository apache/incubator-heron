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

package com.twitter.heron.healthmgr.detectors;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.microsoft.dhalion.detector.Symptom;
import com.microsoft.dhalion.metrics.ComponentMetrics;
import com.microsoft.dhalion.metrics.InstanceMetrics;

import org.junit.Test;

import com.twitter.heron.healthmgr.HealthPolicyConfig;
import com.twitter.heron.healthmgr.sensors.BufferSizeSensor;

import static com.twitter.heron.healthmgr.detectors.GrowingWaitQueueDetector.CONF_LIMIT;
import static com.twitter.heron.healthmgr.sensors.BaseSensor.MetricName.METRIC_BUFFER_SIZE;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class GrowingWaitQueueDetectorTest {
  @Test
  public void testDetector() {
    HealthPolicyConfig config = mock(HealthPolicyConfig.class);
    when(config.getConfig(CONF_LIMIT, 10.0)).thenReturn(5.0);

    ComponentMetrics compMetrics;
    InstanceMetrics instanceMetrics;
    Map<Instant, Double> bufferSizes;
    Map<String, ComponentMetrics> topologyMetrics = new HashMap<>();

    instanceMetrics = new InstanceMetrics("i1");
    bufferSizes = new HashMap<>();
    bufferSizes.put(Instant.ofEpochSecond(1497892222), 0.0);
    bufferSizes.put(Instant.ofEpochSecond(1497892270), 300.0);
    bufferSizes.put(Instant.ofEpochSecond(1497892330), 700.0);
    bufferSizes.put(Instant.ofEpochSecond(1497892390), 1000.0);
    bufferSizes.put(Instant.ofEpochSecond(1497892450), 1300.0);
    instanceMetrics.addMetric(METRIC_BUFFER_SIZE.text(), bufferSizes);

    compMetrics = new ComponentMetrics("bolt");
    compMetrics.addInstanceMetric(instanceMetrics);

    topologyMetrics.put("bolt", compMetrics);

    BufferSizeSensor sensor = mock(BufferSizeSensor.class);
    when(sensor.get()).thenReturn(topologyMetrics);

    GrowingWaitQueueDetector detector = new GrowingWaitQueueDetector(sensor, config);
    List<Symptom> symptoms = detector.detect();

    assertEquals(1, symptoms.size());

    instanceMetrics = new InstanceMetrics("i1");
    bufferSizes = new HashMap<>();
    bufferSizes.put(Instant.ofEpochSecond(1497892222), 0.0);
    bufferSizes.put(Instant.ofEpochSecond(1497892270), 200.0);
    bufferSizes.put(Instant.ofEpochSecond(1497892330), 400.0);
    bufferSizes.put(Instant.ofEpochSecond(1497892390), 600.0);
    bufferSizes.put(Instant.ofEpochSecond(1497892450), 800.0);
    instanceMetrics.addMetric(METRIC_BUFFER_SIZE.text(), bufferSizes);

    compMetrics = new ComponentMetrics("bolt");
    compMetrics.addInstanceMetric(instanceMetrics);

    topologyMetrics.put("bolt", compMetrics);

    sensor = mock(BufferSizeSensor.class);
    when(sensor.get()).thenReturn(topologyMetrics);

    detector = new GrowingWaitQueueDetector(sensor, config);
    symptoms = detector.detect();

    assertEquals(0, symptoms.size());
  }
}
