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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.microsoft.dhalion.detector.Symptom;
import com.microsoft.dhalion.metrics.ComponentMetrics;
import com.microsoft.dhalion.metrics.InstanceMetrics;

import org.junit.Test;

import com.twitter.heron.healthmgr.HealthPolicyConfig;
import com.twitter.heron.healthmgr.sensors.BufferSizeSensor;

import static com.twitter.heron.healthmgr.common.HealthMgrConstants.METRIC_BUFFER_SIZE;
import static com.twitter.heron.healthmgr.detectors.GrowingWaitQueueDetector.CONF_LIMIT;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class GrowingWaitQueueDetectorTest {
  @Test
  public void testDetector() {
    HealthPolicyConfig config = mock(HealthPolicyConfig.class);
    when(config.getConfig(CONF_LIMIT, "10")).thenReturn("5");

    ComponentMetrics compMetrics;
    InstanceMetrics instanceMetrics;
    Map<Long, Double> bufferSizes;
    Map<String, ComponentMetrics> topologyMetrics = new HashMap<>();

    instanceMetrics = new InstanceMetrics("i1");
    bufferSizes = new HashMap<>();
    bufferSizes.put(1497892222L, 0.0);
    bufferSizes.put(1497892270L, 300.0);
    bufferSizes.put(1497892330L, 700.0);
    bufferSizes.put(1497892390L, 1000.0);
    bufferSizes.put(1497892450L, 1300.0);
    instanceMetrics.addMetric(METRIC_BUFFER_SIZE, bufferSizes);

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
    bufferSizes.put(1497892222L, 0.0);
    bufferSizes.put(1497892270L, 200.0);
    bufferSizes.put(1497892330L, 400.0);
    bufferSizes.put(1497892390L, 600.0);
    bufferSizes.put(1497892450L, 800.0);
    instanceMetrics.addMetric(METRIC_BUFFER_SIZE, bufferSizes);

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
