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

import static com.twitter.heron.healthmgr.detectors.WaitQueueDisparityDetector.CONF_DISPARITY_RATIO;
import static com.twitter.heron.healthmgr.sensors.BaseSensor.MetricName.METRIC_BUFFER_SIZE;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class WaitQueueDisparityDetectorTest {
  @Test
  public void testConfigAndFilter() {
    HealthPolicyConfig config = mock(HealthPolicyConfig.class);
    when(config.getConfig(CONF_DISPARITY_RATIO, 20)).thenReturn(15.0);

    ComponentMetrics compMetrics = new ComponentMetrics("bolt");
    compMetrics.addInstanceMetric(new InstanceMetrics("i1", METRIC_BUFFER_SIZE.text(), 1501));
    compMetrics.addInstanceMetric(new InstanceMetrics("i2", METRIC_BUFFER_SIZE.text(), 100));

    Map<String, ComponentMetrics> topologyMetrics = new HashMap<>();
    topologyMetrics.put("bolt", compMetrics);

    BufferSizeSensor sensor = mock(BufferSizeSensor.class);
    when(sensor.get()).thenReturn(topologyMetrics);
    when(sensor.getMetricName()).thenReturn(METRIC_BUFFER_SIZE.text());

    WaitQueueDisparityDetector detector = new WaitQueueDisparityDetector(sensor, config);
    List<Symptom> symptoms = detector.detect();

    assertEquals(1, symptoms.size());

    compMetrics = new ComponentMetrics("bolt");
    compMetrics.addInstanceMetric(new InstanceMetrics("i1", METRIC_BUFFER_SIZE.text(), 1500));
    compMetrics.addInstanceMetric(new InstanceMetrics("i2", METRIC_BUFFER_SIZE.text(), 110));
    topologyMetrics.put("bolt", compMetrics);

    sensor = mock(BufferSizeSensor.class);
    when(sensor.get()).thenReturn(topologyMetrics);

    detector = new WaitQueueDisparityDetector(sensor, config);
    symptoms = detector.detect();

    assertEquals(0, symptoms.size());
  }
}
