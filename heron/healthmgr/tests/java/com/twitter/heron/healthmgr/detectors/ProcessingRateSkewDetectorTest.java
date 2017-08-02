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
import com.twitter.heron.healthmgr.sensors.ExecuteCountSensor;

import static com.twitter.heron.healthmgr.detectors.ProcessingRateSkewDetector.CONF_SKEW_RATIO;
import static com.twitter.heron.healthmgr.sensors.BaseSensor.MetricName.METRIC_EXE_COUNT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ProcessingRateSkewDetectorTest {
  @Test
  public void testConfigAndFilter() {
    HealthPolicyConfig config = mock(HealthPolicyConfig.class);
    when(config.getConfig(CONF_SKEW_RATIO, 1.5)).thenReturn(2.5);

    ComponentMetrics compMetrics = new ComponentMetrics("bolt");
    compMetrics.addInstanceMetric(new InstanceMetrics("i1", METRIC_EXE_COUNT.text(), 1000));
    compMetrics.addInstanceMetric(new InstanceMetrics("i2", METRIC_EXE_COUNT.text(), 200));

    Map<String, ComponentMetrics> topologyMetrics = new HashMap<>();
    topologyMetrics.put("bolt", compMetrics);

    ExecuteCountSensor sensor = mock(ExecuteCountSensor.class);
    when(sensor.get()).thenReturn(topologyMetrics);
    when(sensor.getMetricName()).thenReturn(METRIC_EXE_COUNT.text());

    ProcessingRateSkewDetector detector = new ProcessingRateSkewDetector(sensor, config);
    List<Symptom> symptoms = detector.detect();

    assertEquals(1, symptoms.size());

    compMetrics = new ComponentMetrics("bolt");
    compMetrics.addInstanceMetric(new InstanceMetrics("i1", METRIC_EXE_COUNT.text(), 1000));
    compMetrics.addInstanceMetric(new InstanceMetrics("i2", METRIC_EXE_COUNT.text(), 500));
    topologyMetrics.put("bolt", compMetrics);

    sensor = mock(ExecuteCountSensor.class);
    when(sensor.get()).thenReturn(topologyMetrics);

    detector = new ProcessingRateSkewDetector(sensor, config);
    symptoms = detector.detect();

    assertEquals(0, symptoms.size());
  }

  @Test
  public void testReturnsMultipleComponents() {
    HealthPolicyConfig config = mock(HealthPolicyConfig.class);
    when(config.getConfig(CONF_SKEW_RATIO, 1.5)).thenReturn(2.5);

    ComponentMetrics compMetrics1 = new ComponentMetrics("bolt-1");
    compMetrics1.addInstanceMetric(new InstanceMetrics("i1", METRIC_EXE_COUNT.text(), 1000));
    compMetrics1.addInstanceMetric(new InstanceMetrics("i2", METRIC_EXE_COUNT.text(), 200));

    ComponentMetrics compMetrics2 = new ComponentMetrics("bolt-2");
    compMetrics2.addInstanceMetric(new InstanceMetrics("i1", METRIC_EXE_COUNT.text(), 1000));
    compMetrics2.addInstanceMetric(new InstanceMetrics("i2", METRIC_EXE_COUNT.text(), 200));

    ComponentMetrics compMetrics3 = new ComponentMetrics("bolt-3");
    compMetrics3.addInstanceMetric(new InstanceMetrics("i1", METRIC_EXE_COUNT.text(), 1000));
    compMetrics3.addInstanceMetric(new InstanceMetrics("i2", METRIC_EXE_COUNT.text(), 500));

    Map<String, ComponentMetrics> topologyMetrics = new HashMap<>();
    topologyMetrics.put("bolt-1", compMetrics1);
    topologyMetrics.put("bolt-2", compMetrics2);
    topologyMetrics.put("bolt-3", compMetrics3);

    ExecuteCountSensor sensor = mock(ExecuteCountSensor.class);
    when(sensor.get()).thenReturn(topologyMetrics);
    when(sensor.getMetricName()).thenReturn(METRIC_EXE_COUNT.text());

    ProcessingRateSkewDetector detector = new ProcessingRateSkewDetector(sensor, config);
    List<Symptom> symptoms = detector.detect();

    assertEquals(2, symptoms.size());
    for (Symptom symptom : symptoms) {
      if (symptom.getComponent().getName().equals("bolt-1")) {
        compMetrics1 = null;
      } else if (symptom.getComponent().getName().equals("bolt-2")) {
        compMetrics2 = null;
      } else if (symptom.getComponent().getName().equals("bolt-3")) {
        compMetrics3 = null;
      }
    }

    assertNull(compMetrics1);
    assertNull(compMetrics2);
    assertNotNull(compMetrics3);
  }
}
