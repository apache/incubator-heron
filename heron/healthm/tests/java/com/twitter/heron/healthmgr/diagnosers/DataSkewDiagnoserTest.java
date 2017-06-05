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

package com.twitter.heron.healthmgr.diagnosers;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.microsoft.dhalion.api.ISensor;
import com.microsoft.dhalion.detector.Symptom;
import com.microsoft.dhalion.diagnoser.Diagnosis;
import com.microsoft.dhalion.metrics.ComponentMetrics;
import com.microsoft.dhalion.metrics.InstanceMetrics;

import org.junit.Test;

import com.twitter.heron.healthmgr.common.HealthMgrConstants;
import com.twitter.heron.healthmgr.sensors.ExecuteCountSensor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DataSkewDiagnoserTest {
  @Test
  public void failsIfOnly1of1InstanceInBP() {
    List<Symptom> symptoms = UnderProvisioningDiagnoserTest.createBpSymptom(123);
    ExecuteCountSensor exeSensor = createMockExecuteCountSensor(5000);

    DataSkewDiagnoser diagnoser = new DataSkewDiagnoser(exeSensor);
    Diagnosis result = diagnoser.diagnose(symptoms);
    assertNull(result);
  }

  @Test
  public void diagnosis1DataSkewInstance() {
    List<Symptom> symptoms = UnderProvisioningDiagnoserTest.createBpSymptom(123, 0, 0);
    // set execute count above 100%, hence diagnosis should be under provisioning
    ExecuteCountSensor exeSensor = createMockExecuteCountSensor(5000, 2000, 2000);

    DataSkewDiagnoser diagnoser = new DataSkewDiagnoser(exeSensor);
    Diagnosis result = diagnoser.diagnose(symptoms);
    assertEquals(1, result.getSymptoms().size());
    ComponentMetrics data = result.getSymptoms().values().iterator().next().getComponent();
    assertEquals(123,
        data.getMetricValue("container_1_bolt_0", BaseDiagnoser.BACK_PRESSURE).intValue());
  }

  public static Symptom createBPSymptom(int... bpValues) {
    ComponentMetrics bpMetrics = new ComponentMetrics("bolt");
    for (int i = 0; i < bpValues.length; i++) {
      addInstanceMetric(bpMetrics, i, bpValues[i], BaseDiagnoser.BACK_PRESSURE);
    }
    return new Symptom(BaseDiagnoser.BACK_PRESSURE, bpMetrics);
  }

  public static ExecuteCountSensor createMockExecuteCountSensor(double... exeCounts) {
    ExecuteCountSensor exeSensor = mock(ExecuteCountSensor.class);
    return getMockSensor(HealthMgrConstants.METRIC_EXE_COUNT, exeSensor, exeCounts);
  }

  static <T extends ISensor> T getMockSensor(String metric, T sensor, double... values) {
    ComponentMetrics metrics = new ComponentMetrics("bolt");

    for (int i = 0; i < values.length; i++) {
      addInstanceMetric(metrics, i, values[i], metric);
    }

    Map<String, ComponentMetrics> resultMap = new HashMap<>();
    resultMap.put("bolt", metrics);
    when(sensor.get("bolt")).thenReturn(resultMap);
    return sensor;
  }

  static void addInstanceMetric(ComponentMetrics metrics, int i, double value, String metric) {
    InstanceMetrics instanceMetric = new InstanceMetrics("container_1_bolt_" + i, metric, value);
    metrics.addInstanceMetric(instanceMetric);
  }
}
