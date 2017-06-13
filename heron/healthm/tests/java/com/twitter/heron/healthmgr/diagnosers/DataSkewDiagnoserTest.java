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

import org.junit.Test;

import com.twitter.heron.healthmgr.TestUtils;

import static com.twitter.heron.healthmgr.common.HealthMgrConstants.METRIC_BACK_PRESSURE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.when;

public class DataSkewDiagnoserTest {
  @Test
  public void failsIfNoExeCountDisparity() {
    List<Symptom> symptoms = TestUtils.createBpSymptomList(123);
    Diagnosis result = new DataSkewDiagnoser().diagnose(symptoms);
    assertNull(result);
  }

  @Test
  public void diagnosis1DataSkewInstance() {
    List<Symptom> symptoms = TestUtils.createBpSymptomList(123, 0, 0);
    symptoms.add(TestUtils.createExeCountSymptom(5000, 2000, 2000));

    Diagnosis result = new DataSkewDiagnoser().diagnose(symptoms);
    assertEquals(1, result.getSymptoms().size());
    ComponentMetrics data = result.getSymptoms().values().iterator().next().getComponent();
    assertEquals(123,
        data.getMetricValue("container_1_bolt_0", METRIC_BACK_PRESSURE).intValue());
  }

  static <T extends ISensor> T getMockSensor(String metric, T sensor, double... values) {
    ComponentMetrics metrics = new ComponentMetrics("bolt");

    for (int i = 0; i < values.length; i++) {
      TestUtils.addInstanceMetric(metrics, i, values[i], metric);
    }

    Map<String, ComponentMetrics> resultMap = new HashMap<>();
    resultMap.put("bolt", metrics);
    when(sensor.get("bolt")).thenReturn(resultMap);
    return sensor;
  }
}
