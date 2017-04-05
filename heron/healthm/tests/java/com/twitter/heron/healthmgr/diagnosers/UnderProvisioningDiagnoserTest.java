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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import com.microsoft.dhalion.metrics.ComponentMetricsData;
import com.microsoft.dhalion.symptom.ComponentSymptom;
import com.microsoft.dhalion.symptom.Diagnosis;

import org.junit.Test;

import com.twitter.heron.healthmgr.common.HealthManagerContstants;
import com.twitter.heron.healthmgr.detectors.BackPressureDetector;
import com.twitter.heron.healthmgr.sensors.ExecuteCountSensor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class UnderProvisioningDiagnoserTest {
  @Test
  public void diagnosesCompWhen1Of1InstanceInBP() {
    BackPressureDetector bpDetector = createMockBackPressureDetector(123);
    ExecuteCountSensor exeSensor = createMockExecuteCountSensor(5000);

    UnderProvisioningDiagnoser diagnoser = new UnderProvisioningDiagnoser(bpDetector, exeSensor);
    Diagnosis<ComponentSymptom> result = diagnoser.diagnose();
    assertEquals(1, result.getSymptoms().size());
    ComponentMetricsData data = result.getSymptoms().iterator().next().getMetricsData();
    assertEquals(123,
        data.getMetricValue("container_1_bolt_0",
            HealthManagerContstants.METRIC_INSTANCE_BACK_PRESSURE).intValue());
  }

  @Test
  public void comparableExeCountDiagnosed1Of3InstanceInBP() {
    BackPressureDetector bpDetector = createMockBackPressureDetector(123, 0, 0);
    // set execute count within 20%, hence diagnosis should be under provisioning
    ExecuteCountSensor exeSensor = createMockExecuteCountSensor(5000, 4201, 4201);

    UnderProvisioningDiagnoser diagnoser = new UnderProvisioningDiagnoser(bpDetector, exeSensor);
    Diagnosis<ComponentSymptom> result = diagnoser.diagnose();
    assertEquals(1, result.getSymptoms().size());
    ComponentMetricsData data = result.getSymptoms().iterator().next().getMetricsData();
    assertEquals(123,
        data.getMetricValue("container_1_bolt_0",
            HealthManagerContstants.METRIC_INSTANCE_BACK_PRESSURE).intValue());
  }

  @Test
  public void dataSkewIsNotDiagnosed1Of3InstanceInBP() {
    BackPressureDetector bpDetector = createMockBackPressureDetector(123, 0, 0);
    // set execute count above 20%, hence diagnosis should NOT be under provisioning
    ExecuteCountSensor exeSensor = createMockExecuteCountSensor(5000, 4100, 4100);

    UnderProvisioningDiagnoser diagnoser = new UnderProvisioningDiagnoser(bpDetector, exeSensor);
    Diagnosis<ComponentSymptom> result = diagnoser.diagnose();
    assertNull(result);
  }

  public static ExecuteCountSensor createMockExecuteCountSensor(int... exeCounts) {
    return DataSkewDiagnoserTest.createMockExecuteCountSensor(exeCounts);
  }

  private BackPressureDetector createMockBackPressureDetector(int... bpValues) {
    return DataSkewDiagnoserTest.createMockBackPressureDetector(bpValues);
  }
}
