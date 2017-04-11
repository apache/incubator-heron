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

import com.microsoft.dhalion.metrics.ComponentMetricsData;
import com.microsoft.dhalion.symptom.ComponentSymptom;
import com.microsoft.dhalion.symptom.Diagnosis;

import org.junit.Test;

import com.twitter.heron.healthmgr.common.HealthManagerContstants;
import com.twitter.heron.healthmgr.detectors.BackPressureDetector;
import com.twitter.heron.healthmgr.sensors.BufferSizeSensor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class UnderProvisioningDiagnoserTest {
  @Test
  public void diagnosisWhen1Of1InstanceInBP() {
    BackPressureDetector bpDetector = createMockBackPressureDetector(123);
    BufferSizeSensor bufferSensor = createMockBufferSizeSensor(5000);

    UnderProvisioningDiagnoser diagnoser = new UnderProvisioningDiagnoser(bpDetector, bufferSensor);
    Diagnosis<ComponentSymptom> result = diagnoser.diagnose();
    validateDiagnosis(result);
  }

  @Test
  public void diagnosesSucceedsAllInstanceFullBuffers() {
    BackPressureDetector bpDetector = createMockBackPressureDetector(123, 0, 0);
    // set execute count within 20%, hence diagnosis should be under provisioning
    BufferSizeSensor bufferSensor = createMockBufferSizeSensor(5000, 4000, 3500);

    UnderProvisioningDiagnoser diagnoser = new UnderProvisioningDiagnoser(bpDetector, bufferSensor);
    Diagnosis<ComponentSymptom> result = diagnoser.diagnose();
    validateDiagnosis(result);
  }

  @Test
  public void diagnosisFailsIfBuffersDifferALot() {
    BackPressureDetector bpDetector = createMockBackPressureDetector(123, 0, 0);
    BufferSizeSensor bufferSensor = createMockBufferSizeSensor(5000, 1000, 1000);

    UnderProvisioningDiagnoser diagnoser = new UnderProvisioningDiagnoser(bpDetector, bufferSensor);
    Diagnosis<ComponentSymptom> result = diagnoser.diagnose();
    assertNull(result);
  }

  @Test
  public void diagnosesSucceedsIfBufferSizeIsNotKnown() {
    BackPressureDetector bpDetector = createMockBackPressureDetector(123, 0);
    BufferSizeSensor bufferSensor = createMockBufferSizeSensor();

    UnderProvisioningDiagnoser diagnoser = new UnderProvisioningDiagnoser(bpDetector, bufferSensor);
    Diagnosis<ComponentSymptom> result = diagnoser.diagnose();
    validateDiagnosis(result);
  }

  private void validateDiagnosis(Diagnosis<ComponentSymptom> result) {
    assertEquals(1, result.getSymptoms().size());
    ComponentMetricsData data = result.getSymptoms().iterator().next().getMetricsData();
    assertEquals(123,
        data.getMetricValue("container_1_bolt_0",
            HealthManagerContstants.METRIC_INSTANCE_BACK_PRESSURE).intValue());
  }

  public static BufferSizeSensor createMockBufferSizeSensor(double... bufferSizes) {
    return SlowInstanceDiagnoserTest.createMockBufferSizeSensor(bufferSizes);
  }

  private BackPressureDetector createMockBackPressureDetector(int... bpValues) {
    return DataSkewDiagnoserTest.createMockBackPressureDetector(bpValues);
  }
}
