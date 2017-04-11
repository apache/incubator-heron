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
import static org.mockito.Mockito.mock;

public class SlowInstanceDiagnoserTest {
  @Test
  public void failsIfOnly1of1InstanceInBP() {
    BackPressureDetector bpDetector = createMockBackPressureDetector(123);
    BufferSizeSensor bufferSizeSensor = createMockBufferSizeSensor(1000);

    SlowInstanceDiagnoser diagnoser = new SlowInstanceDiagnoser(bpDetector, bufferSizeSensor);
    Diagnosis<ComponentSymptom> result = diagnoser.diagnose();
    assertNull(result);
  }

  @Test
  public void diagnoses1of3SlowInstances() {
    BackPressureDetector bpDetector = createMockBackPressureDetector(123, 0, 0);
    BufferSizeSensor bufferSizeSensor = createMockBufferSizeSensor(1000, 20, 20);

    SlowInstanceDiagnoser diagnoser = new SlowInstanceDiagnoser(bpDetector, bufferSizeSensor);
    Diagnosis<ComponentSymptom> result = diagnoser.diagnose();
    assertEquals(1, result.getSymptoms().size());
    ComponentMetricsData data = result.getSymptoms().iterator().next().getMetricsData();
    assertEquals(123,
        data.getMetricValue("container_1_bolt_0",
            HealthManagerContstants.METRIC_INSTANCE_BACK_PRESSURE).intValue());
  }

  @Test
  public void failDiagnosisIfBufferSizeDoNotDifferMuch() {
    BackPressureDetector bpDetector = createMockBackPressureDetector(123, 0, 0);
    BufferSizeSensor bufferSizeSensor = createMockBufferSizeSensor(1000, 500, 500);

    SlowInstanceDiagnoser diagnoser = new SlowInstanceDiagnoser(bpDetector, bufferSizeSensor);
    Diagnosis<ComponentSymptom> result = diagnoser.diagnose();
    assertNull(result);
  }

  public static BackPressureDetector createMockBackPressureDetector(int... bpValues) {
    return DataSkewDiagnoserTest.createMockBackPressureDetector(bpValues);
  }

  static BufferSizeSensor createMockBufferSizeSensor(double... values) {
    BufferSizeSensor exeSensor = mock(BufferSizeSensor.class);
    return DataSkewDiagnoserTest.getMockSensor(HealthManagerContstants.METRIC_BUFFER_SIZE,
        exeSensor,
        values);
  }
}
