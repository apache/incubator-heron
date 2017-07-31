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

import java.util.List;

import com.microsoft.dhalion.detector.Symptom;
import com.microsoft.dhalion.diagnoser.Diagnosis;

import org.junit.Test;

import com.twitter.heron.healthmgr.TestUtils;

import static com.twitter.heron.healthmgr.diagnosers.BaseDiagnoser.DiagnosisName.DIAGNOSIS_DATA_SKEW;
import static com.twitter.heron.healthmgr.sensors.BaseSensor.MetricName.METRIC_BACK_PRESSURE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class DataSkewDiagnoserTest {
  @Test
  public void failsIfNoDataSkewSymptom() {
    List<Symptom> symptoms = TestUtils.createBpSymptomList(123);
    Diagnosis result = new DataSkewDiagnoser().diagnose(symptoms);
    assertNull(result);
  }

  @Test
  public void diagnosis1DataSkewInstance() {
    List<Symptom> symptoms = TestUtils.createBpSymptomList(123, 0, 0);
    symptoms.add(TestUtils.createExeCountSymptom(5000, 2000, 2000));
    symptoms.add(TestUtils.createWaitQueueDisparitySymptom(10000, 500, 500));

    Diagnosis result = new DataSkewDiagnoser().diagnose(symptoms);
    assertNotNull(result);
    assertEquals(DIAGNOSIS_DATA_SKEW.text(), result.getName());
    assertEquals(1, result.getSymptoms().size());
    Symptom symptom = result.getSymptoms().values().iterator().next();

    assertEquals(123, symptom.getComponent()
        .getMetricValueSum("container_1_bolt_0", METRIC_BACK_PRESSURE.text()).intValue());
  }

  @Test
  public void diagnosisNoDataSkewLowBufferSize() {
    List<Symptom> symptoms = TestUtils.createBpSymptomList(123, 0, 0);
    symptoms.add(TestUtils.createExeCountSymptom(5000, 2000, 2000));
    symptoms.add(TestUtils.createWaitQueueDisparitySymptom(1, 500, 500));

    Diagnosis result = new DataSkewDiagnoser().diagnose(symptoms);
    assertNull(result);
  }

  @Test
  public void diagnosisNoDataSkewLowRate() {
    List<Symptom> symptoms = TestUtils.createBpSymptomList(123, 0, 0);
    symptoms.add(TestUtils.createExeCountSymptom(100, 2000, 2000));
    symptoms.add(TestUtils.createWaitQueueDisparitySymptom(10000, 500, 500));

    Diagnosis result = new DataSkewDiagnoser().diagnose(symptoms);
    assertNull(result);
  }
}
