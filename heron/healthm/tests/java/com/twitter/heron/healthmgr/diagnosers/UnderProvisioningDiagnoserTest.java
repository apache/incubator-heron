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
import com.microsoft.dhalion.metrics.ComponentMetrics;

import org.junit.Test;

import com.twitter.heron.healthmgr.TestUtils;
import com.twitter.heron.healthmgr.common.HealthMgrConstants;
import com.twitter.heron.healthmgr.sensors.BufferSizeSensor;

import static com.twitter.heron.healthmgr.common.HealthMgrConstants.METRIC_BACK_PRESSURE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;

public class UnderProvisioningDiagnoserTest {
  @Test
  public void diagnosisWhen1Of1InstanceInBP() {
    List<Symptom> symptoms = TestUtils.createBpSymptomList(123);
    symptoms.add(TestUtils.createLargeWaitQSymptom(5000));
    Diagnosis result = new UnderProvisioningDiagnoser().diagnose(symptoms);
    validateDiagnosis(result);
  }

  @Test
  public void diagnosisSucceedsAllInstanceFullBuffers() {
    List<Symptom> symptoms = TestUtils.createBpSymptomList(123, 0, 0);
    // set execute count within 20%, hence diagnosis should be under provisioning
    symptoms.add(TestUtils.createLargeWaitQSymptom(5000, 4000, 3500));
    Diagnosis result = new UnderProvisioningDiagnoser().diagnose(symptoms);
    validateDiagnosis(result);
  }

  @Test
  public void diagnosisFailsIfBufferSizeIsNotKnown() {
    List<Symptom> symptoms = TestUtils.createBpSymptomList(123, 0);
    Diagnosis result = new UnderProvisioningDiagnoser().diagnose(symptoms);
    assertNull(result);
  }

  private void validateDiagnosis(Diagnosis result) {
    assertEquals(1, result.getSymptoms().size());
    ComponentMetrics data = result.getSymptoms().values().iterator().next().getComponent();
    assertEquals(123,
        data.getMetricValue("container_1_bolt_0", METRIC_BACK_PRESSURE).intValue());
  }
}
