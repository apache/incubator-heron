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

import static com.twitter.heron.healthmgr.sensors.BaseSensor.MetricName.METRIC_BACK_PRESSURE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class SlowInstanceDiagnoserTest {
  @Test
  public void failsIfNoBufferSizeDiaparity() {
    SlowInstanceDiagnoser diagnoser = new SlowInstanceDiagnoser();
    Diagnosis result = diagnoser.diagnose(TestUtils.createBpSymptomList(123));
    assertNull(result);
  }

  @Test
  public void diagnosis1of3SlowInstances() {
    List<Symptom> symptoms = TestUtils.createBpSymptomList(123, 0, 0);
    symptoms.add(TestUtils.createWaitQueueDisparitySymptom(1000, 20, 20));

    Diagnosis result = new SlowInstanceDiagnoser().diagnose(symptoms);
    assertEquals(1, result.getSymptoms().size());
    ComponentMetrics data = result.getSymptoms().values().iterator().next().getComponent();
    assertEquals(123,
        data.getMetricValueSum("container_1_bolt_0",METRIC_BACK_PRESSURE.text())
            .intValue());
  }

  @Test
  public void failIfInstanceWithBpHasSmallBuffer() {
    List<Symptom> symptoms = TestUtils.createBpSymptomList(123, 0, 0);
    symptoms.add(TestUtils.createWaitQueueDisparitySymptom(100, 500, 500));

    Diagnosis result = new SlowInstanceDiagnoser().diagnose(symptoms);
    assertNull(result);
  }
}
