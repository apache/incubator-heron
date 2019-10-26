/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.heron.healthmgr.diagnosers;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import com.microsoft.dhalion.api.IDiagnoser;
import com.microsoft.dhalion.core.Diagnosis;
import com.microsoft.dhalion.core.Symptom;
import com.microsoft.dhalion.policy.PoliciesExecutor.ExecutionContext;

import org.junit.Before;
import org.junit.Test;

import static org.apache.heron.healthmgr.detectors.BaseDetector.SymptomType.SYMPTOM_COMP_BACK_PRESSURE;
import static org.apache.heron.healthmgr.detectors.BaseDetector.SymptomType.SYMPTOM_PROCESSING_RATE_SKEW;
import static org.apache.heron.healthmgr.detectors.BaseDetector.SymptomType.SYMPTOM_WAIT_Q_SIZE_SKEW;
import static org.apache.heron.healthmgr.diagnosers.BaseDiagnoser.DiagnosisType.DIAGNOSIS_UNDER_PROVISIONING;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class UnderProvisioningDiagnoserTest {
  private final String comp = "comp";
  private IDiagnoser diagnoser;
  private Instant now = Instant.now();
  private ExecutionContext context;

  @Before
  public void initTestData() {
    now = Instant.now();

    context = mock(ExecutionContext.class);
    when(context.checkpoint()).thenReturn(now);

    diagnoser = new UnderProvisioningDiagnoser();
    diagnoser.initialize(context);
  }

  @Test
  public void diagnosisWhen1Of1InstanceInBP() {
    Collection<String> assign = Collections.singleton(comp);
    Symptom symptom = new Symptom(SYMPTOM_COMP_BACK_PRESSURE.text(), now, assign);
    Collection<Symptom> symptoms = Collections.singletonList(symptom);
    Collection<Diagnosis> result = diagnoser.diagnose(symptoms);
    validateDiagnosis(result);
  }

  @Test
  public void diagnosisFailsNotSimilarQueueSizes() {
    Collection<String> assign = Collections.singleton(comp);
    Symptom bpSymptom = new Symptom(SYMPTOM_COMP_BACK_PRESSURE.text(), now, assign);
    Symptom qDisparitySymptom = new Symptom(SYMPTOM_WAIT_Q_SIZE_SKEW.text(), now, assign);
    Collection<Symptom> symptoms = Arrays.asList(bpSymptom, qDisparitySymptom);

    Collection<Diagnosis> result = diagnoser.diagnose(symptoms);
    assertEquals(0, result.size());
  }

  @Test
  public void diagnosisFailsNotSimilarProcessingRates() {
    // TODO BP instance should be same as the one with high processing rate
    Collection<String> assign = Collections.singleton(comp);
    Symptom bpSymptom = new Symptom(SYMPTOM_COMP_BACK_PRESSURE.text(), now, assign);
    Symptom qDisparitySymptom = new Symptom(SYMPTOM_PROCESSING_RATE_SKEW.text(), now, assign);
    Collection<Symptom> symptoms = Arrays.asList(bpSymptom, qDisparitySymptom);

    Collection<Diagnosis> result = diagnoser.diagnose(symptoms);
    assertEquals(0, result.size());
  }

  private void validateDiagnosis(Collection<Diagnosis> result) {
    assertEquals(1, result.size());
    Diagnosis diagnoses = result.iterator().next();
    assertEquals(DIAGNOSIS_UNDER_PROVISIONING.text(), diagnoses.type());
    assertEquals(1, diagnoses.assignments().size());
    assertEquals(comp, diagnoses.assignments().iterator().next());
    // TODO
//    Assert.assertEquals(1, result.getSymptoms().size());
  }
}
