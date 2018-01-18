// Copyright 2016 Twitter. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.twitter.heron.healthmgr.diagnosers;

import java.util.List;
import java.util.logging.Logger;

import com.microsoft.dhalion.detector.Symptom;
import com.microsoft.dhalion.diagnoser.Diagnosis;
import com.microsoft.dhalion.metrics.ComponentMetrics;

import static com.twitter.heron.healthmgr.diagnosers.BaseDiagnoser.DiagnosisName.DIAGNOSIS_BACKPRESSURE_INSTANCE;
import static com.twitter.heron.healthmgr.diagnosers.BaseDiagnoser.DiagnosisName.SYMPTOM_BACKPRESSURE_INSTANCE;

public class BackpressureInstanceDiagnoser extends BaseDiagnoser {
  private static final Logger LOG = Logger.getLogger(BackpressureInstanceDiagnoser.class.getName());

  @Override
  public Diagnosis diagnose(List<Symptom> symptoms) {
    List<Symptom> bpSymptoms = getBackPressureSymptoms(symptoms);
    if (bpSymptoms.isEmpty()) {
      // Since there is no back pressure, no action is needed
      return null;
    } else if (bpSymptoms.size() > 1) {
      // TODO handle cases where multiple detectors create back pressure symptom
      throw new IllegalStateException("Multiple back-pressure symptoms case");
    }
    ComponentMetrics bpMetrics = bpSymptoms.iterator().next().getComponent();
    Symptom resultSymptom = new Symptom(SYMPTOM_BACKPRESSURE_INSTANCE.text(), bpMetrics);
    return new Diagnosis(DIAGNOSIS_BACKPRESSURE_INSTANCE.text(), resultSymptom);
  }
}
