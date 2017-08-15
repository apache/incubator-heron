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
import java.util.Map;
import java.util.logging.Logger;

import com.microsoft.dhalion.detector.Symptom;
import com.microsoft.dhalion.diagnoser.Diagnosis;
import com.microsoft.dhalion.metrics.ComponentMetrics;

import com.twitter.heron.healthmgr.common.ComponentMetricsHelper;

import static com.twitter.heron.healthmgr.diagnosers.BaseDiagnoser.DiagnosisName.DIAGNOSIS_UNDER_PROVISIONING;
import static com.twitter.heron.healthmgr.diagnosers.BaseDiagnoser.DiagnosisName.SYMPTOM_UNDER_PROVISIONING;

public class UnderProvisioningDiagnoser extends BaseDiagnoser {
  private static final Logger LOG = Logger.getLogger(SlowInstanceDiagnoser.class.getName());

  @Override
  public Diagnosis diagnose(List<Symptom> symptoms) {
    List<Symptom> bpSymptoms = getBackPressureSymptoms(symptoms);
    Map<String, ComponentMetrics> processingRateSkewComponents =
        getProcessingRateSkewComponents(symptoms);
    Map<String, ComponentMetrics> waitQDisparityComponents = getWaitQDisparityComponents(symptoms);

    if (bpSymptoms.isEmpty() || !processingRateSkewComponents.isEmpty()
        || !waitQDisparityComponents.isEmpty()) {
      // Since there is no back pressure or similar processing rates
      // and buffer sizes, no action is needed
      return null;
    } else if (bpSymptoms.size() > 1) {
      // TODO handle cases where multiple detectors create back pressure symptom
      throw new IllegalStateException("Multiple back-pressure symptoms case");
    }
    ComponentMetrics bpMetrics = bpSymptoms.iterator().next().getComponent();

    ComponentMetricsHelper compStats = new ComponentMetricsHelper(bpMetrics);
    compStats.computeBpStats();
    LOG.info(String.format("UNDER_PROVISIONING: %s back-pressure(%s) and similar processing rates "
            + "and buffer sizes",
        bpMetrics.getName(), compStats.getTotalBackpressure()));


    Symptom resultSymptom = new Symptom(SYMPTOM_UNDER_PROVISIONING.text(), bpMetrics);
    return new Diagnosis(DIAGNOSIS_UNDER_PROVISIONING.text(), resultSymptom);
  }
}
