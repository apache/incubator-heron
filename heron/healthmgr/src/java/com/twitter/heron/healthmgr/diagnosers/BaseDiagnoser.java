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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.microsoft.dhalion.api.IDiagnoser;
import com.microsoft.dhalion.detector.Symptom;
import com.microsoft.dhalion.metrics.ComponentMetrics;

import com.twitter.heron.healthmgr.detectors.BaseDetector.SymptomName;

import static com.twitter.heron.healthmgr.detectors.BaseDetector.SymptomName.SYMPTOM_BACK_PRESSURE;
import static com.twitter.heron.healthmgr.detectors.BaseDetector.SymptomName.SYMPTOM_PROCESSING_RATE_SKEW;
import static com.twitter.heron.healthmgr.detectors.BaseDetector.SymptomName.SYMPTOM_WAIT_Q_DISPARITY;

abstract class BaseDiagnoser implements IDiagnoser {
  enum DiagnosisName {
    SYMPTOM_UNDER_PROVISIONING("SYMPTOM_UNDER_PROVISIONING"),
    SYMPTOM_DATA_SKEW("SYMPTOM_DATA_SKEW"),
    SYMPTOM_SLOW_INSTANCE("SYMPTOM_SLOW_INSTANCE"),

    DIAGNOSIS_UNDER_PROVISIONING(UnderProvisioningDiagnoser.class.getSimpleName()),
    DIAGNOSIS_SLOW_INSTANCE(SlowInstanceDiagnoser.class.getSimpleName()),
    DIAGNOSIS_DATA_SKEW(DataSkewDiagnoser.class.getSimpleName());

    private String text;

    DiagnosisName(String name) {
      this.text = name;
    }

    public String text() {
      return text;
    }

    @Override
    public String toString() {
      return text();
    }
  }

  List<Symptom> getBackPressureSymptoms(List<Symptom> symptoms) {
    return getFilteredSymptoms(symptoms, SYMPTOM_BACK_PRESSURE);
  }

  Map<String, ComponentMetrics> getProcessingRateSkewComponents(List<Symptom> symptoms) {
    return getFilteredComponents(symptoms, SYMPTOM_PROCESSING_RATE_SKEW);
  }

  Map<String, ComponentMetrics> getWaitQDisparityComponents(List<Symptom> symptoms) {
    return getFilteredComponents(symptoms, SYMPTOM_WAIT_Q_DISPARITY);
  }

  private List<Symptom> getFilteredSymptoms(List<Symptom> symptoms, SymptomName type) {
    List<Symptom> result = new ArrayList<>();
    for (Symptom symptom : symptoms) {
      if (symptom.getName().equals(type.text())) {
        result.add(symptom);
      }
    }
    return result;
  }

  private Map<String, ComponentMetrics> getFilteredComponents(List<Symptom> symptoms,
                                                              SymptomName type) {
    Map<String, ComponentMetrics> result = new HashMap<>();
    for (Symptom symptom : symptoms) {
      if (symptom.getName().equals(type.text())) {
        result.putAll(symptom.getComponents());
      }
    }
    return result;
  }
}
