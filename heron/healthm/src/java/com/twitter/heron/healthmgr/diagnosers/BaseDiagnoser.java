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
import java.util.List;

import com.microsoft.dhalion.api.IDiagnoser;
import com.microsoft.dhalion.detector.Symptom;

import static com.twitter.heron.healthmgr.common.HealthMgrConstants.SYMPTOM_BACK_PRESSURE;
import static com.twitter.heron.healthmgr.common.HealthMgrConstants.SYMPTOM_LOAD_DISPARITY;

public abstract class BaseDiagnoser implements IDiagnoser {
  protected List<Symptom> getBackPressureSymptoms(List<Symptom> symptoms) {
    return getFilteredSymptoms(symptoms, SYMPTOM_BACK_PRESSURE);
  }

  protected List<Symptom> getLoadDisparitySymptoms(List<Symptom> symptoms) {
    return getFilteredSymptoms(symptoms, SYMPTOM_LOAD_DISPARITY);
  }

  private List<Symptom> getFilteredSymptoms(List<Symptom> symptoms, String type) {
    List<Symptom> result = new ArrayList<>();
    for (Symptom symptom : symptoms) {
      if (symptom.getName().equals(type)) {
        result.add(symptom);
      }
    }
    return result;
  }
}
