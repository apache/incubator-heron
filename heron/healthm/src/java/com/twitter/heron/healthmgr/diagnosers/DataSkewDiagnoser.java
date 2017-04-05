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

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.inject.Inject;

import com.microsoft.dhalion.api.IDiagnoser;
import com.microsoft.dhalion.metrics.ComponentMetricsData;
import com.microsoft.dhalion.metrics.InstanceMetricsData;
import com.microsoft.dhalion.symptom.ComponentSymptom;
import com.microsoft.dhalion.symptom.Diagnosis;

import com.twitter.heron.healthmgr.common.HealthManagerContstants;
import com.twitter.heron.healthmgr.detectors.BackPressureDetector;
import com.twitter.heron.healthmgr.sensors.ExecuteCountSensor;

public class DataSkewDiagnoser extends BaseDiagnoser {
  private final BackPressureDetector bpDetector;
  private final ExecuteCountSensor executeCountSensor;
  private double limit = 0.5;

  @Inject
  DataSkewDiagnoser(BackPressureDetector bpDetector, ExecuteCountSensor executeCountSensor) {
    this.bpDetector = bpDetector;
    this.executeCountSensor = executeCountSensor;
  }

  @Override
  public Diagnosis<ComponentSymptom> diagnose() {
    Collection<ComponentSymptom> backPressureSymptoms = bpDetector.detect();
    if (backPressureSymptoms.isEmpty()) {
      // no issue as there is no back pressure
      return null;
    }

    Set<ComponentSymptom> symptoms = new HashSet<>();
    for (ComponentSymptom backPressureSymptom : backPressureSymptoms) {
      ComponentMetricsData bpMetricsData = backPressureSymptom.getMetricsData();
      Map<String, ComponentMetricsData> result = executeCountSensor.get(bpMetricsData.getName());
      ComponentMetricsData executionCountData = result.get(bpMetricsData.getName());

      ComponentMetricsData mergedData =
          ComponentMetricsData.merge(bpMetricsData, executionCountData);

      ComponentBackPressureExeStats compStats = new ComponentBackPressureExeStats(mergedData);

      // if a minority of instances who are starting back pressures are also executing majority
      // of the tuples
      if (compStats.bpInstanceCount < compStats.totalInstances
          && compStats.avgNonBPExeCount < limit * compStats.avgBPExeCount) {
        symptoms.add(ComponentSymptom.from(mergedData));
      }
    }

    return symptoms.size() > 0 ? new Diagnosis<>(symptoms) : null;
  }
}
