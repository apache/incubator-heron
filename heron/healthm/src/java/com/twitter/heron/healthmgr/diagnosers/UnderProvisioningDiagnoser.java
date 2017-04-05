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

import com.microsoft.dhalion.metrics.ComponentMetricsData;
import com.microsoft.dhalion.symptom.ComponentSymptom;
import com.microsoft.dhalion.symptom.Diagnosis;

import com.twitter.heron.healthmgr.detectors.BackPressureDetector;
import com.twitter.heron.healthmgr.sensors.ExecuteCountSensor;

public class UnderProvisioningDiagnoser extends BaseDiagnoser {
  private final BackPressureDetector bpDetector;
  private final ExecuteCountSensor executeCountSensor;
  private final double limit;

  @Inject
  UnderProvisioningDiagnoser(BackPressureDetector bpDetector,
                             ExecuteCountSensor executeCountSensor) {
    this.bpDetector = bpDetector;
    this.executeCountSensor = executeCountSensor;
    limit = 0.2;
  }

  @Override
  public Diagnosis<ComponentSymptom> diagnose() {
    Collection<ComponentSymptom> backPressureSymptoms = bpDetector.detect();
    if (backPressureSymptoms.isEmpty()) {
      // Since there is no back pressure, any more capacity is not needed
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
      double executeDiff = Math.abs(compStats.avgBPExeCount - compStats.avgNonBPExeCount);
      double lowerAvgBPExecuteCount =
          compStats.avgBPExeCount > compStats.avgNonBPExeCount ?
              compStats.avgNonBPExeCount : compStats.avgBPExeCount;

      // if all instances are starting backpressure or the diff
      if (compStats.bpInstanceCount == compStats.totalInstances
          || executeDiff < limit * lowerAvgBPExecuteCount) {
        symptoms.add(ComponentSymptom.from(mergedData));
      }
    }

    return symptoms.size() > 0 ? new Diagnosis<>(symptoms) : null;
  }
}
