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
import java.util.logging.Logger;

import javax.inject.Inject;

import com.microsoft.dhalion.metrics.ComponentMetricsData;
import com.microsoft.dhalion.metrics.InstanceMetricsData;
import com.microsoft.dhalion.symptom.ComponentSymptom;
import com.microsoft.dhalion.symptom.Diagnosis;

import com.twitter.heron.healthmgr.detectors.BackPressureDetector;
import com.twitter.heron.healthmgr.sensors.ExecuteCountSensor;

public class DataSkewDiagnoser extends BaseDiagnoser {
  private static final Logger LOG = Logger.getLogger(DataSkewDiagnoser.class.getName());

  private final BackPressureDetector bpDetector;
  private final ExecuteCountSensor exeCountSensor;
  private double limit = 1.5;

  @Inject
  DataSkewDiagnoser(BackPressureDetector bpDetector, ExecuteCountSensor exeCountSensor) {
    this.bpDetector = bpDetector;
    this.exeCountSensor = exeCountSensor;
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
      if (bpMetricsData.getMetrics().size() <= 1) {
        // Need more than one instance for comparison
        continue;
      }

      Map<String, ComponentMetricsData> result = exeCountSensor.get(bpMetricsData.getName());
      ComponentMetricsData exeCountData = result.get(bpMetricsData.getName());
      ComponentMetricsData mergedData = ComponentMetricsData.merge(bpMetricsData, exeCountData);

      ComponentBackpressureStats compStats = new ComponentBackpressureStats(mergedData);
      compStats.computeExeCountStats();

      if (compStats.exeCountMax > limit * compStats.exeCountMin) {
        // there is wide gap between max and min executionCount, potential skew if the instances
        // who are starting back pressures are also executing majority of the tuples
        for (InstanceMetricsData boltMetrics : compStats.boltsWithBackpressure) {
          int exeCount = boltMetrics.getMetricIntValue(EXE_COUNT);
          int bpValue = boltMetrics.getMetricIntValue(BACK_PRESSURE);
          if (compStats.exeCountMax < 1.10 * exeCount) {
            LOG.info(String.format("DataSkew: %s back-pressure(%s) and high execution count: %s",
                boltMetrics.getName(), bpValue, exeCount));
            symptoms.add(ComponentSymptom.from(mergedData));
          }
        }
      }
    }

    return symptoms.size() > 0 ? new Diagnosis<>(symptoms) : null;
  }
}
