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

import javax.inject.Inject;

import com.microsoft.dhalion.detector.Symptom;
import com.microsoft.dhalion.diagnoser.Diagnosis;
import com.microsoft.dhalion.metrics.ComponentMetrics;
import com.microsoft.dhalion.metrics.InstanceMetrics;

import com.twitter.heron.healthmgr.sensors.ExecuteCountSensor;

public class DataSkewDiagnoser extends BaseDiagnoser {
  private static final Logger LOG = Logger.getLogger(DataSkewDiagnoser.class.getName());

  private final ExecuteCountSensor exeCountSensor;
  private double limit = 1.5;

  @Inject
  DataSkewDiagnoser(ExecuteCountSensor exeCountSensor) {
    this.exeCountSensor = exeCountSensor;
  }

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

    Symptom backPressureSymptom = bpSymptoms.iterator().next();

    ComponentMetrics bpMetricsData = backPressureSymptom.getComponent();
    if (bpMetricsData.getMetrics().size() <= 1) {
      // Need more than one instance for comparison
      return null;
    }

    Map<String, ComponentMetrics> result = exeCountSensor.get(bpMetricsData.getName());
    ComponentMetrics exeCountData = result.get(bpMetricsData.getName());
    ComponentMetrics mergedData = ComponentMetrics.merge(bpMetricsData, exeCountData);

    ComponentBackpressureStats compStats = new ComponentBackpressureStats(mergedData);
    compStats.computeExeCountStats();

    Symptom resultSymptom = null;
    if (compStats.exeCountMax > limit * compStats.exeCountMin) {
      // there is wide gap between max and min executionCount, potential skew if the instances
      // who are starting back pressures are also executing majority of the tuples
      for (InstanceMetrics boltMetrics : compStats.boltsWithBackpressure) {
        double exeCount = boltMetrics.getMetricValue(EXE_COUNT);
        double bpValue = boltMetrics.getMetricValue(BACK_PRESSURE);
        if (compStats.exeCountMax < 1.10 * exeCount) {
          LOG.info(String.format("DataSkew: %s back-pressure(%s) and high execution count: %s",
              boltMetrics.getName(), bpValue, exeCount));
          resultSymptom = backPressureSymptom;
          // TODO add other symptoms applicable to this diagnosis
        }
      }
    }

    return resultSymptom != null ?
        new Diagnosis(this.getClass().getSimpleName(), resultSymptom)
        : null;
  }
}
