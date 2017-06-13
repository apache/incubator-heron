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

import com.twitter.heron.healthmgr.common.ComponentMetricsHelper;
import com.twitter.heron.healthmgr.common.HealthMgrConstants;
import com.twitter.heron.healthmgr.sensors.BufferSizeSensor;

import static com.twitter.heron.healthmgr.common.HealthMgrConstants.METRIC_BACK_PRESSURE;
import static com.twitter.heron.healthmgr.common.HealthMgrConstants.METRIC_BUFFER_SIZE;

public class SlowInstanceDiagnoser extends BaseDiagnoser {
  private static final Logger LOG = Logger.getLogger(SlowInstanceDiagnoser.class.getName());

  private final BufferSizeSensor bufferSizeSensor;
  private double limit = 25;

  @Inject
  SlowInstanceDiagnoser(BufferSizeSensor bufferSizeSensor) {
    this.bufferSizeSensor = bufferSizeSensor;
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

    Map<String, ComponentMetrics> result = bufferSizeSensor.get(bpMetricsData.getName());
    ComponentMetrics bufferSizeData = result.get(bpMetricsData.getName());
    ComponentMetrics mergedData = ComponentMetrics.merge(bpMetricsData, bufferSizeData);

    ComponentMetricsHelper compStats = new ComponentMetricsHelper(mergedData);
    compStats.computeBpStats();
    compStats.computeBufferSizeStats();

    Symptom resultSymptom = null;
    if (compStats.getBufferSizeMax() > limit * compStats.getBufferSizeMin()) {
      // there is wide gap between max and min bufferSize, potential slow instance if the
      // instances who are starting back pressure are also executing less tuples

      for (InstanceMetrics boltMetrics : compStats.getBoltsWithBackpressure()) {
        double bpValue = boltMetrics.getMetricValue(METRIC_BACK_PRESSURE);
        double bufferSize = boltMetrics.getMetricValue(METRIC_BUFFER_SIZE);
        if (compStats.getBufferSizeMax() < bufferSize * 2) {
          LOG.info(String.format("SLOW: %s back-pressure(%s) and high buffer size: %s",
              boltMetrics.getName(), bpValue, bufferSize));
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
