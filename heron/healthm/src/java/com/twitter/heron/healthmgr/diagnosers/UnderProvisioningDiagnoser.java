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
import java.util.Map;
import java.util.logging.Logger;

import javax.inject.Inject;

import com.microsoft.dhalion.detector.Symptom;
import com.microsoft.dhalion.diagnoser.Diagnosis;
import com.microsoft.dhalion.metrics.ComponentMetrics;

import com.twitter.heron.healthmgr.sensors.BufferSizeSensor;

public class UnderProvisioningDiagnoser extends BaseDiagnoser {
  private static final Logger LOG = Logger.getLogger(SlowInstanceDiagnoser.class.getName());

  private final BufferSizeSensor bufferSizeSensor;
  private final double limit = 1000;

  @Inject
  UnderProvisioningDiagnoser(BufferSizeSensor bufferSizeSensor) {
    this.bufferSizeSensor = bufferSizeSensor;
  }

  @Override
  public Diagnosis diagnose(List<Symptom> symptoms) {
    List<Symptom> bpSymptoms = getBackPressureSymptoms(symptoms);
    if (bpSymptoms.isEmpty()) {
      // Since there is no back pressure, any more capacity is not needed
      return null;
    }

    List<Symptom> resultSymptoms = new ArrayList<>();
    for (Symptom backPressureSymptom : bpSymptoms) {
      ComponentMetrics bpMetricsData = backPressureSymptom.getMetrics();

      Map<String, ComponentMetrics> result = bufferSizeSensor.get(bpMetricsData.getName());
      ComponentMetrics bufferSizeData = result.get(bpMetricsData.getName());

      ComponentMetrics mergedData = ComponentMetrics.merge(bpMetricsData, bufferSizeData);

      ComponentBackpressureStats compStats = new ComponentBackpressureStats(mergedData);
      compStats.computeBufferSizeStats();

      // if all instances are reporting backpressure or if all instances have large pending buffers
      if (compStats.bufferSizeMin > limit
          && compStats.bufferSizeMin * 5 > compStats.bufferSizeMax) {
        LOG.info(String.format("UNDER_PROVISIONING: %s back-pressure(%s) and min buffer size: %s",
            mergedData.getName(), compStats.totalBackpressure, compStats.bufferSizeMin));
        resultSymptoms.add(Symptom.from(mergedData));
      }
    }
    return resultSymptoms.size() > 0 ? new Diagnosis(resultSymptoms) : null;
  }
}
