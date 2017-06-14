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

import static com.twitter.heron.healthmgr.common.HealthMgrConstants.DIAGNOSIS_UNDER_PROVISIONING;
import static com.twitter.heron.healthmgr.common.HealthMgrConstants.SYMPTOM_UNDER_PROVISIONING;

public class UnderProvisioningDiagnoser extends BaseDiagnoser {
  private static final Logger LOG = Logger.getLogger(SlowInstanceDiagnoser.class.getName());

  @Override
  public Diagnosis diagnose(List<Symptom> symptoms) {
    List<Symptom> bpSymptoms = getBackPressureSymptoms(symptoms);
    Map<String, ComponentMetrics> largeWaitQComponents = getLargeWaitQComponents(symptoms);

    if (bpSymptoms.isEmpty() || largeWaitQComponents.isEmpty()) {
      // Since there is no back pressure or large pending queue, no action is needed
      return null;
    } else if (bpSymptoms.size() > 1) {
      // TODO handle cases where multiple detectors create back pressure symptom
      throw new IllegalStateException("Multiple back-pressure symptoms case");
    }
    ComponentMetrics bpMetrics = bpSymptoms.iterator().next().getComponent();

    // verify large buffer queue and back pressure for the same component exists
    ComponentMetrics pendingBufferMetrics = largeWaitQComponents.get(bpMetrics.getName());
    if (pendingBufferMetrics == null) {
      // wait Q for the component with back pressure is small. There is no under provisioning
      return null;
    }

    // all instances have large pending buffers and this comp is initiating back pressure.
    ComponentMetrics mergedData = ComponentMetrics.merge(bpMetrics, pendingBufferMetrics);
    ComponentMetricsHelper compStats = new ComponentMetricsHelper(mergedData);
    compStats.computeBpStats();
    compStats.computeBufferSizeStats();
    LOG.info(String.format("UNDER_PROVISIONING: %s back-pressure(%s) and min buffer size: %s",
        mergedData.getName(), compStats.getTotalBackpressure(), compStats.getBufferSizeMin()));


    Symptom resultSymptom = new Symptom(SYMPTOM_UNDER_PROVISIONING, mergedData);
    return new Diagnosis(DIAGNOSIS_UNDER_PROVISIONING, resultSymptom);
  }
}
