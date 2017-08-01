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
import com.microsoft.dhalion.metrics.InstanceMetrics;

import com.twitter.heron.healthmgr.common.ComponentMetricsHelper;
import com.twitter.heron.healthmgr.common.MetricsStats;

import static com.twitter.heron.healthmgr.diagnosers.BaseDiagnoser.DiagnosisName.DIAGNOSIS_SLOW_INSTANCE;
import static com.twitter.heron.healthmgr.diagnosers.BaseDiagnoser.DiagnosisName.SYMPTOM_SLOW_INSTANCE;
import static com.twitter.heron.healthmgr.sensors.BaseSensor.MetricName.METRIC_BACK_PRESSURE;
import static com.twitter.heron.healthmgr.sensors.BaseSensor.MetricName.METRIC_BUFFER_SIZE;

public class SlowInstanceDiagnoser extends BaseDiagnoser {
  private static final Logger LOG = Logger.getLogger(SlowInstanceDiagnoser.class.getName());

  @Override
  public Diagnosis diagnose(List<Symptom> symptoms) {
    List<Symptom> bpSymptoms = getBackPressureSymptoms(symptoms);
    Map<String, ComponentMetrics> processingRateSkewComponents =
        getProcessingRateSkewComponents(symptoms);
    Map<String, ComponentMetrics> waitQDisparityComponents = getWaitQDisparityComponents(symptoms);

    if (bpSymptoms.isEmpty() || waitQDisparityComponents.isEmpty()
        || !processingRateSkewComponents.isEmpty()) {
      // Since there is no back pressure or disparate wait count or similar
      // execution count, no action is needed
      return null;
    } else if (bpSymptoms.size() > 1) {
      // TODO handle cases where multiple detectors create back pressure symptom
      throw new IllegalStateException("Multiple back-pressure symptoms case");
    }
    ComponentMetrics bpMetrics = bpSymptoms.iterator().next().getComponent();

    // verify wait Q disparity and back pressure for the same component exists
    ComponentMetrics pendingBufferMetrics = waitQDisparityComponents.get(bpMetrics.getName());
    if (pendingBufferMetrics == null) {
      // no wait Q disparity for the component with back pressure. There is no slow instance
      return null;
    }

    ComponentMetrics mergedData = ComponentMetrics.merge(bpMetrics, pendingBufferMetrics);
    ComponentMetricsHelper compStats = new ComponentMetricsHelper(mergedData);
    compStats.computeBpStats();
    MetricsStats bufferStats = compStats.computeMinMaxStats(METRIC_BUFFER_SIZE);

    Symptom resultSymptom = null;
    for (InstanceMetrics boltMetrics : compStats.getBoltsWithBackpressure()) {
      double bufferSize = boltMetrics.getMetricValueSum(METRIC_BUFFER_SIZE.text());
      double bpValue = boltMetrics.getMetricValueSum(METRIC_BACK_PRESSURE.text());
      if (bufferStats.getMetricMax() < bufferSize * 2) {
        LOG.info(String.format("SLOW: %s back-pressure(%s) and high buffer size: %s "
                + "and similar processing rates",
            boltMetrics.getName(), bpValue, bufferSize));
        resultSymptom = new Symptom(SYMPTOM_SLOW_INSTANCE.text(), mergedData);
      }
    }

    return resultSymptom != null
        ? new Diagnosis(DIAGNOSIS_SLOW_INSTANCE.text(), resultSymptom) : null;
  }
}
