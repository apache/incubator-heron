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


package com.twitter.heron.healthmgr.detectors;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import javax.inject.Inject;

import com.microsoft.dhalion.api.IDetector;
import com.microsoft.dhalion.detector.Symptom;
import com.microsoft.dhalion.metrics.ComponentMetrics;

import com.twitter.heron.healthmgr.HealthPolicyConfig;
import com.twitter.heron.healthmgr.common.ComponentMetricsHelper;
import com.twitter.heron.healthmgr.common.MetricsStats;
import com.twitter.heron.healthmgr.sensors.BufferSizeSensor;

import static com.twitter.heron.healthmgr.detectors.BaseDetector.SymptomName.SYMPTOM_LARGE_WAIT_Q;
import static com.twitter.heron.healthmgr.sensors.BaseSensor.MetricName.METRIC_BUFFER_SIZE;

public class LargeWaitQueueDetector implements IDetector {
  static final String CONF_SIZE_LIMIT = "LargeWaitQueueDetector.limit";

  private static final Logger LOG = Logger.getLogger(LargeWaitQueueDetector.class.getName());
  private final BufferSizeSensor pendingBufferSensor;
  private final int sizeLimit;

  @Inject
  LargeWaitQueueDetector(BufferSizeSensor pendingBufferSensor,
                         HealthPolicyConfig policyConfig) {
    this.pendingBufferSensor = pendingBufferSensor;
    sizeLimit = (int) policyConfig.getConfig(CONF_SIZE_LIMIT, 1000);
  }

  /**
   * Detects all components unable to keep up with input load, hence having a large pending buffer
   * or wait queue
   *
   * @return A collection of all components executing slower than input rate.
   */
  @Override
  public List<Symptom> detect() {
    ArrayList<Symptom> result = new ArrayList<>();

    Map<String, ComponentMetrics> bufferSizes = pendingBufferSensor.get();
    for (ComponentMetrics compMetrics : bufferSizes.values()) {
      ComponentMetricsHelper compStats = new ComponentMetricsHelper(compMetrics);
      MetricsStats stats = compStats.computeMinMaxStats(METRIC_BUFFER_SIZE.text());
      if (stats.getMetricMin() > sizeLimit) {
        LOG.info(String.format("Detected large wait queues for %s, smallest queue is %f",
            compMetrics.getName(), stats.getMetricMin()));
        result.add(new Symptom(SYMPTOM_LARGE_WAIT_Q.text(), compMetrics));
      }
    }

    return result;
  }
}
