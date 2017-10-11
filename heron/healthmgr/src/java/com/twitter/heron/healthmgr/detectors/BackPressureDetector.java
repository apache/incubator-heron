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
import com.twitter.heron.healthmgr.sensors.BackPressureSensor;

import static com.twitter.heron.healthmgr.detectors.BaseDetector.SymptomName.SYMPTOM_BACK_PRESSURE;

public class BackPressureDetector implements IDetector {
  static final String CONF_NOISE_FILTER = "BackPressureDetector.noiseFilterMillis";

  private static final Logger LOG = Logger.getLogger(BackPressureDetector.class.getName());
  private final BackPressureSensor bpSensor;
  private final int noiseFilterMillis;

  @Inject
  BackPressureDetector(BackPressureSensor bpSensor,
                       HealthPolicyConfig policyConfig) {
    this.bpSensor = bpSensor;
    noiseFilterMillis = (int) policyConfig.getConfig(CONF_NOISE_FILTER, 20);
  }

  /**
   * Detects all components initiating backpressure above the configured limit. Normally there
   * will be only one component
   *
   * @return A collection of all components causing backpressure.
   */
  @Override
  public List<Symptom> detect() {
    ArrayList<Symptom> result = new ArrayList<>();

    Map<String, ComponentMetrics> backpressureMetrics = bpSensor.get();
    for (ComponentMetrics compMetrics : backpressureMetrics.values()) {
      ComponentMetricsHelper compStats = new ComponentMetricsHelper(compMetrics);
      compStats.computeBpStats();
      if (compStats.getTotalBackpressure() > noiseFilterMillis) {
        LOG.info(String.format("Detected back pressure for %s, total back pressure is %f",
            compMetrics.getName(), compStats.getTotalBackpressure()));
        result.add(new Symptom(SYMPTOM_BACK_PRESSURE.text(), compMetrics));
      }
    }

    return result;
  }
}
