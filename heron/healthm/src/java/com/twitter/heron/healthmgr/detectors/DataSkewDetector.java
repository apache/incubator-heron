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

import com.microsoft.dhalion.detector.Symptom;
import com.microsoft.dhalion.metrics.ComponentMetrics;

import com.twitter.heron.healthmgr.HealthPolicyConfig;
import com.twitter.heron.healthmgr.common.ComponentMetricsHelper;
import com.twitter.heron.healthmgr.sensors.ExecuteCountSensor;

import static com.twitter.heron.healthmgr.common.HealthMgrConstants.SYMPTOM_DATA_SKEW;

public class DataSkewDetector extends BaseDetector {
  public static final String CONF_SKEW_RATIO = "DataSkewDetector.skewRatio";

  private static final Logger LOG = Logger.getLogger(DataSkewDetector.class.getName());
  private final ExecuteCountSensor exeCountSensor;
  private final double skewRatio;

  @Inject
  DataSkewDetector(ExecuteCountSensor exeCountSensor,
                   HealthPolicyConfig policyConfig) {
    this.exeCountSensor = exeCountSensor;
    skewRatio = Double.valueOf(policyConfig.getConfig(CONF_SKEW_RATIO, "1.5"));
  }

  /**
   * @return A collection of all components with instances with vastly different execute counts.
   */
  @Override
  public List<Symptom> detect() {
    ArrayList<Symptom> result = new ArrayList<>();

    Map<String, ComponentMetrics> exeCountMetrics = exeCountSensor.get();
    for (ComponentMetrics compMetrics : exeCountMetrics.values()) {
      ComponentMetricsHelper compStats = new ComponentMetricsHelper(compMetrics);
      compStats.computeExeCountStats();
      if (compStats.getExeCountMax() > skewRatio * compStats.getExeCountMin()) {
        LOG.info(String.format("Detected data skew for %s, min = %f, max = %f",
            compMetrics.getName(), compStats.getExeCountMin(), compStats.getExeCountMax()));
        result.add(new Symptom(SYMPTOM_DATA_SKEW, compMetrics));
      }
    }

    return result;
  }
}
