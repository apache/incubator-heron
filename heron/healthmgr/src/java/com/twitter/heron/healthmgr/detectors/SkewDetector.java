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

import com.twitter.heron.healthmgr.common.ComponentMetricsHelper;
import com.twitter.heron.healthmgr.common.MetricsStats;
import com.twitter.heron.healthmgr.sensors.BaseSensor;

public class SkewDetector extends BaseDetector {
  private static final Logger LOG = Logger.getLogger(SkewDetector.class.getName());
  private final BaseSensor sensor;
  private final double skewRatio;
  private final BaseDetector.SymptomName symptomName;

  @Inject
  SkewDetector(BaseSensor sensor, double skewRatio, BaseDetector.SymptomName symptom) {
    this.sensor = sensor;
    this.skewRatio = skewRatio;
    this.symptomName = symptom;
  }

  /**
   * Detects components experiencing data skew, instances with vastly different execute counts.
   *
   * @return A collection of affected components
   */
  @Override
  public List<Symptom> detect() {
    ArrayList<Symptom> result = new ArrayList<>();

    Map<String, ComponentMetrics> metrics = sensor.get();
    for (ComponentMetrics compMetrics : metrics.values()) {
      ComponentMetricsHelper compStats = new ComponentMetricsHelper(compMetrics);
      MetricsStats stats = compStats.computeMinMaxStats(sensor.getMetricName());
      if (stats.getMetricMax() > skewRatio * stats.getMetricMin()) {
        LOG.info(String.format("Detected skew for %s, min = %f, max = %f",
            compMetrics.getName(), stats.getMetricMin(), stats.getMetricMax()));
        result.add(new Symptom(symptomName.text(), compMetrics));
      }
    }

    return result;
  }
}
