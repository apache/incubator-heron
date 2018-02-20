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

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Logger;
import javax.inject.Inject;

import com.microsoft.dhalion.api.IDetector;
import com.microsoft.dhalion.core.Measurement;
import com.microsoft.dhalion.core.MeasurementsTable;
import com.microsoft.dhalion.core.Symptom;

import com.twitter.heron.healthmgr.HealthPolicyConfig;

import static com.twitter.heron.healthmgr.detectors.BaseDetector.SymptomType.SYMPTOM_BACK_PRESSURE;
import static com.twitter.heron.healthmgr.sensors.BaseSensor.MetricName.METRIC_BACK_PRESSURE;

public class BackPressureDetector implements IDetector {
  public static final String CONF_NOISE_FILTER = "BackPressureDetector.noiseFilterMillis";

  private static final Logger LOG = Logger.getLogger(BackPressureDetector.class.getName());
  private final int noiseFilterMillis;

  @Inject
  BackPressureDetector(HealthPolicyConfig policyConfig) {
    noiseFilterMillis = (int) policyConfig.getConfig(CONF_NOISE_FILTER, 20);
  }

  /**
   * Detects all components initiating backpressure above the configured limit. Normally there
   * will be only one component
   *
   * @return A collection of symptoms each one corresponding to a components with backpressure.
   */
  @Override
  public Collection<Symptom> detect(Collection<Measurement> measurements) {
    Collection<Symptom> result = new ArrayList<>();

    MeasurementsTable bpMetrics = MeasurementsTable.of(measurements).type(METRIC_BACK_PRESSURE
        .text());
    for (String component : bpMetrics.uniqueComponents()) {
      Set<String> addresses = new HashSet<>();
      double compBackPressure = bpMetrics.component(component).sum();
      if (compBackPressure > noiseFilterMillis) {
        LOG.info(String.format("Detected back pressure for %s, total back pressure is %f",
            component, compBackPressure));
        addresses.add(component);
        /*bpMetrics.component(component).uniqueInstances().forEach(instance -> {
          if (bpMetrics.instance(instance) != null && bpMetrics.instance(instance).sum() > 0) {
            addresses.add(instance);
          }
        });*/
        result.add(new Symptom(SYMPTOM_BACK_PRESSURE.text(), Instant.now(), addresses));
      }
    }
    return result;
  }
}
