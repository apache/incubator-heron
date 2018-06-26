/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.heron.healthmgr.detectors;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.logging.Logger;

import javax.inject.Inject;

import com.microsoft.dhalion.core.Measurement;
import com.microsoft.dhalion.core.MeasurementsTable;
import com.microsoft.dhalion.core.Symptom;

import org.apache.heron.healthmgr.HealthManagerMetrics;
import org.apache.heron.healthmgr.HealthPolicyConfig;

import static org.apache.heron.healthmgr.detectors.BaseDetector.SymptomType.SYMPTOM_COMP_BACK_PRESSURE;
import static org.apache.heron.healthmgr.detectors.BaseDetector.SymptomType.SYMPTOM_INSTANCE_BACK_PRESSURE;
import static org.apache.heron.healthmgr.sensors.BaseSensor.MetricName.METRIC_BACK_PRESSURE;

public class BackPressureDetector extends BaseDetector {
  public static final String BACK_PRESSURE_DETECTOR = "BackPressureDetector";
  static final String CONF_NOISE_FILTER = "BackPressureDetector.noiseFilterMillis";

  private static final Logger LOG = Logger.getLogger(BackPressureDetector.class.getName());
  private final int noiseFilterMillis;
  private HealthManagerMetrics publishingMetrics;

  @Inject
  BackPressureDetector(HealthPolicyConfig policyConfig,
                       HealthManagerMetrics publishingMetrics) {
    noiseFilterMillis = (int) policyConfig.getConfig(CONF_NOISE_FILTER, 20);
    this.publishingMetrics = publishingMetrics;
  }

  /**
   * Detects all components initiating backpressure above the configured limit. Normally there
   * will be only one component
   *
   * @return A collection of symptoms each one corresponding to a components with backpressure.
   */
  @Override
  public Collection<Symptom> detect(Collection<Measurement> measurements) {
    publishingMetrics.executeDetectorIncr(BACK_PRESSURE_DETECTOR);

    Collection<Symptom> result = new ArrayList<>();
    Instant now = context.checkpoint();

    MeasurementsTable bpMetrics
        = MeasurementsTable.of(measurements).type(METRIC_BACK_PRESSURE.text());
    for (String component : bpMetrics.uniqueComponents()) {
      double compBackPressure = bpMetrics.component(component).sum();
      if (compBackPressure > noiseFilterMillis) {
        LOG.info(String.format("Detected component back-pressure for %s, total back pressure is %f",
            component, compBackPressure));
        List<String> addresses = Collections.singletonList(component);
        result.add(new Symptom(SYMPTOM_COMP_BACK_PRESSURE.text(), now, addresses));
      }
    }
    for (String instance : bpMetrics.uniqueInstances()) {
      double totalBP = bpMetrics.instance(instance).sum();
      if (totalBP > noiseFilterMillis) {
        LOG.info(String.format("Detected instance back-pressure for %s, total back pressure is %f",
            instance, totalBP));
        List<String> addresses = Collections.singletonList(instance);
        result.add(new Symptom(SYMPTOM_INSTANCE_BACK_PRESSURE.text(), now, addresses));
      }
    }
    return result;
  }
}
