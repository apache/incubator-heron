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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.logging.Logger;

import javax.inject.Inject;

import com.microsoft.dhalion.core.Measurement;
import com.microsoft.dhalion.core.MeasurementsTable;
import com.microsoft.dhalion.core.Symptom;

import org.apache.commons.math3.stat.regression.SimpleRegression;

import org.apache.heron.healthmgr.HealthPolicyConfig;

import static org.apache.heron.healthmgr.detectors.BaseDetector.SymptomType.SYMPTOM_GROWING_WAIT_Q;
import static org.apache.heron.healthmgr.sensors.BaseSensor.MetricName.METRIC_WAIT_Q_SIZE;


public class GrowingWaitQueueDetector extends BaseDetector {
  static final String CONF_LIMIT
      = GrowingWaitQueueDetector.class.getSimpleName() + ".limit";

  private static final Logger LOG = Logger.getLogger(GrowingWaitQueueDetector.class.getName());
  private final double rateLimit;

  @Inject
  GrowingWaitQueueDetector(HealthPolicyConfig policyConfig) {
    rateLimit = (double) policyConfig.getConfig(CONF_LIMIT, 10.0);
  }

  /**
   * Detects all components unable to keep up with input load, hence having a growing pending buffer
   * or wait queue
   *
   * @return A collection of symptoms each one corresponding to a components executing slower
   * than input rate.
   */
  @Override
  public Collection<Symptom> detect(Collection<Measurement> measurements) {
    Collection<Symptom> result = new ArrayList<>();

    MeasurementsTable waitQueueMetrics
        = MeasurementsTable.of(measurements).type(METRIC_WAIT_Q_SIZE.text());

    for (String component : waitQueueMetrics.uniqueComponents()) {
      double maxSlope = computeWaitQueueSizeTrend(waitQueueMetrics.component(component));
      if (maxSlope > rateLimit) {
        LOG.info(String.format("Detected growing wait queues for %s, max rate %f",
            component, maxSlope));
        Collection<String> addresses = Collections.singletonList(component);
        result.add(new Symptom(SYMPTOM_GROWING_WAIT_Q.text(), context.checkpoint(), addresses));
      }
    }

    return result;
  }


  private double computeWaitQueueSizeTrend(MeasurementsTable metrics) {
    double maxSlope = 0;
    for (String instance : metrics.uniqueInstances()) {

      if (metrics.instance(instance) == null || metrics.instance(instance).size() < 3) {
        // missing of insufficient data for creating a trend line
        continue;
      }

      Collection<Measurement> measurements
          = metrics.instance(instance).sort(false, MeasurementsTable.SortKey.TIME_STAMP).get();
      SimpleRegression simpleRegression = new SimpleRegression(true);

      for (Measurement m : measurements) {
        simpleRegression.addData(m.instant().getEpochSecond(), m.value());
      }

      double slope = simpleRegression.getSlope();

      if (maxSlope < slope) {
        maxSlope = slope;
      }
    }
    return maxSlope;
  }
}
