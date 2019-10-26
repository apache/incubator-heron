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
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Logger;

import javax.inject.Inject;

import com.microsoft.dhalion.core.Measurement;
import com.microsoft.dhalion.core.MeasurementsTable;
import com.microsoft.dhalion.core.Symptom;

import org.apache.heron.healthmgr.HealthPolicyConfig;

import static org.apache.heron.healthmgr.detectors.BaseDetector.SymptomType.SYMPTOM_LARGE_WAIT_Q;
import static org.apache.heron.healthmgr.sensors.BaseSensor.MetricName.METRIC_WAIT_Q_SIZE;

public class LargeWaitQueueDetector extends BaseDetector {
  static final String CONF_SIZE_LIMIT = "LargeWaitQueueDetector.limit";

  private static final Logger LOG = Logger.getLogger(LargeWaitQueueDetector.class.getName());
  private final int sizeLimit;

  @Inject
  LargeWaitQueueDetector(HealthPolicyConfig policyConfig) {
    sizeLimit = (int) policyConfig.getConfig(CONF_SIZE_LIMIT, 1000);
  }

  /**
   * Detects all components having a large pending buffer or wait queue
   *
   * @return A collection of symptoms each one corresponding to components with
   * large wait queues.
   */
  @Override
  public Collection<Symptom> detect(Collection<Measurement> measurements) {

    Collection<Symptom> result = new ArrayList<>();

    MeasurementsTable waitQueueMetrics
        = MeasurementsTable.of(measurements).type(METRIC_WAIT_Q_SIZE.text());
    for (String component : waitQueueMetrics.uniqueComponents()) {
      Set<String> addresses = new HashSet<>();
      MeasurementsTable instanceMetrics = waitQueueMetrics.component(component);
      for (String instance : instanceMetrics.uniqueInstances()) {
        double avgWaitQSize = instanceMetrics.instance(instance).mean();
        if (avgWaitQSize > sizeLimit) {
          LOG.info(String.format("Detected large wait queues for instance"
              + "%s, smallest queue is + %f", instance, avgWaitQSize));
          addresses.add(instance);
        }
      }
      if (addresses.size() > 0) {
        result.add(new Symptom(SYMPTOM_LARGE_WAIT_Q.text(), context.checkpoint(), addresses));
      }
    }

    return result;
  }
}



