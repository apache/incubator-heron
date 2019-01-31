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

package org.apache.heron.healthmgr.sensors;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import javax.inject.Inject;

import com.microsoft.dhalion.api.MetricsProvider;
import com.microsoft.dhalion.core.Measurement;
import com.microsoft.dhalion.core.MeasurementsTable;

import org.apache.heron.healthmgr.HealthPolicyConfig;
import org.apache.heron.healthmgr.common.PackingPlanProvider;
import org.apache.heron.healthmgr.common.PhysicalPlanProvider;

import static org.apache.heron.healthmgr.sensors.BaseSensor.MetricName.METRIC_WAIT_Q_SIZE;

public class BufferSizeSensor extends BaseSensor {
  private final MetricsProvider metricsProvider;
  private final PackingPlanProvider packingPlanProvider;
  private final PhysicalPlanProvider physicalPlanProvider;

  @Inject
  public BufferSizeSensor(HealthPolicyConfig policyConfig,
                          PackingPlanProvider packingPlanProvider,
                          PhysicalPlanProvider physicalPlanProvider,
                          MetricsProvider metricsProvider) {
    super(policyConfig, METRIC_WAIT_Q_SIZE.text(), BufferSizeSensor.class.getSimpleName());
    this.packingPlanProvider = packingPlanProvider;
    this.physicalPlanProvider = physicalPlanProvider;
    this.metricsProvider = metricsProvider;
  }

  /**
   * The buffer size as provided by tracker
   *
   * @return buffer size measurements
   */
  @Override
  public Collection<Measurement> fetch() {
    Collection<Measurement> result = new ArrayList<>();
    Instant now = context.checkpoint();

    List<String> boltComponents = physicalPlanProvider.getBoltNames();
    Duration duration = getDuration();

    for (String component : boltComponents) {
      String[] boltInstanceNames = packingPlanProvider.getBoltInstanceNames(component);
      for (String instance : boltInstanceNames) {
        String metric = getMetricName() + instance + MetricName.METRIC_WAIT_Q_SIZE_SUFFIX;

        Collection<Measurement> stmgrResult
            = metricsProvider.getMeasurements(now, duration, metric, COMPONENT_STMGR);
        if (stmgrResult.isEmpty()) {
          continue;
        }

        MeasurementsTable table = MeasurementsTable.of(stmgrResult).component(COMPONENT_STMGR);
        if (table.size() == 0) {
          continue;
        }
        double totalSize = table.type(metric).sum();

        Measurement measurement
            = new Measurement(component, instance, getMetricName(), now, totalSize);
        result.add(measurement);
      }
    }

    return result;
  }
}
