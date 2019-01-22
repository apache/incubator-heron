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

import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import com.microsoft.dhalion.api.MetricsProvider;
import com.microsoft.dhalion.core.Measurement;
import com.microsoft.dhalion.core.MeasurementsTable;
import com.microsoft.dhalion.policy.PoliciesExecutor.ExecutionContext;

import org.junit.Test;

import org.apache.heron.healthmgr.HealthManagerMetrics;
import org.apache.heron.healthmgr.common.PackingPlanProvider;
import org.apache.heron.healthmgr.common.PhysicalPlanProvider;

import static org.apache.heron.healthmgr.sensors.BaseSensor.DEFAULT_METRIC_DURATION;
import static org.apache.heron.healthmgr.sensors.BaseSensor.MetricName.METRIC_BACK_PRESSURE;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class BackPressureSensorTest {
  @Test
  public void providesBackPressureMetricForBolts() throws IOException {
    PhysicalPlanProvider topologyProvider = mock(PhysicalPlanProvider.class);
    when(topologyProvider.getBoltNames()).thenReturn(
        Arrays.asList(new String[]{"bolt-1", "bolt-2"}));

    String[] boltIds = new String[]{"container_1_bolt-1_1",
        "container_2_bolt-2_22",
        "container_1_bolt-2_333"};

    PackingPlanProvider packingPlanProvider = mock(PackingPlanProvider.class);
    when(packingPlanProvider.getBoltInstanceNames("bolt-1"))
        .thenReturn(new String[]{boltIds[0]});
    when(packingPlanProvider.getBoltInstanceNames("bolt-2"))
        .thenReturn(new String[]{boltIds[1], boltIds[2]});

    MetricsProvider metricsProvider = mock(MetricsProvider.class);

    for (String boltId : boltIds) {
      String metric = METRIC_BACK_PRESSURE + boltId;
      // the back pressure sensor will return average bp per second, so multiply by duration
      registerStMgrInstanceMetricResponse(metricsProvider,
          metric,
          boltId.length() * DEFAULT_METRIC_DURATION.getSeconds());
    }


    HealthManagerMetrics publishingMetrics = mock(HealthManagerMetrics.class);
    BackPressureSensor backPressureSensor =
        new BackPressureSensor(
            packingPlanProvider, topologyProvider, null, metricsProvider, publishingMetrics);

    ExecutionContext context = mock(ExecutionContext.class);
    when(context.checkpoint()).thenReturn(Instant.now());
    backPressureSensor.initialize(context);

    Collection<Measurement> componentMetrics = backPressureSensor.fetch();
    assertEquals(3, componentMetrics.size());
    MeasurementsTable table = MeasurementsTable.of(componentMetrics);
    assertEquals(1, table.component("bolt-1").size());
    assertEquals(boltIds[0].length(), table.component("bolt-1").instance(boltIds[0])
        .type(METRIC_BACK_PRESSURE.text()).sum(), 0.01);

    assertEquals(2, table.component("bolt-2").size());
    assertEquals(boltIds[1].length(), table.component("bolt-2").instance(boltIds[1])
        .type(METRIC_BACK_PRESSURE.text()).sum(), 0.01);
    assertEquals(boltIds[2].length(), table.component("bolt-2").instance(boltIds[2])
        .type(METRIC_BACK_PRESSURE.text()).sum(), 0.01);
  }

  static void registerStMgrInstanceMetricResponse(MetricsProvider metricsProvider,
                                                  String metric,
                                                  long value) {
    Instant instant = Instant.ofEpochSecond(10);
    Measurement measurement = new Measurement("__stmgr__", "stmgr-1", metric, instant, value);
    Collection<Measurement> result = Collections.singletonList(measurement);

    when(metricsProvider.getMeasurements(
        any(Instant.class), eq(DEFAULT_METRIC_DURATION), eq(metric), eq("__stmgr__")))
        .thenReturn(result);
  }
}
