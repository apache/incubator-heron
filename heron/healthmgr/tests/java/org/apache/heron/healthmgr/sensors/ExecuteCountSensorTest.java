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

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import com.microsoft.dhalion.api.MetricsProvider;
import com.microsoft.dhalion.core.Measurement;
import com.microsoft.dhalion.core.MeasurementsTable;
import com.microsoft.dhalion.policy.PoliciesExecutor;

import org.junit.Test;

import org.apache.heron.healthmgr.common.PhysicalPlanProvider;

import static org.apache.heron.healthmgr.sensors.BaseSensor.DEFAULT_METRIC_DURATION;
import static org.apache.heron.healthmgr.sensors.BaseSensor.MetricName.METRIC_EXE_COUNT;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ExecuteCountSensorTest {
  @Test
  public void providesBoltExecutionCountMetrics() {
    Instant now = Instant.now();
    String metric = METRIC_EXE_COUNT.text();
    PhysicalPlanProvider topologyProvider = mock(PhysicalPlanProvider.class);
    when(topologyProvider.getBoltNames()).thenReturn(
        Arrays.asList(new String[]{"bolt-1", "bolt-2"}));

    MetricsProvider metricsProvider = mock(MetricsProvider.class);

    Collection<Measurement> result = new ArrayList<>();
    result.add(new Measurement("bolt-1", "container_1_bolt-1_1", metric, now, 123));
    result.add(new Measurement("bolt-1", "container_1_bolt-1_2", metric, now, 345));
    result.add(new Measurement("bolt-2", "container_1_bolt-2_3", metric, now, 321));
    result.add(new Measurement("bolt-2", "container_1_bolt-2_4", metric, now, 543));

    Collection<String> comps = Arrays.asList("bolt-1", "bolt-2");
    when(metricsProvider.getMeasurements(
        any(Instant.class),
        eq(DEFAULT_METRIC_DURATION), eq(Collections.singletonList(metric)), eq(comps)))
        .thenReturn(result);

    ExecuteCountSensor executeCountSensor
        = new ExecuteCountSensor(topologyProvider, null, metricsProvider);
    PoliciesExecutor.ExecutionContext context = mock(PoliciesExecutor.ExecutionContext.class);
    when(context.checkpoint()).thenReturn(now);
    executeCountSensor.initialize(context);

    Collection<Measurement> componentMetrics = executeCountSensor.fetch();
    assertEquals(4, componentMetrics.size());
    MeasurementsTable table = MeasurementsTable.of(componentMetrics);
    assertEquals(123, table.component("bolt-1").instance("container_1_bolt-1_1")
        .type(metric).sum(), 0.01);
    assertEquals(345, table.component("bolt-1").instance("container_1_bolt-1_2")
        .type(metric).sum(), 0.01);
    assertEquals(321, table.component("bolt-2").instance("container_1_bolt-2_3")
        .type(metric).sum(), 0.01);
    assertEquals(543, table.component("bolt-2").instance("container_1_bolt-2_4")
        .type(metric).sum(), 0.01);
  }
}
