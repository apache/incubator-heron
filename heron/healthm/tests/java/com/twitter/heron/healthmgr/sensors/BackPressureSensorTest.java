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

package com.twitter.heron.healthmgr.sensors;

import java.util.HashMap;
import java.util.Map;

import com.microsoft.dhalion.api.MetricsProvider;
import com.microsoft.dhalion.metrics.ComponentMetricsData;
import com.microsoft.dhalion.metrics.InstanceMetricsData;

import org.junit.Test;

import com.twitter.heron.healthmgr.common.HealthManagerContstants;
import com.twitter.heron.healthmgr.common.PackingPlanProvider;
import com.twitter.heron.healthmgr.common.TopologyProvider;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class BackPressureSensorTest {
  @Test
  public void providesBackPressureMetricForBolts() {
    TopologyProvider topologyProvider = mock(TopologyProvider.class);
    when(topologyProvider.getBoltNames()).thenReturn(new String[]{"bolt-1", "bolt-2"});

    PackingPlanProvider packingPlanProvider = mock(PackingPlanProvider.class);
    when(packingPlanProvider.getBoltInstanceNames("bolt-1"))
        .thenReturn(new String[]{"container_1_bolt-1_1"});
    when(packingPlanProvider.getBoltInstanceNames("bolt-2"))
        .thenReturn(new String[]{"container_1_bolt-2_3", "container_2_bolt-2_2"});

    MetricsProvider metricsProvider = mock(MetricsProvider.class);

    registerBackPressureResponse(metricsProvider, "stmgr-1", "container_1_bolt-1_1", 123);
    registerBackPressureResponse(metricsProvider, "stmgr-2", "container_2_bolt-2_2", 234);
    registerBackPressureResponse(metricsProvider, "stmgr-1", "container_1_bolt-2_3", 345);

    BackPressureSensor backPressureSensor =
        new BackPressureSensor(packingPlanProvider, topologyProvider, metricsProvider);

    Map<String, ComponentMetricsData> componentMetrics = backPressureSensor.get();
    assertEquals(2, componentMetrics.size());

    assertEquals(1, componentMetrics.get("bolt-1").getMetrics().size());
    assertEquals(123,
        componentMetrics.get("bolt-1").getMetrics("container_1_bolt-1_1")
            .getMetricIntValue(HealthManagerContstants.METRIC_INSTANCE_BACK_PRESSURE));

    assertEquals(2, componentMetrics.get("bolt-2").getMetrics().size());
    assertEquals(234,
        componentMetrics.get("bolt-2").getMetrics("container_2_bolt-2_2")
            .getMetricIntValue(HealthManagerContstants.METRIC_INSTANCE_BACK_PRESSURE));
    assertEquals(345,
        componentMetrics.get("bolt-2").getMetrics("container_1_bolt-2_3")
            .getMetricIntValue(HealthManagerContstants.METRIC_INSTANCE_BACK_PRESSURE));
  }

  private void registerBackPressureResponse(MetricsProvider metricsProvider,
                                            String stmgr,
                                            String metric,
                                            int value) {
    metric = HealthManagerContstants.METRIC_INSTANCE_BACK_PRESSURE + metric;

    Map<String, ComponentMetricsData> result = new HashMap<>();
    ComponentMetricsData metrics = new ComponentMetricsData("__stmgr__");
    InstanceMetricsData instanceMetrics = new InstanceMetricsData(stmgr);
    instanceMetrics.addMetric(metric, value);
    metrics.addInstanceMetric(instanceMetrics);
    result.put("__stmgr__", metrics);

    when(metricsProvider.getComponentMetrics(metric, 60, "__stmgr__"))
        .thenReturn(result);
  }

  //TODO
  void emptyMetricTest() {
//    {"status": "success", "executiontime": 0.30780792236328125, "message": "", "version": "delayedTopology", "result": {"metrics": {}, "interval": 0, "component": "split"}}
  }
}
