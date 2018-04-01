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

import java.util.Map;

import com.microsoft.dhalion.api.MetricsProvider;
import com.microsoft.dhalion.metrics.ComponentMetrics;

import org.junit.Test;

import com.twitter.heron.healthmgr.common.PackingPlanProvider;
import com.twitter.heron.healthmgr.common.TopologyProvider;
import com.twitter.heron.healthmgr.sensors.BaseSensor.MetricName;

import static com.twitter.heron.healthmgr.sensors.BaseSensor.DEFAULT_METRIC_DURATION;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class BackPressureSensorTest {
  @Test
  public void providesBackPressureMetricForBolts() {
    TopologyProvider topologyProvider = mock(TopologyProvider.class);
    when(topologyProvider.getBoltNames()).thenReturn(new String[]{"bolt-1", "bolt-2"});

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
      String metric = MetricName.METRIC_BACK_PRESSURE + boltId;
      // the back pressure sensor will return average bp per second, so multiply by duration
      BufferSizeSensorTest.registerStMgrInstanceMetricResponse(metricsProvider,
          metric,
          boltId.length() * DEFAULT_METRIC_DURATION.getSeconds());
    }

    BackPressureSensor backPressureSensor =
        new BackPressureSensor(packingPlanProvider, topologyProvider, null, metricsProvider);

    Map<String, ComponentMetrics> componentMetrics = backPressureSensor.get();
    assertEquals(2, componentMetrics.size());

    assertEquals(1, componentMetrics.get("bolt-1").getMetrics().size());
    assertEquals(boltIds[0].length(), componentMetrics.get("bolt-1").getMetrics(boltIds[0])
        .getMetricValueSum(MetricName.METRIC_BACK_PRESSURE.text()).intValue());

    assertEquals(2, componentMetrics.get("bolt-2").getMetrics().size());
    assertEquals(boltIds[1].length(), componentMetrics.get("bolt-2").getMetrics(boltIds[1])
        .getMetricValueSum(MetricName.METRIC_BACK_PRESSURE.text()).intValue());
    assertEquals(boltIds[2].length(), componentMetrics.get("bolt-2").getMetrics(boltIds[2])
        .getMetricValueSum(MetricName.METRIC_BACK_PRESSURE.text()).intValue());
  }
}
