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

import java.util.Collection;

import com.microsoft.dhalion.api.MetricsProvider;
import com.microsoft.dhalion.core.Measurement;
import com.microsoft.dhalion.core.MeasurementsTable;

import org.junit.Test;

import com.twitter.heron.healthmgr.common.PackingPlanProvider;
import com.twitter.heron.healthmgr.common.TopologyProvider;
import com.twitter.heron.healthmgr.sensors.BaseSensor.MetricName;

import static com.twitter.heron.healthmgr.sensors.BaseSensor.MetricName.METRIC_BUFFER_SIZE;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class BufferSizeSensorTest {
  @Test
  public void providesBufferSizeMetricForBolts() {
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
      String metric = METRIC_BUFFER_SIZE
          + boltId + MetricName.METRIC_BUFFER_SIZE_SUFFIX;
      BackPressureSensorTest
          .registerStMgrInstanceMetricResponse(metricsProvider, metric, boltId.length());
    }

    BufferSizeSensor bufferSizeSensor =
        new BufferSizeSensor(null, packingPlanProvider, topologyProvider, metricsProvider);

    Collection<Measurement> componentMetrics = bufferSizeSensor.fetch();
    assertEquals(3, componentMetrics.size());

    MeasurementsTable table = MeasurementsTable.of(componentMetrics);
    assertEquals(1, table.component("bolt-1").size());
    assertEquals(boltIds[0].length(), table.component("bolt-1").instance(boltIds[0])
        .type(METRIC_BUFFER_SIZE.text()).sum(), 0.01);

    assertEquals(2, table.component("bolt-2").size());
    assertEquals(boltIds[1].length(), table.component("bolt-2").instance(boltIds[1])
        .type(METRIC_BUFFER_SIZE.text()).sum(), 0.01);
    assertEquals(boltIds[2].length(), table.component("bolt-2").instance(boltIds[2])
        .type(METRIC_BUFFER_SIZE.text()).sum(), 0.01);
  }
}
