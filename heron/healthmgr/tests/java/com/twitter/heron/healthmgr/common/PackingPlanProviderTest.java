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

package com.twitter.heron.healthmgr.common;

import java.util.HashSet;

import com.microsoft.dhalion.events.EventManager;

import org.junit.Assert;
import org.junit.Test;

import com.twitter.heron.healthmgr.common.HealthManagerEvents.TopologyUpdate;
import com.twitter.heron.packing.roundrobin.RoundRobinPacking;
import com.twitter.heron.proto.system.PackingPlans;
import com.twitter.heron.spi.packing.PackingPlan;
import com.twitter.heron.spi.statemgr.SchedulerStateManagerAdaptor;
import com.twitter.heron.spi.utils.PackingTestUtils;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class PackingPlanProviderTest {
  String topologyName = "topologyName";
  private EventManager eventManager = new EventManager();

  @Test
  public void fetchesAndCachesPackingFromStateMgr() {
    PackingPlans.PackingPlan proto
        = PackingTestUtils.testProtoPackingPlan(topologyName, new RoundRobinPacking());

    SchedulerStateManagerAdaptor adaptor = mock(SchedulerStateManagerAdaptor.class);
    when(adaptor.getPackingPlan(topologyName)).thenReturn(proto);

    PackingPlanProvider provider = new PackingPlanProvider(adaptor, eventManager, topologyName);
    PackingPlan packing = provider.get();
    Assert.assertEquals(1, packing.getContainers().size());

    // once fetched it is cached
    provider.get();
    verify(adaptor, times(1)).getPackingPlan(topologyName);
  }

  @Test
  public void refreshesPackingPlanOnUpdate() {
    PackingPlans.PackingPlan proto
        = PackingTestUtils.testProtoPackingPlan(topologyName, new RoundRobinPacking());

    SchedulerStateManagerAdaptor adaptor = mock(SchedulerStateManagerAdaptor.class);
    when(adaptor.getPackingPlan(topologyName)).thenReturn(proto);

    PackingPlanProvider provider = new PackingPlanProvider(adaptor, eventManager, topologyName);
    PackingPlan packing = provider.get();
    Assert.assertEquals(1, packing.getContainers().size());

    provider.onEvent(new TopologyUpdate());
    provider.get();
    verify(adaptor, times(2)).getPackingPlan(topologyName);
  }

  @Test
  public void providesBoltInstanceNames() {
    PackingPlans.PackingPlan proto
        = PackingTestUtils.testProtoPackingPlan(topologyName, new RoundRobinPacking());

    SchedulerStateManagerAdaptor adaptor = mock(SchedulerStateManagerAdaptor.class);
    when(adaptor.getPackingPlan(topologyName)).thenReturn(proto);

    PackingPlanProvider packing = new PackingPlanProvider(adaptor, eventManager, topologyName);

    String[] boltNames = packing.getBoltInstanceNames("testBolt");
    assertEquals(3, boltNames.length);

    HashSet<String> expectedBoltNames = new HashSet<>();
    expectedBoltNames.add("container_1_testBolt_3");
    expectedBoltNames.add("container_1_testBolt_4");
    expectedBoltNames.add("container_1_testBolt_5");

    for (String name : boltNames) {
      expectedBoltNames.remove(name);
    }
    assertEquals(0, expectedBoltNames.size());
  }
}
