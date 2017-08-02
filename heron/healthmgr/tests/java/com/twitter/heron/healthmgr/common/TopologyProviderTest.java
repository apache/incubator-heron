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

import java.util.HashMap;
import java.util.Map;

import com.microsoft.dhalion.events.EventManager;

import org.junit.Assert;
import org.junit.Test;

import com.twitter.heron.api.Config;
import com.twitter.heron.api.generated.TopologyAPI.Topology;
import com.twitter.heron.healthmgr.common.HealthManagerEvents.TopologyUpdate;
import com.twitter.heron.proto.system.PhysicalPlans;
import com.twitter.heron.spi.statemgr.SchedulerStateManagerAdaptor;
import com.twitter.heron.spi.utils.TopologyTests;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TopologyProviderTest {
  private String topology = "topology";
  private EventManager eventManager = new EventManager();

  @Test
  public void fetchesAndCachesPackingFromStateMgr() {
    SchedulerStateManagerAdaptor adaptor = getMockSchedulerStateManagerAdaptor();
    TopologyProvider provider = new TopologyProvider(adaptor, eventManager, topology);
    Assert.assertEquals(2, provider.get().getBoltsCount());

    // once fetched it is cached
    provider.get();
    verify(adaptor, times(1)).getPhysicalPlan(topology);
  }

  @Test
  public void refreshesPackingPlanOnUpdate() {
    SchedulerStateManagerAdaptor adaptor = getMockSchedulerStateManagerAdaptor();

    TopologyProvider provider = new TopologyProvider(adaptor, eventManager, topology);
    Assert.assertEquals(2, provider.get().getBoltsCount());

    // once fetched it is cached
    provider.onEvent(new TopologyUpdate());
    provider.get();
    verify(adaptor, times(2)).getPhysicalPlan(topology);
  }

  private SchedulerStateManagerAdaptor getMockSchedulerStateManagerAdaptor() {
    Topology testTopology
        = TopologyTests.createTopology(topology, new Config(), getSpouts(), getBolts());
    PhysicalPlans.PhysicalPlan plan =
        PhysicalPlans.PhysicalPlan.newBuilder().setTopology(testTopology).build();
    SchedulerStateManagerAdaptor adaptor = mock(SchedulerStateManagerAdaptor.class);
    when(adaptor.getPhysicalPlan(topology)).thenReturn(plan);
    return adaptor;
  }

  @Test
  public void providesBoltNames() {
    SchedulerStateManagerAdaptor adaptor = getMockSchedulerStateManagerAdaptor();
    TopologyProvider topologyProvider = new TopologyProvider(adaptor, eventManager, topology);

    Map<String, Integer> bolts = getBolts();
    assertEquals(2, bolts.size());
    String[] boltNames = topologyProvider.getBoltNames();
    assertEquals(bolts.size(), boltNames.length);
    for (String boltName : boltNames) {
      bolts.remove(boltName);
    }
    assertEquals(0, bolts.size());
  }

  private Map<String, Integer> getBolts() {
    Map<String, Integer> bolts = new HashMap<>();
    bolts.put("bolt-1", 1);
    bolts.put("bolt-2", 1);
    return bolts;
  }

  private Map<String, Integer> getSpouts() {
    Map<String, Integer> spouts = new HashMap<>();
    spouts.put("spout", 1);
    return spouts;
  }
}
