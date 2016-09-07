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
package com.twitter.heron.scheduler;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Optional;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.proto.system.PackingPlans;
import com.twitter.heron.scheduler.UpdateTopologyManager.ContainerDelta;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Keys;
import com.twitter.heron.spi.packing.PackingPlan;
import com.twitter.heron.spi.packing.PackingPlanProtoSerializer;
import com.twitter.heron.spi.scheduler.IScalable;
import com.twitter.heron.spi.statemgr.SchedulerStateManagerAdaptor;
import com.twitter.heron.spi.utils.PackingTestUtils;
import com.twitter.heron.spi.utils.TopologyTests;

public class UpdateTopologyManagerTest {

  private Set<PackingPlan.ContainerPlan> currentContainerPlan;
  private Set<PackingPlan.ContainerPlan> proposedContainerPlan;
  private Set<PackingPlan.ContainerPlan> expectedContainersToAdd;
  private Set<PackingPlan.ContainerPlan> expectedContainersToRemove;

  @Before
  public void init() {
    Integer[] instanceIndexA = new Integer[] {37, 48, 59};
    Integer[] instanceIndexB = new Integer[] {17, 22};
    currentContainerPlan = buildContainerSet(new Integer[] {1, 2, 3, 4}, instanceIndexA);
    proposedContainerPlan = buildContainerSet(new Integer[] {1, 3, 5, 6}, instanceIndexB);
    expectedContainersToAdd = buildContainerSet(new Integer[] {5, 6}, instanceIndexB);
    expectedContainersToRemove = buildContainerSet(new Integer[] {2, 4}, instanceIndexA);
  }

  @Test
  public void testContainerDelta() {
    ContainerDelta result =  new ContainerDelta(currentContainerPlan, proposedContainerPlan);
    Assert.assertNotNull(result);
    Assert.assertEquals(expectedContainersToAdd, result.getContainersToAdd());
    Assert.assertEquals(expectedContainersToRemove, result.getContainersToRemove());
  }

  /**
   * Test scalable scheduler invocation
   */
  @Test
  public void requestsToAddAndRemoveContainers() throws Exception {
    PackingPlanProtoSerializer serializer = new PackingPlanProtoSerializer();

    PackingPlan currentPacking
        = new PackingPlan("current", currentContainerPlan);
    PackingPlan proposedPacking
        = new PackingPlan("proposed", proposedContainerPlan);

    PackingPlans.PackingPlan currentProtoPlan = serializer.toProto(currentPacking);
    PackingPlans.PackingPlan proposedProtoPlan = serializer.toProto(proposedPacking);

    SchedulerStateManagerAdaptor mockStateMgr = Mockito.mock(SchedulerStateManagerAdaptor.class);
    Config mockRuntime = Mockito.mock(Config.class);
    Mockito.when(mockRuntime.get(Keys.schedulerStateManagerAdaptor())).thenReturn(mockStateMgr);

    IScalable mockScheduler = Mockito.mock(IScalable.class);

    UpdateTopologyManager updateManager
        = new UpdateTopologyManager(mockRuntime, Optional.of(mockScheduler));
    UpdateTopologyManager spyUpdateManager = Mockito.spy(updateManager);

    Mockito.doReturn(null).when(spyUpdateManager).
        getUpdatedTopology(null, proposedPacking, mockStateMgr);

    spyUpdateManager.updateTopology(currentProtoPlan, proposedProtoPlan);

    Mockito.verify(mockScheduler).addContainers(expectedContainersToAdd);
    Mockito.verify(mockScheduler).removeContainers(expectedContainersToRemove);
  }

  @Test
  public void testUpdateTopology() {
    Map<String, Integer> bolts = new HashMap<>();
    bolts.put("bolt1", 1);
    bolts.put("bolt7", 7);

    Map<String, Integer> spouts = new HashMap<>();
    spouts.put("spout3", 3);
    spouts.put("spout5", 5);

    TopologyAPI.Topology topology = TopologyTests.createTopology(
        "test", new com.twitter.heron.api.Config(), spouts, bolts);

    // assert that the initial config settings are as expected
    assertParallelism(topology, spouts, bolts);

    Map<String, Integer> boltUpdates = new HashMap<>();
    boltUpdates.put("bolt1", 3);
    boltUpdates.put("bolt7", 2);

    Map<String, Integer> spoutUpdates = new HashMap<>();
    spoutUpdates.put("spout3", 8);

    Map<String, Integer> updates = new HashMap<>();
    updates.putAll(boltUpdates);
    updates.putAll(spoutUpdates);

    // assert that the updated topology config settings are as expected
    topology = UpdateTopologyManager.mergeTopology(topology, updates);
    bolts.putAll(boltUpdates);
    spouts.putAll(spoutUpdates);

    assertParallelism(topology, spouts, bolts);
  }

  private void assertParallelism(TopologyAPI.Topology topology,
                                 Map<String, Integer> expectedSouts,
                                 Map<String, Integer> expectedBolts) {
    for (String boltName : expectedBolts.keySet()) {
      String foundParallelism = null;
      for (TopologyAPI.Bolt bolt : topology.getBoltsList()) {
        foundParallelism = getParallelism(bolt.getComp(), boltName);
        if (foundParallelism != null) {
          break;
        }
      }
      Assert.assertEquals(Integer.toString(expectedBolts.get(boltName)), foundParallelism);
    }

    for (String spoutName : expectedSouts.keySet()) {
      String foundParallelism = null;
      for (TopologyAPI.Spout spout : topology.getSpoutsList()) {
        foundParallelism = getParallelism(spout.getComp(), spoutName);
        if (foundParallelism != null) {
          break;
        }
      }
      Assert.assertEquals(Integer.toString(expectedSouts.get(spoutName)), foundParallelism);
    }
  }

  private static String getParallelism(TopologyAPI.Component component, String componentName) {
    if (component.getName().equals(componentName)) {
      for (TopologyAPI.Config.KeyValue keyValue : component.getConfig().getKvsList()) {
        if (keyValue.getKey().equals(com.twitter.heron.api.Config.TOPOLOGY_COMPONENT_PARALLELISM)) {
          return keyValue.getValue();
        }
      }
    }
    return null;
  }

  private static Set<PackingPlan.ContainerPlan> buildContainerSet(Integer[] containerIds,
                                                                  Integer[] instanceIndexes) {
    Set<PackingPlan.ContainerPlan> containerPlan = new HashSet<>();
    for (int containerId : containerIds) {
      containerPlan.add(PackingTestUtils.testContainerPlan(containerId, instanceIndexes));
    }
    return containerPlan;
  }
}
