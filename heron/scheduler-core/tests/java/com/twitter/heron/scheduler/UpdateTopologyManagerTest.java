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
import java.util.Map;

import com.google.common.base.Optional;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.packing.roundrobin.RoundRobinPacking;
import com.twitter.heron.proto.system.PackingPlans;
import com.twitter.heron.scheduler.UpdateTopologyManager.ContainerDelta;
import com.twitter.heron.spi.common.ClusterDefaults;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Keys;
import com.twitter.heron.spi.packing.PackingPlan;
import com.twitter.heron.spi.packing.PackingPlanProtoSerializer;
import com.twitter.heron.spi.statemgr.SchedulerStateManagerAdaptor;
import com.twitter.heron.spi.utils.TopologyTests;

public class UpdateTopologyManagerTest {
  @Test
  public void computesContainerDeltaAccurately() {
    PackingPlan.ContainerPlan mockContainerPlan = Mockito.mock(PackingPlan.ContainerPlan.class);

    Map<String, PackingPlan.ContainerPlan> currentPlan = new HashMap<>();
    currentPlan.put("current-1", mockContainerPlan);
    currentPlan.put("current-2", mockContainerPlan);
    currentPlan.put("current-3", mockContainerPlan);
    currentPlan.put("current-4", mockContainerPlan);

    Map<String, PackingPlan.ContainerPlan> updatedPlan = new HashMap<>();
    updatedPlan.put("current-1", mockContainerPlan);
    updatedPlan.put("current-3", mockContainerPlan);
    updatedPlan.put("new-1", mockContainerPlan);
    updatedPlan.put("new-2", mockContainerPlan);

    UpdateTopologyManager manager = new UpdateTopologyManager(null, null);
    ContainerDelta result = manager.getContainerDelta(currentPlan, updatedPlan);
    Assert.assertNotNull(result);
    Assert.assertEquals(4, result.size());
    Assert.assertEquals(2, result.getContainersToAdd().size());
    Assert.assertTrue(result.getContainersToAdd().containsKey("new-1"));
    Assert.assertTrue(result.getContainersToAdd().containsKey("new-2"));
    Assert.assertEquals(2, result.getContainersToRemove().size());
    Assert.assertTrue(result.getContainersToRemove().containsKey("current-2"));
    Assert.assertTrue(result.getContainersToRemove().containsKey("current-4"));
  }

  @Test
  public void requestsToAddAndRemoveContainers() throws Exception {
    SchedulerStateManagerAdaptor mockStateMgr = Mockito.mock(SchedulerStateManagerAdaptor.class);
    Config mockRuntime = Mockito.mock(Config.class);
    Mockito.when(mockRuntime.get(Keys.schedulerStateManagerAdaptor())).thenReturn(mockStateMgr);

    ScalableScheduler mockScheduler = Mockito.mock(ScalableScheduler.class);
    UpdateTopologyManager updateManager;
    updateManager = new UpdateTopologyManager(mockRuntime, Optional.of(mockScheduler));
    UpdateTopologyManager spyUpdateManager = Mockito.spy(updateManager);

    PackingPlans.PackingPlan currentPlan = createTestTopology(2, 2, 0);
    PackingPlans.PackingPlan proposedPlan = createTestTopology(2, 3, 1);

    Mockito.doNothing().when(spyUpdateManager).
        validateCurrentPackingPlan(currentPlan, null, mockStateMgr);
    Mockito.doReturn(null).when(spyUpdateManager).
        getUpdatedTopology(Mockito.anyString(),
            Mockito.any(PackingPlan.class),
            Mockito.eq(mockStateMgr));

    Map<String, PackingPlan.ContainerPlan> containersToAdd = new HashMap<>();
    containersToAdd.put("a1", null);
    Map<String, PackingPlan.ContainerPlan> containersToRemove = new HashMap<>();
    containersToRemove.put("r1", null);

    ContainerDelta mockContainerDelta = Mockito.mock(ContainerDelta.class);
    Mockito.when(mockContainerDelta.getContainersToAdd()).thenReturn(containersToAdd);
    Mockito.when(mockContainerDelta.getContainersToRemove()).thenReturn(containersToRemove);

    Mockito.doReturn(mockContainerDelta).when(spyUpdateManager).getContainerDelta(
        Mockito.anyMapOf(String.class, PackingPlan.ContainerPlan.class),
        Mockito.anyMapOf(String.class, PackingPlan.ContainerPlan.class));

    spyUpdateManager.updateTopology(currentPlan, proposedPlan);

    Mockito.verify(mockScheduler).addContainers(containersToAdd);
    Mockito.verify(mockScheduler).removeContainers(
        Matchers.anyMapOf(String.class, PackingPlan.ContainerPlan.class),
        Mockito.eq(containersToRemove));
  }

  /* offset will be added to container id to simulate deletion of existing containers
   */
  PackingPlans.PackingPlan createTestTopology(int parallelism, int numContainers, int offset) {
    Map<String, Integer> spouts = new HashMap<>();
    spouts.put("testSpout", parallelism);

    Map<String, Integer> bolts = new HashMap<>();
    bolts.put("testBolt", 1);

    com.twitter.heron.api.Config topologyConfig = new com.twitter.heron.api.Config();
    topologyConfig.put(com.twitter.heron.api.Config.TOPOLOGY_STMGRS, numContainers);

    TopologyAPI.Topology topology =
        TopologyTests.createTopology("testTopology", topologyConfig, spouts, bolts);

    Config config = Config.newBuilder()
        .put(Keys.topologyId(), topology.getId())
        .put(Keys.topologyName(), topology.getName())
        .putAll(ClusterDefaults.getDefaults())
        .build();

    RoundRobinPacking packing = new RoundRobinPacking();
    packing.initialize(config, topology);
    PackingPlan plan = packing.pack();
    if (offset > 0) {
      Map<String, PackingPlan.ContainerPlan> containers = plan.containers;
      for (String containerId : plan.containers.keySet()) {
        PackingPlan.ContainerPlan container = plan.containers.get(containerId);
        String newId = String.valueOf(Integer.valueOf(containerId) + offset);
        PackingPlan.ContainerPlan newContainer = new PackingPlan.ContainerPlan(
            newId, container.instances, container.resource);
        containers.put(newId, newContainer);
      }
      plan = new PackingPlan(plan.id, containers, plan.resource);
    }

    PackingPlanProtoSerializer serializer = new PackingPlanProtoSerializer();
    return serializer.toProto(plan);
  }
}
