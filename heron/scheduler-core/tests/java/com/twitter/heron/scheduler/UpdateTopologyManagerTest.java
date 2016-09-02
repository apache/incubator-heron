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

import java.util.HashSet;
import java.util.Set;

import com.google.common.base.Optional;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import com.twitter.heron.packing.roundrobin.RoundRobinPacking;
import com.twitter.heron.proto.system.PackingPlans;
import com.twitter.heron.scheduler.UpdateTopologyManager.ContainerDelta;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Keys;
import com.twitter.heron.spi.packing.IPacking;
import com.twitter.heron.spi.packing.PackingPlan;
import com.twitter.heron.spi.packing.PackingPlanProtoDeserializer;
import com.twitter.heron.spi.scheduler.IScalable;
import com.twitter.heron.spi.statemgr.SchedulerStateManagerAdaptor;
import com.twitter.heron.spi.utils.PackingTestUtils;

public class UpdateTopologyManagerTest {
  @Test
  public void computesContainerDeltaAccurately() {
    Set<PackingPlan.ContainerPlan> currentPlan = new HashSet<>();
    for (int i = 1; i < 5; i++) {
      currentPlan.add(PackingTestUtils.testContainerPlan("current-" + i));
    }

    Set<PackingPlan.ContainerPlan> updatedPlan = new HashSet<>();
    updatedPlan.add(PackingTestUtils.testContainerPlan("current-1"));
    updatedPlan.add(PackingTestUtils.testContainerPlan("current-3"));
    updatedPlan.add(PackingTestUtils.testContainerPlan("new-1"));
    updatedPlan.add(PackingTestUtils.testContainerPlan("new-2"));

    UpdateTopologyManager manager = new UpdateTopologyManager(null, null);
    ContainerDelta result = manager.getContainerDelta(currentPlan, updatedPlan);
    Assert.assertNotNull(result);
    Assert.assertEquals(2, result.getContainersToAdd().size());
    Assert.assertTrue(result.getContainersToAdd().contains(
        PackingTestUtils.testContainerPlan("new-1")));
    Assert.assertTrue(result.getContainersToAdd().contains(
        PackingTestUtils.testContainerPlan("new-2")));
    Assert.assertEquals(2, result.getContainersToRemove().size());
    Assert.assertTrue(result.getContainersToRemove().contains(
        PackingTestUtils.testContainerPlan("current-2")));
    Assert.assertTrue(result.getContainersToRemove().contains(
        PackingTestUtils.testContainerPlan("current-4")));
  }

  /**
   * Test scalable scheduler invocation
   */
  @Test
  public void requestsToAddAndRemoveContainers() throws Exception {
    PackingPlanProtoDeserializer deserializer = Mockito.mock(PackingPlanProtoDeserializer.class);
    String topologyName = "testTopology";
    IPacking packing = new RoundRobinPacking();

    Set<PackingPlan.ContainerPlan> currentContainers = new HashSet<>();
    PackingPlans.PackingPlan currentPlan =
        PackingTestUtils.testProtoPackingPlan(topologyName, packing);
    PackingPlan currentPacking = new PackingPlan("current", currentContainers, null);
    Mockito.when(deserializer.fromProto(currentPlan)).thenReturn(currentPacking);

    PackingPlan proposedPlan = PackingTestUtils.testPackingPlan(topologyName, packing);
    Set<PackingPlan.ContainerPlan> proposedContainers = proposedPlan.getContainers();
    PackingPlans.PackingPlan proposedProtoPlan =
        PackingTestUtils.testProtoPackingPlan(topologyName, packing);
    PackingPlan proposedPacking = new PackingPlan("proposed", proposedContainers, null);
    Mockito.when(deserializer.fromProto(proposedProtoPlan)).thenReturn(proposedPacking);

    SchedulerStateManagerAdaptor mockStateMgr = Mockito.mock(SchedulerStateManagerAdaptor.class);
    Config mockRuntime = Mockito.mock(Config.class);
    Mockito.when(mockRuntime.get(Keys.schedulerStateManagerAdaptor())).thenReturn(mockStateMgr);

    IScalable mockScheduler = Mockito.mock(IScalable.class);

    UpdateTopologyManager updateManager
        = new UpdateTopologyManager(mockRuntime, Optional.of(mockScheduler), deserializer);
    UpdateTopologyManager spyUpdateManager = Mockito.spy(updateManager);

    Mockito.doNothing().when(spyUpdateManager)
        .validateCurrentPackingPlan(currentPlan, null, mockStateMgr);
    Mockito.doReturn(null).when(spyUpdateManager).
        getUpdatedTopology(null, proposedPacking, mockStateMgr);

    Set<PackingPlan.ContainerPlan> containersToAdd = new HashSet<>();
    containersToAdd.add(PackingTestUtils.testContainerPlan("a1"));
    Set<PackingPlan.ContainerPlan> containersToRemove = new HashSet<>();
    containersToRemove.add(PackingTestUtils.testContainerPlan("r1"));

    ContainerDelta mockContainerDelta = Mockito.mock(ContainerDelta.class);
    Mockito.when(mockContainerDelta.getContainersToAdd()).thenReturn(containersToAdd);
    Mockito.when(mockContainerDelta.getContainersToRemove()).thenReturn(containersToRemove);

    Mockito.doReturn(mockContainerDelta).when(spyUpdateManager).getContainerDelta(
        currentPacking.getContainers(), proposedPacking.getContainers());

    spyUpdateManager.updateTopology(currentPlan, proposedProtoPlan);

    Mockito.verify(mockScheduler).addContainers(containersToAdd);
    Mockito.verify(mockScheduler).removeContainers(containersToRemove);
  }

}
