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
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.twitter.heron.proto.system.PackingPlans;
import com.twitter.heron.scheduler.UpdateTopologyManager.ContainerDelta;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Keys;
import com.twitter.heron.spi.packing.PackingPlan;
import com.twitter.heron.spi.packing.PackingPlanProtoSerializer;
import com.twitter.heron.spi.packing.Resource;
import com.twitter.heron.spi.scheduler.IScalable;
import com.twitter.heron.spi.statemgr.SchedulerStateManagerAdaptor;
import com.twitter.heron.spi.utils.PackingTestUtils;

public class UpdateTopologyManagerTest {

  private Set<PackingPlan.ContainerPlan> currentContainerPlan;
  private Set<PackingPlan.ContainerPlan> proposedContainerPlan;
  private Set<PackingPlan.ContainerPlan> expectedContainersToAdd;
  private Set<PackingPlan.ContainerPlan> expectedContainersToRemove;

  @Before
  public void init() {
    currentContainerPlan = buildContainerSet("current-1", "current-2", "current-3", "current-4");
    proposedContainerPlan = buildContainerSet("current-1", "current-3", "new-1", "new-2");
    expectedContainersToAdd = buildContainerSet("new-1", "new-2");
    expectedContainersToRemove = buildContainerSet("current-2", "current-4");
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
        = new PackingPlan("current", currentContainerPlan, new Resource(0.5, 1, 2));
    PackingPlan proposedPacking
        = new PackingPlan("proposed", proposedContainerPlan, new Resource(0.5, 1, 2));

    PackingPlans.PackingPlan currentProtoPlan = serializer.toProto(currentPacking);
    PackingPlans.PackingPlan proposedProtoPlan = serializer.toProto(proposedPacking);

    SchedulerStateManagerAdaptor mockStateMgr = Mockito.mock(SchedulerStateManagerAdaptor.class);
    Config mockRuntime = Mockito.mock(Config.class);
    Mockito.when(mockRuntime.get(Keys.schedulerStateManagerAdaptor())).thenReturn(mockStateMgr);

    IScalable mockScheduler = Mockito.mock(IScalable.class);

    UpdateTopologyManager updateManager
        = new UpdateTopologyManager(mockRuntime, Optional.of(mockScheduler));
    UpdateTopologyManager spyUpdateManager = Mockito.spy(updateManager);

    Mockito.doNothing().when(spyUpdateManager)
        .validateCurrentPackingPlan(currentProtoPlan, null, mockStateMgr);
    Mockito.doReturn(null).when(spyUpdateManager).
        getUpdatedTopology(null, proposedPacking, mockStateMgr);

    spyUpdateManager.updateTopology(currentProtoPlan, proposedProtoPlan);

    Mockito.verify(mockScheduler).addContainers(expectedContainersToAdd);
    Mockito.verify(mockScheduler).removeContainers(expectedContainersToRemove);
  }

  private static Set<PackingPlan.ContainerPlan> buildContainerSet(String... containerIds) {
    Set<PackingPlan.ContainerPlan> containerPlan = new HashSet<>();
    for (String containerId : containerIds) {
      containerPlan.add(PackingTestUtils.testContainerPlan(containerId));
    }
    return containerPlan;
  }
}
