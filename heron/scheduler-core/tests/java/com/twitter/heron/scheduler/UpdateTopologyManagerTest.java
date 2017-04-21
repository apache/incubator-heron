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

import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Optional;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.proto.system.PackingPlans;
import com.twitter.heron.proto.system.PhysicalPlans;
import com.twitter.heron.scheduler.UpdateTopologyManager.ContainerDelta;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Key;
import com.twitter.heron.spi.packing.PackingPlan;
import com.twitter.heron.spi.packing.PackingPlanProtoSerializer;
import com.twitter.heron.spi.scheduler.IScalable;
import com.twitter.heron.spi.statemgr.IStateManager;
import com.twitter.heron.spi.statemgr.Lock;
import com.twitter.heron.spi.statemgr.SchedulerStateManagerAdaptor;
import com.twitter.heron.spi.utils.NetworkUtils;
import com.twitter.heron.spi.utils.PackingTestUtils;
import com.twitter.heron.spi.utils.TMasterUtils;
import com.twitter.heron.spi.utils.TopologyTests;

@RunWith(PowerMockRunner.class)
public class UpdateTopologyManagerTest {

  private static final String TOPOLOGY_NAME = "topologyName";

  private Set<PackingPlan.ContainerPlan> currentContainerPlan;
  private Set<PackingPlan.ContainerPlan> proposedContainerPlan;
  private Set<PackingPlan.ContainerPlan> expectedContainersToAdd;
  private Set<PackingPlan.ContainerPlan> expectedContainersToRemove;

  private PackingPlan proposedPacking;
  private PackingPlans.PackingPlan currentProtoPlan;
  private PackingPlans.PackingPlan proposedProtoPlan;
  private TopologyAPI.Topology testTopology;

  @Before
  public void init() {
    Integer[] instanceIndexA = new Integer[] {37, 48, 59};
    Integer[] instanceIndexB = new Integer[] {17, 22};
    currentContainerPlan = buildContainerSet(new Integer[] {1, 2, 3, 4}, instanceIndexA);
    proposedContainerPlan = buildContainerSet(new Integer[] {1, 3, 5, 6}, instanceIndexB);
    expectedContainersToAdd = buildContainerSet(new Integer[] {5, 6}, instanceIndexB);
    expectedContainersToRemove = buildContainerSet(new Integer[] {2, 4}, instanceIndexA);

    PackingPlanProtoSerializer serializer = new PackingPlanProtoSerializer();
    PackingPlan currentPacking = new PackingPlan("current", currentContainerPlan);
    proposedPacking = new PackingPlan("proposed", proposedContainerPlan);

    currentProtoPlan = serializer.toProto(currentPacking);
    proposedProtoPlan = serializer.toProto(proposedPacking);

    testTopology = TopologyTests.createTopology(
        TOPOLOGY_NAME, new com.twitter.heron.api.Config(), "spoutname", "boltname", 1, 1);
    assertEquals(TopologyAPI.TopologyState.RUNNING, testTopology.getState());
  }

  private static Lock mockLock(boolean available) throws InterruptedException {
    Lock lock = mock(Lock.class);
    when(lock.tryLock(any(Long.class), any(TimeUnit.class))).thenReturn(available);
    return lock;
  }

  private static SchedulerStateManagerAdaptor mockStateManager(TopologyAPI.Topology topology,
                                                               PackingPlans.PackingPlan packingPlan,
                                                               Lock lock) {
    SchedulerStateManagerAdaptor stateManager = mock(SchedulerStateManagerAdaptor.class);
    when(stateManager.getPhysicalPlan(TOPOLOGY_NAME))
        .thenReturn(PhysicalPlans.PhysicalPlan.getDefaultInstance());
    when(stateManager.getTopology(TOPOLOGY_NAME)).thenReturn(topology);
    when(stateManager.getPackingPlan(eq(TOPOLOGY_NAME))).thenReturn(packingPlan);
    when(stateManager.getLock(eq(TOPOLOGY_NAME), eq(IStateManager.LockName.UPDATE_TOPOLOGY)))
        .thenReturn(lock);
    return stateManager;
  }

  private static Config mockRuntime(SchedulerStateManagerAdaptor stateManager) {
    Config runtime = mock(Config.class);
    when(runtime.getStringValue(Key.TOPOLOGY_NAME)).thenReturn(TOPOLOGY_NAME);
    when(runtime.get(Key.SCHEDULER_STATE_MANAGER_ADAPTOR)).thenReturn(stateManager);
    return runtime;
  }

  private UpdateTopologyManager spyUpdateManager(SchedulerStateManagerAdaptor stateManager,
                                                 IScalable scheduler,
                                                 TopologyAPI.Topology updatedTopology) {
    Config mockRuntime = mockRuntime(stateManager);
    UpdateTopologyManager spyUpdateManager = spy(new UpdateTopologyManager(
        mock(Config.class), mockRuntime, Optional.of(scheduler))
    );

    when(spyUpdateManager.getUpdatedTopology(TOPOLOGY_NAME, this.proposedPacking, stateManager))
        .thenReturn(updatedTopology);
    return spyUpdateManager;
  }

  @Test
  public void testContainerDelta() {
    ContainerDelta result =  new ContainerDelta(currentContainerPlan, proposedContainerPlan);
    assertNotNull(result);
    assertEquals(expectedContainersToAdd, result.getContainersToAdd());
    assertEquals(expectedContainersToRemove, result.getContainersToRemove());
  }

  /**
   * Test scalable scheduler invocation
   */
  @Test
  @PrepareForTest(TMasterUtils.class)
  public void requestsToAddAndRemoveContainers() throws Exception {
    Lock lock = mockLock(true);
    SchedulerStateManagerAdaptor mockStateMgr = mockStateManager(
        testTopology, this.currentProtoPlan, lock);
    IScalable mockScheduler = mock(IScalable.class);
    UpdateTopologyManager spyUpdateManager =
        spyUpdateManager(mockStateMgr, mockScheduler, testTopology);

    PowerMockito.spy(TMasterUtils.class);
    PowerMockito.doNothing().when(TMasterUtils.class, "sendToTMaster",
        any(String.class), eq(TOPOLOGY_NAME),
        eq(mockStateMgr), any(NetworkUtils.TunnelConfig.class));

    // reactivation won't happen since topology state is still running due to mock state manager
    PowerMockito.doNothing().when(TMasterUtils.class, "transitionTopologyState",
        eq(TOPOLOGY_NAME), eq(TMasterUtils.TMasterCommand.ACTIVATE), eq(mockStateMgr),
        eq(TopologyAPI.TopologyState.PAUSED), eq(TopologyAPI.TopologyState.RUNNING),
        any(NetworkUtils.TunnelConfig.class));

    spyUpdateManager.updateTopology(currentProtoPlan, proposedProtoPlan);

    verify(spyUpdateManager).deactivateTopology(eq(mockStateMgr), eq(testTopology));
    verify(spyUpdateManager).reactivateTopology(eq(mockStateMgr), eq(testTopology), eq(2));
    verify(mockScheduler).addContainers(expectedContainersToAdd);
    verify(mockScheduler).removeContainers(expectedContainersToRemove);
    verify(lock).tryLock(any(Long.class), any(TimeUnit.class));
    verify(lock).unlock();

    PowerMockito.verifyStatic(times(1));
    TMasterUtils.transitionTopologyState(eq(TOPOLOGY_NAME),
        eq(TMasterUtils.TMasterCommand.DEACTIVATE), eq(mockStateMgr),
        eq(TopologyAPI.TopologyState.RUNNING), eq(TopologyAPI.TopologyState.PAUSED),
        any(NetworkUtils.TunnelConfig.class));

    PowerMockito.verifyStatic(times(1));
    TMasterUtils.transitionTopologyState(eq(TOPOLOGY_NAME),
        eq(TMasterUtils.TMasterCommand.ACTIVATE), eq(mockStateMgr),
        eq(TopologyAPI.TopologyState.PAUSED), eq(TopologyAPI.TopologyState.RUNNING),
        any(NetworkUtils.TunnelConfig.class));
  }

  @Test(expected = ConcurrentModificationException.class)
  public void testLockTaken() throws Exception {
    SchedulerStateManagerAdaptor mockStateMgr = mockStateManager(
        testTopology, this.currentProtoPlan, mockLock(false));
    UpdateTopologyManager spyUpdateManager =
        spyUpdateManager(mockStateMgr, mock(IScalable.class), testTopology);

    spyUpdateManager.updateTopology(currentProtoPlan, proposedProtoPlan);
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
        TOPOLOGY_NAME, new com.twitter.heron.api.Config(), spouts, bolts);

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
                                 Map<String, Integer> expectedSpouts,
                                 Map<String, Integer> expectedBolts) {
    for (String boltName : expectedBolts.keySet()) {
      String foundParallelism = null;
      for (TopologyAPI.Bolt bolt : topology.getBoltsList()) {
        foundParallelism = getParallelism(bolt.getComp(), boltName);
        if (foundParallelism != null) {
          break;
        }
      }
      assertEquals(Integer.toString(expectedBolts.get(boltName)), foundParallelism);
    }

    for (String spoutName : expectedSpouts.keySet()) {
      String foundParallelism = null;
      for (TopologyAPI.Spout spout : topology.getSpoutsList()) {
        foundParallelism = getParallelism(spout.getComp(), spoutName);
        if (foundParallelism != null) {
          break;
        }
      }
      assertEquals(Integer.toString(expectedSpouts.get(spoutName)), foundParallelism);
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
