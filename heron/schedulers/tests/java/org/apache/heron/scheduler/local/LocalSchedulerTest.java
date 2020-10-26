/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.heron.scheduler.local;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import org.apache.heron.api.generated.TopologyAPI;
import org.apache.heron.proto.scheduler.Scheduler;
import org.apache.heron.spi.common.Config;
import org.apache.heron.spi.common.Key;
import org.apache.heron.spi.packing.PackingPlan;
import org.apache.heron.spi.utils.PackingTestUtils;

@SuppressWarnings("unchecked")
public class LocalSchedulerTest {
  private static final String TOPOLOGY_NAME = "testTopology";
  private static final int MAX_WAITING_SECOND = 10;
  private static LocalScheduler scheduler;
  private Config config;
  private Config runtime;

  @Before
  public void before() throws Exception {
    scheduler = Mockito.spy(LocalScheduler.class);
    config = Mockito.mock(Config.class);
    Mockito.when(config.getStringValue(Key.TOPOLOGY_NAME)).thenReturn(TOPOLOGY_NAME);

    runtime = Mockito.mock(Config.class);

    scheduler.initialize(config, runtime);
    Mockito.when(runtime.get(Key.TOPOLOGY_DEFINITION)).thenReturn(TopologyAPI.Topology
        .newBuilder()
        .setId("a")
        .setName("a")
        .setState(TopologyAPI.TopologyState.RUNNING)
        .setTopologyConfig(
            TopologyAPI.Config.newBuilder()
                .addKvs(TopologyAPI.Config.KeyValue.newBuilder()
                    .setKey(org.apache.heron.api.Config.TOPOLOGY_REMOTE_DEBUGGING_ENABLE)
                    .setValue("false"))).build());

    scheduler.initialize(config, runtime);
  }

  @After
  public void after() throws Exception {
    scheduler.close();
  }

  @Test
  public void testClose() throws Exception {
    LocalScheduler localScheduler = new LocalScheduler();
    localScheduler.initialize(Mockito.mock(Config.class), Mockito.mock(Config.class));
    // The MonitorService should started
    ExecutorService monitorService = localScheduler.getMonitorService();
    Assert.assertFalse(monitorService.isShutdown());
    Assert.assertEquals(0, localScheduler.getProcessToContainer().size());

    // The MonitorService should shutdown
    localScheduler.close();
    Assert.assertTrue(monitorService.isShutdown());
  }

  @Test
  public void testOnSchedule() throws Exception {
    Mockito.doNothing().
        when(scheduler).startExecutorMonitor(Mockito.anyInt(), Mockito.any(Process.class),
        Mockito.anySet());
    Process[] mockProcesses = new Process[4];
    for (int i = 0; i < 4; i++) {
      mockProcesses[i] = Mockito.mock(Process.class);
      Set<PackingPlan.InstancePlan> instances
          = (i == 0) ?  null : PackingTestUtils.testContainerPlan(i).getInstances();
      Mockito.doReturn(mockProcesses[i]).when(scheduler).startExecutorProcess(i, instances);
    }

    PackingPlan packingPlan = Mockito.mock(PackingPlan.class);
    Set<PackingPlan.ContainerPlan> containers = new HashSet<>();
    containers.add(PackingTestUtils.testContainerPlan(1));
    containers.add(PackingTestUtils.testContainerPlan(3));
    Mockito.when(packingPlan.getContainers()).thenReturn(containers);

    Assert.assertTrue(scheduler.onSchedule(packingPlan));
    verifyIdsOfLaunchedContainers(0, 1, 3);

    for (int i = 0; i < 4; i++) {
      if (i == 2) {
        // id 2 was not in the container plan
        continue;
      }

      Set<PackingPlan.InstancePlan> instances
          = (i == 0) ?  null : PackingTestUtils.testContainerPlan(i).getInstances();
      Mockito.verify(scheduler).startExecutor(i, instances);
      Mockito.verify(scheduler).startExecutorProcess(i, instances);
      Mockito.verify(scheduler).startExecutorMonitor(i, mockProcesses[i], instances);
    }
  }

  @Test
  public void testAddContainer() throws Exception {
    Mockito.when(runtime.getLongValue(Key.NUM_CONTAINERS)).thenReturn(2L);
    scheduler.initialize(config, runtime);

    //verify plan is deployed and containers are created
    Mockito.doNothing().
        when(scheduler).startExecutorMonitor(Mockito.anyInt(), Mockito.any(Process.class),
        Mockito.anySet());
    Process mockProcessTM = Mockito.mock(Process.class);
    Mockito.doReturn(mockProcessTM).when(scheduler).startExecutorProcess(
        0, null);

    Process mockProcessWorker1 = Mockito.mock(Process.class);
    Mockito.doReturn(mockProcessWorker1).when(scheduler).startExecutorProcess(
        1,  PackingTestUtils.testContainerPlan(1).getInstances());

    PackingPlan packingPlan = Mockito.mock(PackingPlan.class);
    Set<PackingPlan.ContainerPlan> containers = new HashSet<>();
    containers.add(PackingTestUtils.testContainerPlan(1));
    Mockito.when(packingPlan.getContainers()).thenReturn(containers);
    Assert.assertTrue(scheduler.onSchedule(packingPlan));

    Mockito.verify(scheduler, Mockito.times(2)).startExecutor(Mockito.anyInt(),
        Mockito.anySet());

    //now verify add container adds new container
    Process mockProcessWorker2 = Mockito.mock(Process.class);
    Mockito.doReturn(mockProcessWorker2).when(scheduler).startExecutorProcess(
        3, PackingTestUtils.testContainerPlan(3).getInstances());
    containers.clear();
    containers.add(PackingTestUtils.testContainerPlan(3));
    scheduler.addContainers(containers);
    Mockito.verify(scheduler).startExecutor(3,
        PackingTestUtils.testContainerPlan(3).getInstances());

    Process mockProcess = Mockito.mock(Process.class);
    Mockito.doReturn(mockProcess).when(scheduler).startExecutorProcess(
        Mockito.anyInt(), Mockito.anySet());
    containers.clear();
    containers.add(PackingTestUtils.testContainerPlan(4));
    containers.add(PackingTestUtils.testContainerPlan(5));
    scheduler.addContainers(containers);
    Mockito.verify(scheduler).startExecutor(4,
        PackingTestUtils.testContainerPlan(4).getInstances());
    Mockito.verify(scheduler).startExecutor(5,
        PackingTestUtils.testContainerPlan(5).getInstances());
  }

  /**
   * Verify containers can be removed by Local Scheduler
   */
  @Test
  public void testRemoveContainer() throws Exception {
    final int LOCAL_NUM_CONTAINER = 6;

    //verify plan is deployed and containers are created
    Mockito.doNothing().
        when(scheduler).startExecutorMonitor(Mockito.anyInt(),
        Mockito.any(Process.class), Mockito.anySet());

    Process[] processes = new Process[LOCAL_NUM_CONTAINER];
    Set<PackingPlan.ContainerPlan> existingContainers = new HashSet<>();
    for (int i = 0; i < LOCAL_NUM_CONTAINER; i++) {
      processes[i] = Mockito.mock(Process.class);
      Set<PackingPlan.InstancePlan> instances
          = (i == 0) ?  null : PackingTestUtils.testContainerPlan(i).getInstances();
      Mockito.doReturn(processes[i]).when(scheduler)
          .startExecutorProcess(i,  instances);
      if (i > 0) {
        // ignore the container for TManager. existing containers simulate the containers created
        // by packing plan
        existingContainers.add(PackingTestUtils.testContainerPlan(i));
      }
    }

    PackingPlan packingPlan = Mockito.mock(PackingPlan.class);
    Mockito.when(packingPlan.getContainers()).thenReturn(existingContainers);
    Assert.assertTrue(scheduler.onSchedule(packingPlan));
    verifyIdsOfLaunchedContainers(0, 1, 2, 3, 4, 5);
    Mockito.verify(scheduler, Mockito.times(LOCAL_NUM_CONTAINER)).startExecutor(
        Mockito.anyInt(), Mockito.anySet());

    Set<PackingPlan.ContainerPlan> containersToRemove = new HashSet<>();
    PackingPlan.ContainerPlan containerToRemove =
        PackingTestUtils.testContainerPlan(LOCAL_NUM_CONTAINER - 1);
    containersToRemove.add(containerToRemove);
    scheduler.removeContainers(containersToRemove);
    verifyIdsOfLaunchedContainers(0, 1, 2, 3, 4);
    Mockito.verify(processes[LOCAL_NUM_CONTAINER - 1]).destroy();
    // verify no new process restarts
    Mockito.verify(scheduler, Mockito.times(LOCAL_NUM_CONTAINER)).startExecutor(
        Mockito.anyInt(), Mockito.anySet());

    containersToRemove.clear();
    containersToRemove.add(PackingTestUtils.testContainerPlan(1));
    containersToRemove.add(PackingTestUtils.testContainerPlan(2));
    scheduler.removeContainers(containersToRemove);
    verifyIdsOfLaunchedContainers(0, 3, 4);
    Mockito.verify(processes[1]).destroy();
    Mockito.verify(processes[2]).destroy();
    // verify no new process restarts
    Mockito.verify(scheduler, Mockito.times(LOCAL_NUM_CONTAINER)).startExecutor(
        Mockito.anyInt(), Mockito.anySet());
  }

  private void verifyIdsOfLaunchedContainers(int... ids) {
    HashSet<Integer> containerIdsLaunched =
        new HashSet<>(scheduler.getProcessToContainer().values());
    Assert.assertEquals(ids.length, containerIdsLaunched.size());
    for (int i = 0; i < ids.length; i++) {
      Assert.assertTrue(containerIdsLaunched.contains(ids[i]));
    }
  }

  @Test
  public void testOnKill() throws Exception {
    Process mockProcess = Mockito.mock(Process.class);
    scheduler.getProcessToContainer().put(mockProcess, 0);

    Assert.assertTrue(scheduler.onKill(Scheduler.KillTopologyRequest.getDefaultInstance()));
    Assert.assertEquals(0, scheduler.getProcessToContainer().size());
    Assert.assertTrue(scheduler.isTopologyKilled());
    Mockito.verify(mockProcess).destroy();
  }

  @Test
  public void testOnRestart() throws Exception {
    Process mockProcess = Mockito.mock(Process.class);

    // Restart only one container
    Scheduler.RestartTopologyRequest restartOne =
        Scheduler.RestartTopologyRequest.newBuilder().
            setTopologyName(TOPOLOGY_NAME).
            setContainerIndex(1).
            build();

    scheduler.getProcessToContainer().put(mockProcess, 0);
    // Failed to restart due to container not exist
    Assert.assertFalse(scheduler.onRestart(restartOne));

    // Happy to restart one container
    Scheduler.RestartTopologyRequest restartZero =
        Scheduler.RestartTopologyRequest.newBuilder().
            setTopologyName(TOPOLOGY_NAME).
            setContainerIndex(0).
            build();
    scheduler.getProcessToContainer().put(mockProcess, 0);
    Assert.assertTrue(scheduler.onRestart(restartZero));


    // Happy to restart all containers
    Scheduler.RestartTopologyRequest restartAll =
        Scheduler.RestartTopologyRequest.newBuilder().
            setTopologyName(TOPOLOGY_NAME).
            setContainerIndex(-1).
            build();
    scheduler.getProcessToContainer().put(mockProcess, 0);
    Assert.assertTrue(scheduler.onRestart(restartAll));
  }

  @Test
  public void testStartExecutorMonitorNotKilled() throws Exception {
    int containerId = 0;
    int exitValue = 1;
    Process containerExecutor = Mockito.mock(Process.class);
    Mockito.doReturn(exitValue).when(containerExecutor).exitValue();
    Mockito.doNothing().when(scheduler).startExecutor(
        Mockito.anyInt(), Mockito.anySet());

    // Start the process
    scheduler.getProcessToContainer().put(containerExecutor, containerId);
    scheduler.startExecutorMonitor(
        containerId, containerExecutor,
        PackingTestUtils.testContainerPlan(containerId).getInstances());

    // Shut down the MonitorService
    scheduler.getMonitorService().shutdown();
    scheduler.getMonitorService().awaitTermination(MAX_WAITING_SECOND, TimeUnit.SECONDS);
    // The dead process should be restarted
    Mockito.verify(scheduler).startExecutor(containerId,
        PackingTestUtils.testContainerPlan(containerId).getInstances());
    Assert.assertFalse(scheduler.isTopologyKilled());
  }

  @Test
  public void testStartExecutorMonitorKilled() throws Exception {
    int containerId = 0;
    int exitValue = 1;

    Process containerExecutor = Mockito.mock(Process.class);
    Mockito.doReturn(exitValue).when(containerExecutor).exitValue();
    Mockito.doNothing().when(scheduler).startExecutor(Mockito.anyInt(), Mockito.anySet());

    // Set the killed flag and the dead process should not be restarted
    scheduler.onKill(Scheduler.KillTopologyRequest.getDefaultInstance());

    // Start the process
    scheduler.getProcessToContainer().put(containerExecutor, containerId);
    scheduler.startExecutorMonitor(containerId, containerExecutor,
        PackingTestUtils.testContainerPlan(containerId).getInstances());

    // Shut down the MonitorService
    scheduler.getMonitorService().shutdown();
    scheduler.getMonitorService().awaitTermination(MAX_WAITING_SECOND, TimeUnit.SECONDS);
    // The dead process should not be restarted
    Mockito.verify(scheduler, Mockito.never())
        .startExecutor(Mockito.anyInt(), Mockito.anySet());
    Assert.assertTrue(scheduler.isTopologyKilled());
  }
}
