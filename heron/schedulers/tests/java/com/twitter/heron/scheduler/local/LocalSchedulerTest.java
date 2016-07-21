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

package com.twitter.heron.scheduler.local;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.twitter.heron.proto.scheduler.Scheduler;
import com.twitter.heron.spi.common.ConfigKeys;
import com.twitter.heron.spi.common.Keys;
import com.twitter.heron.spi.common.SpiCommonConfig;
import com.twitter.heron.spi.packing.PackingPlan;

public class LocalSchedulerTest {
  private static final String TOPOLOGY_NAME = "testTopology";
  private static final long NUM_CONTAINER = 2;
  private static final int MAX_WAITING_SECOND = 10;
  private static LocalScheduler scheduler;

  @Before
  public void before() throws Exception {
    scheduler = Mockito.spy(LocalScheduler.class);
    SpiCommonConfig config = Mockito.mock(SpiCommonConfig.class);
    Mockito.when(config.getStringValue(ConfigKeys.get("TOPOLOGY_NAME"))).thenReturn(TOPOLOGY_NAME);

    SpiCommonConfig runtime = Mockito.mock(SpiCommonConfig.class);
    Mockito.when(runtime.getLongValue(Keys.numContainers())).thenReturn(NUM_CONTAINER);
    scheduler.initialize(config, runtime);
  }

  @After
  public void after() throws Exception {
    scheduler.close();
  }

  @Test
  public void testClose() throws Exception {
    LocalScheduler localScheduler = new LocalScheduler();
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
        when(scheduler).startExecutorMonitor(Mockito.anyInt(), Mockito.any(Process.class));
    Process mockProcess = Mockito.mock(Process.class);
    Mockito.doReturn(mockProcess).
        when(scheduler).startExecutorProcess(Mockito.anyInt());

    PackingPlan packingPlan = Mockito.mock(PackingPlan.class);
    Assert.assertTrue(scheduler.onSchedule(packingPlan));
    Assert.assertTrue(scheduler.getProcessToContainer().size() > 0);

    for (int i = 0; i < NUM_CONTAINER; i++) {
      Mockito.verify(scheduler).startExecutor(i);
      Mockito.verify(scheduler).startExecutorProcess(i);
      Mockito.verify(scheduler).startExecutorMonitor(i, mockProcess);
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
    Mockito.doNothing().when(scheduler).startExecutor(Mockito.anyInt());

    // Start the process
    scheduler.getProcessToContainer().put(containerExecutor, containerId);
    scheduler.startExecutorMonitor(containerId, containerExecutor);

    // Shut down the MonitorService
    scheduler.getMonitorService().shutdown();
    scheduler.getMonitorService().awaitTermination(MAX_WAITING_SECOND, TimeUnit.SECONDS);
    // The dead process should be restarted
    Mockito.verify(scheduler).startExecutor(containerId);
    Assert.assertFalse(scheduler.isTopologyKilled());
  }

  @Test
  public void testStartExecutorMonitorKilled() throws Exception {
    int containerId = 0;
    int exitValue = 1;

    Process containerExecutor = Mockito.mock(Process.class);
    Mockito.doReturn(exitValue).when(containerExecutor).exitValue();
    Mockito.doNothing().when(scheduler).startExecutor(Mockito.anyInt());

    // Set the killed flag and the dead process should not be restarted
    scheduler.onKill(Scheduler.KillTopologyRequest.getDefaultInstance());

    // Start the process
    scheduler.getProcessToContainer().put(containerExecutor, containerId);
    scheduler.startExecutorMonitor(containerId, containerExecutor);

    // Shut down the MonitorService
    scheduler.getMonitorService().shutdown();
    scheduler.getMonitorService().awaitTermination(MAX_WAITING_SECOND, TimeUnit.SECONDS);
    // The dead process should not be restarted
    Mockito.verify(scheduler, Mockito.never()).startExecutor(Mockito.anyInt());
    Assert.assertTrue(scheduler.isTopologyKilled());
  }
}
