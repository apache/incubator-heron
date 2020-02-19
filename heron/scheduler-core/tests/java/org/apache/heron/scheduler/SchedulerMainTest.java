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

package org.apache.heron.scheduler;

import java.util.HashMap;
import java.util.Properties;

import com.google.common.util.concurrent.SettableFuture;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import org.apache.heron.api.generated.TopologyAPI;
import org.apache.heron.api.utils.TopologyUtils;
import org.apache.heron.common.utils.topology.TopologyTests;
import org.apache.heron.packing.roundrobin.RoundRobinPacking;
import org.apache.heron.proto.system.PackingPlans;
import org.apache.heron.scheduler.server.SchedulerServer;
import org.apache.heron.scheduler.utils.Runtime;
import org.apache.heron.scheduler.utils.SchedulerUtils;
import org.apache.heron.scheduler.utils.Shutdown;
import org.apache.heron.spi.common.Config;
import org.apache.heron.spi.common.Key;
import org.apache.heron.spi.packing.PackingPlan;
import org.apache.heron.spi.scheduler.IScheduler;
import org.apache.heron.spi.statemgr.IStateManager;
import org.apache.heron.spi.utils.PackingTestUtils;
import org.apache.heron.spi.utils.ReflectionUtils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


@RunWith(PowerMockRunner.class)
@PowerMockIgnore("jdk.internal.reflect.*")
@PrepareForTest({
    TopologyUtils.class, ReflectionUtils.class, SchedulerUtils.class, TopologyAPI.Topology.class})
public class SchedulerMainTest {
  private static final String STATE_MANAGER_CLASS = "STATE_MANAGER_CLASS";
  private static final String SCHEDULER_CLASS = "SCHEDULER_CLASS";
  private static final String TOPOLOGY_NAME = "topologyName";
  @Rule
  public final ExpectedException exception = ExpectedException.none();
  private IStateManager stateManager;
  private IScheduler scheduler;
  private SchedulerMain schedulerMain;
  private SchedulerServer schedulerServer;
  private String iTopologyName = "topologyName";

  /**
   * Basic setup before executing a test case
   */
  @Before
  public void setUp() throws Exception {
    Config config = mock(Config.class);
    when(config.getStringValue(Key.STATE_MANAGER_CLASS)).
        thenReturn(STATE_MANAGER_CLASS);
    when(config.getStringValue(Key.SCHEDULER_CLASS)).
        thenReturn(SCHEDULER_CLASS);

    int iSchedulerServerPort = 0;

    TopologyAPI.Topology topology = TopologyTests.createTopology(
        iTopologyName, new org.apache.heron.api.Config(),
        new HashMap<String, Integer>(), new HashMap<String, Integer>());

    // Mock objects to be verified
    stateManager = mock(IStateManager.class);
    scheduler = mock(IScheduler.class);

    final SettableFuture<PackingPlans.PackingPlan> future = getTestPacking();
    when(stateManager.getPackingPlan(null, iTopologyName)).thenReturn(future);

    // Mock ReflectionUtils stuff
    PowerMockito.spy(ReflectionUtils.class);
    PowerMockito.doReturn(stateManager).
        when(ReflectionUtils.class, "newInstance", STATE_MANAGER_CLASS);
    PowerMockito.doReturn(scheduler).
        when(ReflectionUtils.class, "newInstance", SCHEDULER_CLASS);

    // Mock objects to be verified
    schedulerMain = spy(new SchedulerMain(
        config, topology, iSchedulerServerPort, mock(Properties.class)));
    schedulerServer = mock(SchedulerServer.class);
    doReturn(schedulerServer).when(schedulerMain).getServer(
        any(Config.class), eq(scheduler), eq(iSchedulerServerPort));

    doReturn(true).when(scheduler).onSchedule(any(PackingPlan.class));

    // Mock SchedulerUtils stuff
    PowerMockito.spy(SchedulerUtils.class);
    PowerMockito.doReturn(true).when(SchedulerUtils.class, "setSchedulerLocation",
        any(Config.class), anyString(), eq(scheduler));

    // Avoid infinite waiting
    Shutdown shutdown = mock(Shutdown.class);
    doReturn(shutdown).when(schedulerMain).getShutdown();
  }

  private SettableFuture<PackingPlans.PackingPlan> getTestPacking() {
    PackingPlans.PackingPlan packingPlan =
        PackingTestUtils.testProtoPackingPlan("testTopology", new RoundRobinPacking());
    final SettableFuture<PackingPlans.PackingPlan> future = SettableFuture.create();
    assertTrue(future.set(packingPlan));
    return future;
  }

  // Exceptions during reflection --
  // 1. should return false executing runScheduler()
  // 2. Nothing should be initialized
  @Test
  public void testExceptionsInReflections() throws Exception {
    PowerMockito.doThrow(new ClassNotFoundException("")).
        when(ReflectionUtils.class, "newInstance", STATE_MANAGER_CLASS);
    assertFalse(schedulerMain.runScheduler());
    verify(stateManager, never()).initialize(any(Config.class));

    verify(stateManager, never()).getPackingPlan(null, iTopologyName);
    verify(scheduler, never()).initialize(any(Config.class), any(Config.class));
  }

  // Exceptions during initialize components --
  // 1. should bubble up the exceptions directly executing runScheduler()
  @Test
  public void testExceptionsInInit() throws Exception {
    PowerMockito.doReturn(stateManager).
        when(ReflectionUtils.class, "newInstance", STATE_MANAGER_CLASS);
    doThrow(new RuntimeException()).when(stateManager).initialize(any(Config.class));
    exception.expect(RuntimeException.class);
    schedulerMain.runScheduler();

    // Should not be invoked; thread exited already
    fail();
  }

  // Failed to IScheduler.onSchedule
  // 1. should return false executing runScheduler()
  // 2. close() should still be invoked
  // 3. SchedulerServer should not start
  @Test
  public void testOnSchedulerFailure() throws Exception {
    doNothing().when(stateManager).initialize(any(Config.class));
    doReturn(false).when(scheduler).onSchedule(any(PackingPlan.class));
    assertFalse(schedulerMain.runScheduler());
    verify(stateManager).close();
    verify(scheduler).close();
    verify(schedulerServer, never()).start();
  }

  // Exceptions during start the server
  // 1. should bubble up the exceptions executing runScheduler()

  @Test
  public void testExceptionsInStartingServer() throws Exception {
    doThrow(new RuntimeException()).when(schedulerServer).start();
    exception.expect(RuntimeException.class);
    schedulerMain.runScheduler();

    // Should not be invoked; thread exited already
    fail();
  }

  // Failed to set SchedulerLocation
  // 1. should return false executing runScheduler()
  // 2. close() should still be invoked
  // 3. SchedulerServer.stop() should be invoked
  @Test
  public void testSetSchedulerLocationFailure() throws Exception {
    PowerMockito.doReturn(false).when(SchedulerUtils.class, "setSchedulerLocation",
        any(Config.class), anyString(), eq(scheduler));
    assertFalse(schedulerMain.runScheduler());

    verify(stateManager).close();
    verify(scheduler).close();
    verify(schedulerServer).stop();
  }

  // Happy path
  @Test
  public void testRunScheduler() throws Exception {
    assertTrue(schedulerMain.runScheduler());
  }

  @Test
  public void updateNumContainersIfNeeded() {

    int configuredNumContainers = 4;
    int configuredNumStreamManagers = configuredNumContainers - 1;
    int packingPlanSize = 1;

    SubmitterMain submitterMain = new SubmitterMain(
        Config.newBuilder()
        .put(Key.PACKING_CLASS, "org.apache.heron.packing.roundrobin.ResourceCompliantRRPacking")
        .build(), null);
    PackingPlan packingPlan =
        PackingTestUtils.testPackingPlan(TOPOLOGY_NAME, new RoundRobinPacking());
    assertEquals(packingPlanSize, packingPlan.getContainers().size());

    org.apache.heron.api.Config apiConfig = new org.apache.heron.api.Config();
    apiConfig.setNumStmgrs(configuredNumStreamManagers);

    TopologyAPI.Topology initialTopology = TopologyTests.createTopology(
        TOPOLOGY_NAME, apiConfig, new HashMap<String, Integer>(), new HashMap<String, Integer>());

    Config initialConfig = Config.newBuilder()
        .put(Key.NUM_CONTAINERS, configuredNumContainers)
        .put(Key.PACKING_CLASS, RoundRobinPacking.class.getName())
        .put(Key.TOPOLOGY_DEFINITION, initialTopology)
        .build();

    // assert preconditions
    assertEquals(Integer.toString(configuredNumStreamManagers),
        apiConfig.get(org.apache.heron.api.Config.TOPOLOGY_STMGRS));
    assertEquals(configuredNumStreamManagers, TopologyUtils.getNumContainers(initialTopology));

    assertContainerCount(configuredNumStreamManagers, initialConfig);

    Config newConfig =
        submitterMain.updateNumContainersIfNeeded(initialConfig, initialTopology, packingPlan);

    assertContainerCount(packingPlanSize, newConfig);
  }

  private void assertContainerCount(int expectedNumStreamManagers, Config config) {
    assertEquals(expectedNumStreamManagers + 1, config.get(Key.NUM_CONTAINERS));
    assertEquals(expectedNumStreamManagers,
        TopologyUtils.getNumContainers(Runtime.topology(config)));
  }
}
