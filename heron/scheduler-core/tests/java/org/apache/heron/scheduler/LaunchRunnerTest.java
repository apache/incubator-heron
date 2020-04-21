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

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import org.apache.heron.api.HeronTopology;
import org.apache.heron.api.bolt.BaseBasicBolt;
import org.apache.heron.api.bolt.BasicOutputCollector;
import org.apache.heron.api.generated.TopologyAPI;
import org.apache.heron.api.spout.BaseRichSpout;
import org.apache.heron.api.spout.SpoutOutputCollector;
import org.apache.heron.api.topology.OutputFieldsDeclarer;
import org.apache.heron.api.topology.TopologyBuilder;
import org.apache.heron.api.topology.TopologyContext;
import org.apache.heron.api.tuple.Tuple;
import org.apache.heron.proto.system.ExecutionEnvironment;
import org.apache.heron.proto.system.PackingPlans;
import org.apache.heron.scheduler.utils.LauncherUtils;
import org.apache.heron.scheduler.utils.Runtime;
import org.apache.heron.spi.common.Config;
import org.apache.heron.spi.common.Key;
import org.apache.heron.spi.packing.IPacking;
import org.apache.heron.spi.packing.PackingPlan;
import org.apache.heron.spi.packing.PackingPlan.ContainerPlan;
import org.apache.heron.spi.scheduler.ILauncher;
import org.apache.heron.spi.scheduler.LauncherException;
import org.apache.heron.spi.statemgr.SchedulerStateManagerAdaptor;
import org.apache.heron.spi.utils.PackingTestUtils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


@RunWith(PowerMockRunner.class)
@PowerMockIgnore("jdk.internal.reflect.*")
@PrepareForTest(LauncherUtils.class)
public class LaunchRunnerTest {
  private static final String TOPOLOGY_NAME = "testTopology";
  private static final String CLUSTER = "testCluster";
  private static final String ROLE = "testRole";
  private static final String ENVIRON = "testEnviron";
  private static final String SUBMIT_USER = "testUser";
  private static final String BUILD_VERSION = "live";
  private static final String BUILD_USER = "user";

  public static TopologyAPI.Topology createTopology(org.apache.heron.api.Config heronConfig) {
    TopologyBuilder builder = new TopologyBuilder();
    builder.setSpout("spout-1", new BaseRichSpout() {
      private static final long serialVersionUID = -762965195665496156L;

      public void declareOutputFields(OutputFieldsDeclarer declarer) {
      }

      public void open(
          Map<String, Object> conf,
          TopologyContext context,
          SpoutOutputCollector collector) {
      }

      public void nextTuple() {
      }
    }, 2);
    builder.setBolt("bolt-1", new BaseBasicBolt() {
      private static final long serialVersionUID = -5738458486388778812L;

      public void execute(Tuple input, BasicOutputCollector collector) {
      }

      public void declareOutputFields(OutputFieldsDeclarer declarer) {
      }
    }, 1);
    HeronTopology heronTopology = builder.createTopology();

    return heronTopology.
        setName(TOPOLOGY_NAME).
        setConfig(heronConfig).
        setState(TopologyAPI.TopologyState.RUNNING).
        getTopology();
  }

  private static Config createRunnerConfig() {
    Config config = mock(Config.class);
    when(config.getStringValue(Key.TOPOLOGY_NAME)).thenReturn(TOPOLOGY_NAME);
    when(config.getStringValue(Key.CLUSTER)).thenReturn(CLUSTER);
    when(config.getStringValue(Key.ROLE)).thenReturn(ROLE);
    when(config.getStringValue(Key.ENVIRON)).thenReturn(ENVIRON);
    when(config.getStringValue(Key.SUBMIT_USER)).thenReturn(SUBMIT_USER);
    when(config.getStringValue(Key.BUILD_VERSION)).thenReturn(BUILD_VERSION);
    when(config.getStringValue(Key.BUILD_USER)).thenReturn(BUILD_USER);

    return config;
  }

  private static Config createRunnerRuntime() throws Exception {
    return createRunnerRuntime(new org.apache.heron.api.Config());
  }

  private static Config createRunnerRuntime(
      org.apache.heron.api.Config topologyConfig) throws Exception {
    Config runtime = spy(Config.newBuilder().build());
    ILauncher launcher = mock(ILauncher.class);
    IPacking packing = mock(IPacking.class);
    SchedulerStateManagerAdaptor adaptor = mock(SchedulerStateManagerAdaptor.class);
    TopologyAPI.Topology topology = createTopology(topologyConfig);

    doReturn(launcher).when(runtime).get(Key.LAUNCHER_CLASS_INSTANCE);
    doReturn(adaptor).when(runtime).get(Key.SCHEDULER_STATE_MANAGER_ADAPTOR);
    doReturn(topology).when(runtime).get(Key.TOPOLOGY_DEFINITION);

    PackingPlan packingPlan = mock(PackingPlan.class);
    when(packingPlan.getContainers()).thenReturn(
        new HashSet<ContainerPlan>());
    when(packingPlan.getComponentRamDistribution()).thenReturn("ramdist");
    when(packingPlan.getId()).thenReturn("packing_plan_id");
    Set<ContainerPlan> containerPlans = new HashSet<>();
    containerPlans.add(PackingTestUtils.testContainerPlan(1)); // just need it to be of size 1
    when(packingPlan.getContainers()).thenReturn(containerPlans);
    when(packing.pack()).thenReturn(packingPlan);

    LauncherUtils mockLauncherUtils = mock(LauncherUtils.class);
    when(mockLauncherUtils.createPackingPlan(any(Config.class), any(Config.class)))
        .thenReturn(packingPlan);
    PowerMockito.spy(LauncherUtils.class);
    PowerMockito.doReturn(mockLauncherUtils).when(LauncherUtils.class, "getInstance");

    return runtime;
  }

  private static SchedulerStateManagerAdaptor createTestSchedulerStateManager(Config runtime) {
    SchedulerStateManagerAdaptor statemgr = Runtime.schedulerStateManagerAdaptor(runtime);
    when(statemgr.setTopology(any(TopologyAPI.Topology.class), eq(TOPOLOGY_NAME))).
        thenReturn(true);
    when(statemgr.setPackingPlan(any(PackingPlans.PackingPlan.class), eq(TOPOLOGY_NAME))).
        thenReturn(true);
    when(statemgr.setExecutionState(
        any(ExecutionEnvironment.ExecutionState.class), eq(TOPOLOGY_NAME))).
        thenReturn(true);
    return statemgr;
  }

  @Before
  public void setUp() throws Exception {
  }

  @Test
  public void testTrimTopology() throws Exception {
    LaunchRunner launchRunner = new LaunchRunner(createRunnerConfig(), createRunnerRuntime());
    TopologyAPI.Topology topologyBeforeTrimmed = createTopology(new org.apache.heron.api.Config());
    TopologyAPI.Topology topologyAfterTrimmed = launchRunner.trimTopology(topologyBeforeTrimmed);

    for (TopologyAPI.Spout spout : topologyBeforeTrimmed.getSpoutsList()) {
      assertTrue(spout.getComp().hasSerializedObject());
    }

    for (TopologyAPI.Bolt bolt : topologyBeforeTrimmed.getBoltsList()) {
      assertTrue(bolt.getComp().hasSerializedObject());
    }

    for (TopologyAPI.Spout spout : topologyAfterTrimmed.getSpoutsList()) {
      assertFalse(spout.getComp().hasSerializedObject());
    }

    for (TopologyAPI.Bolt bolt : topologyAfterTrimmed.getBoltsList()) {
      assertFalse(bolt.getComp().hasSerializedObject());
    }
  }

  @Test
  public void testCreateExecutionState() throws Exception {
    LaunchRunner launchRunner = new LaunchRunner(createRunnerConfig(), createRunnerRuntime());
    ExecutionEnvironment.ExecutionState executionState = launchRunner.createExecutionState();

    assertTrue(executionState.isInitialized());

    assertEquals(TOPOLOGY_NAME, executionState.getTopologyName());
    assertEquals(CLUSTER, executionState.getCluster());
    assertEquals(ROLE, executionState.getRole());
    assertEquals(ENVIRON, executionState.getEnviron());
    assertEquals(SUBMIT_USER, executionState.getSubmissionUser());

    assertNotNull(executionState.getTopologyId());
    assertTrue(executionState.getSubmissionTime() <= (System.currentTimeMillis() / 1000));

    assertNotNull(executionState.getReleaseState());
    assertNotNull(executionState.getReleaseState().getReleaseVersion());
    assertNotNull(executionState.getReleaseState().getReleaseUsername());
  }

  @Test(expected = LauncherException.class)
  public void testSetExecutionStateFail() throws Exception {
    Config runtime = createRunnerRuntime();
    Config config = createRunnerConfig();
    ILauncher launcher = Runtime.launcherClassInstance(runtime);

    LaunchRunner launchRunner = new LaunchRunner(config, runtime);

    SchedulerStateManagerAdaptor statemgr = Runtime.schedulerStateManagerAdaptor(runtime);
    when(statemgr.setExecutionState(
        any(ExecutionEnvironment.ExecutionState.class), eq(TOPOLOGY_NAME))).
        thenReturn(false);

    try {
      launchRunner.call();
    } finally {
      verify(launcher, never()).launch(any(PackingPlan.class));
    }
  }

  @Test(expected = LauncherException.class)
  public void testSetTopologyFail() throws Exception {
    Config runtime = createRunnerRuntime();
    Config config = createRunnerConfig();
    ILauncher launcher = Runtime.launcherClassInstance(runtime);

    LaunchRunner launchRunner = new LaunchRunner(config, runtime);

    SchedulerStateManagerAdaptor statemgr = Runtime.schedulerStateManagerAdaptor(runtime);
    when(statemgr.setTopology(any(TopologyAPI.Topology.class), eq(TOPOLOGY_NAME)))
        .thenReturn(false);

    try {
      launchRunner.call();
    } finally {
      verify(launcher, never()).launch(any(PackingPlan.class));
    }
  }

  @Test(expected = LauncherException.class)
  public void testLaunchFailCleanUp() throws Exception {
    Config runtime = createRunnerRuntime();
    Config config = createRunnerConfig();
    ILauncher launcher = Runtime.launcherClassInstance(runtime);
    SchedulerStateManagerAdaptor statemgr = createTestSchedulerStateManager(runtime);

    LaunchRunner launchRunner = new LaunchRunner(config, runtime);
    when(launcher.launch(any(PackingPlan.class))).thenReturn(false);

    try {
      launchRunner.call();
    } finally {
      // Verify set && clean
      verify(statemgr).setTopology(any(TopologyAPI.Topology.class), eq(TOPOLOGY_NAME));
      verify(statemgr).setExecutionState(
          any(ExecutionEnvironment.ExecutionState.class), eq(TOPOLOGY_NAME));
      verify(statemgr).deleteExecutionState(eq(TOPOLOGY_NAME));
      verify(statemgr).deleteTopology(eq(TOPOLOGY_NAME));
    }
  }

  @Test
  public void testCallSuccess() throws Exception {
    doTestLaunch(new org.apache.heron.api.Config());
  }

  @Test
  public void testCallSuccessWithDifferentNumContainers() throws Exception {
    org.apache.heron.api.Config topologyConfig = new org.apache.heron.api.Config();
    topologyConfig.setNumStmgrs(2); // packing plan has only 1 container plan but numStmgrs is 2

    doTestLaunch(topologyConfig);
  }

  private void doTestLaunch(org.apache.heron.api.Config topologyConfig) throws Exception {
    Config runtime = createRunnerRuntime(topologyConfig);
    Config config = createRunnerConfig();
    ILauncher launcher = Runtime.launcherClassInstance(runtime);
    SchedulerStateManagerAdaptor statemgr = createTestSchedulerStateManager(runtime);

    LaunchRunner launchRunner = new LaunchRunner(config, runtime);
    when(launcher.launch(any(PackingPlan.class))).thenReturn(true);

    launchRunner.call();

    // Verify set && clean
    verify(statemgr).setTopology(any(TopologyAPI.Topology.class), eq(TOPOLOGY_NAME));
    verify(statemgr).setExecutionState(
        any(ExecutionEnvironment.ExecutionState.class), eq(TOPOLOGY_NAME));
    verify(statemgr, never()).deleteExecutionState(eq(TOPOLOGY_NAME));
    verify(statemgr, never()).deleteTopology(eq(TOPOLOGY_NAME));
  }
}
