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
import java.util.Map;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.twitter.heron.api.HeronTopology;
import com.twitter.heron.api.bolt.BaseBasicBolt;
import com.twitter.heron.api.bolt.BasicOutputCollector;
import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.api.spout.BaseRichSpout;
import com.twitter.heron.api.spout.SpoutOutputCollector;
import com.twitter.heron.api.topology.OutputFieldsDeclarer;
import com.twitter.heron.api.topology.TopologyBuilder;
import com.twitter.heron.api.topology.TopologyContext;
import com.twitter.heron.api.tuple.Tuple;
import com.twitter.heron.proto.system.ExecutionEnvironment;
import com.twitter.heron.proto.system.PackingPlans;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.ConfigKeys;
import com.twitter.heron.spi.common.Keys;
import com.twitter.heron.spi.packing.IPacking;
import com.twitter.heron.spi.packing.PackingPlan;
import com.twitter.heron.spi.scheduler.ILauncher;
import com.twitter.heron.spi.statemgr.SchedulerStateManagerAdaptor;
import com.twitter.heron.spi.utils.LauncherUtils;
import com.twitter.heron.spi.utils.Runtime;


@RunWith(PowerMockRunner.class)
@PrepareForTest(LauncherUtils.class)
public class LaunchRunnerTest {
  private static final String TOPOLOGY_NAME = "testTopology";
  private static final String CLUSTER = "testCluster";
  private static final String ROLE = "testRole";
  private static final String ENVIRON = "testEnviron";
  private static final String MOCK_PACKING_STRING = "mockPackString";
  private static final String BUILD_VERSION = "live";
  private static final String BUILD_USER = "user";

  private static TopologyAPI.Config.KeyValue getConfig(String key, String value) {
    return TopologyAPI.Config.KeyValue.newBuilder().setKey(key).setValue(value).
        setType(TopologyAPI.ConfigValueType.STRING_VALUE).build();
  }

  public static TopologyAPI.Topology createTopology(com.twitter.heron.api.Config heronConfig) {
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
    Config config = Mockito.mock(Config.class);
    Mockito.when(config.getStringValue(ConfigKeys.get("TOPOLOGY_NAME"))).thenReturn(TOPOLOGY_NAME);
    Mockito.when(config.getStringValue(ConfigKeys.get("CLUSTER"))).thenReturn(CLUSTER);
    Mockito.when(config.getStringValue(ConfigKeys.get("ROLE"))).thenReturn(ROLE);
    Mockito.when(config.getStringValue(ConfigKeys.get("ENVIRON"))).thenReturn(ENVIRON);
    Mockito.when(config.getStringValue(ConfigKeys.get("BUILD_VERSION"))).thenReturn(BUILD_VERSION);
    Mockito.when(config.getStringValue(ConfigKeys.get("BUILD_USER"))).thenReturn(BUILD_USER);

    return config;
  }

  private static Config createRunnerRuntime() throws Exception {
    Config runtime = Mockito.spy(Config.newBuilder().build());
    ILauncher launcher = Mockito.mock(ILauncher.class);
    IPacking packing = Mockito.mock(IPacking.class);
    SchedulerStateManagerAdaptor adaptor = Mockito.mock(SchedulerStateManagerAdaptor.class);
    TopologyAPI.Topology topology = createTopology(new com.twitter.heron.api.Config());

    Mockito.doReturn(launcher).when(runtime).get(Keys.launcherClassInstance());
    Mockito.doReturn(adaptor).when(runtime).get(Keys.schedulerStateManagerAdaptor());
    Mockito.doReturn(topology).when(runtime).get(Keys.topologyDefinition());

    PackingPlan packingPlan = Mockito.mock(PackingPlan.class);
    Mockito.when(packingPlan.getContainers()).thenReturn(
        new HashSet<PackingPlan.ContainerPlan>());
    Mockito.when(packingPlan.getComponentRamDistribution()).thenReturn("ramdist");
    Mockito.when(packingPlan.getId()).thenReturn("packing_plan_id");
    Mockito.when(packing.pack()).thenReturn(packingPlan);
    Mockito.when(packingPlan.getInstanceDistribution()).thenReturn(MOCK_PACKING_STRING);

    LauncherUtils mockLauncherUtils = Mockito.mock(LauncherUtils.class);
    Mockito.when(
        mockLauncherUtils.createPackingPlan(Mockito.any(Config.class), Mockito.any(Config.class)))
        .thenReturn(packingPlan);
    PowerMockito.spy(LauncherUtils.class);
    PowerMockito.doReturn(mockLauncherUtils).when(LauncherUtils.class, "getInstance");

    return runtime;
  }

  @Before
  public void setUp() throws Exception {
  }

  @Test
  public void testTrimTopology() throws Exception {
    LaunchRunner launchRunner = new LaunchRunner(createRunnerConfig(), createRunnerRuntime());
    TopologyAPI.Topology topologyBeforeTrimmed = createTopology(new com.twitter.heron.api.Config());
    TopologyAPI.Topology topologyAfterTrimmed = launchRunner.trimTopology(topologyBeforeTrimmed);

    for (TopologyAPI.Spout spout : topologyBeforeTrimmed.getSpoutsList()) {
      Assert.assertTrue(spout.getComp().hasSerializedObject());
    }

    for (TopologyAPI.Bolt bolt : topologyBeforeTrimmed.getBoltsList()) {
      Assert.assertTrue(bolt.getComp().hasSerializedObject());
    }

    for (TopologyAPI.Spout spout : topologyAfterTrimmed.getSpoutsList()) {
      Assert.assertFalse(spout.getComp().hasSerializedObject());
    }

    for (TopologyAPI.Bolt bolt : topologyAfterTrimmed.getBoltsList()) {
      Assert.assertFalse(bolt.getComp().hasSerializedObject());
    }
  }

  @Test
  public void testCreateExecutionState() throws Exception {
    LaunchRunner launchRunner = new LaunchRunner(createRunnerConfig(), createRunnerRuntime());
    ExecutionEnvironment.ExecutionState executionState = launchRunner.createExecutionState();

    Assert.assertTrue(executionState.isInitialized());

    Assert.assertEquals(TOPOLOGY_NAME, executionState.getTopologyName());
    Assert.assertEquals(CLUSTER, executionState.getCluster());
    Assert.assertEquals(ROLE, executionState.getRole());
    Assert.assertEquals(ENVIRON, executionState.getEnviron());
    Assert.assertEquals(System.getProperty("user.name"), executionState.getSubmissionUser());

    Assert.assertNotNull(executionState.getTopologyId());
    Assert.assertTrue(executionState.getSubmissionTime() <= (System.currentTimeMillis() / 1000));

    Assert.assertNotNull(executionState.getReleaseState());
    Assert.assertNotNull(executionState.getReleaseState().getReleaseVersion());
    Assert.assertNotNull(executionState.getReleaseState().getReleaseUsername());
  }

  @Test
  public void testSetExecutionStateFail() throws Exception {
    Config runtime = createRunnerRuntime();
    Config config = createRunnerConfig();
    ILauncher launcher = Runtime.launcherClassInstance(runtime);

    LaunchRunner launchRunner = new LaunchRunner(config, runtime);

    SchedulerStateManagerAdaptor statemgr = Runtime.schedulerStateManagerAdaptor(runtime);
    Mockito.when(
        statemgr.setExecutionState(
            Mockito.any(ExecutionEnvironment.ExecutionState.class), Mockito.eq(TOPOLOGY_NAME))).
        thenReturn(false);

    Assert.assertFalse(launchRunner.call());

    Mockito.verify(launcher, Mockito.never()).launch(Mockito.any(PackingPlan.class));
  }

  @Test
  public void testSetTopologyFail() throws Exception {
    Config runtime = createRunnerRuntime();
    Config config = createRunnerConfig();
    ILauncher launcher = Runtime.launcherClassInstance(runtime);

    LaunchRunner launchRunner = new LaunchRunner(config, runtime);

    SchedulerStateManagerAdaptor statemgr = Runtime.schedulerStateManagerAdaptor(runtime);
    Mockito.when(
        statemgr.setTopology(Mockito.any(TopologyAPI.Topology.class), Mockito.eq(TOPOLOGY_NAME))).
        thenReturn(false);

    Assert.assertFalse(launchRunner.call());

    Mockito.verify(launcher, Mockito.never()).launch(Mockito.any(PackingPlan.class));
  }

  @Test
  public void testLaunchFail() throws Exception {
    Config runtime = createRunnerRuntime();
    Config config = createRunnerConfig();
    ILauncher launcher = Runtime.launcherClassInstance(runtime);

    SchedulerStateManagerAdaptor statemgr = Runtime.schedulerStateManagerAdaptor(runtime);
    Mockito.when(
        statemgr.setTopology(Mockito.any(TopologyAPI.Topology.class), Mockito.eq(TOPOLOGY_NAME))).
        thenReturn(true);
    Mockito.when(
        statemgr.setPackingPlan(
            Mockito.any(PackingPlans.PackingPlan.class), Mockito.eq(TOPOLOGY_NAME))).
        thenReturn(true);
    Mockito.when(
        statemgr.setExecutionState(
            Mockito.any(ExecutionEnvironment.ExecutionState.class), Mockito.eq(TOPOLOGY_NAME))).
        thenReturn(true);

    LaunchRunner launchRunner = new LaunchRunner(config, runtime);
    Mockito.when(launcher.launch(Mockito.any(PackingPlan.class))).thenReturn(false);

    Assert.assertFalse(launchRunner.call());

    // Verify set && clean
    Mockito.verify(statemgr).setTopology(
        Mockito.any(TopologyAPI.Topology.class), Mockito.eq(TOPOLOGY_NAME));
    Mockito.verify(statemgr).setExecutionState(
        Mockito.any(ExecutionEnvironment.ExecutionState.class), Mockito.eq(TOPOLOGY_NAME));
    Mockito.verify(statemgr).deleteExecutionState(Mockito.eq(TOPOLOGY_NAME));
    Mockito.verify(statemgr).deleteTopology(Mockito.eq(TOPOLOGY_NAME));
  }

  @Test
  public void testCallSuccess() throws Exception {
    Config runtime = createRunnerRuntime();
    Config config = createRunnerConfig();
    ILauncher launcher = Runtime.launcherClassInstance(runtime);
    Mockito.when(launcher.launch(Mockito.any(PackingPlan.class))).thenReturn(true);

    SchedulerStateManagerAdaptor statemgr = Runtime.schedulerStateManagerAdaptor(runtime);
    Mockito.when(
        statemgr.setTopology(Mockito.any(TopologyAPI.Topology.class), Mockito.eq(TOPOLOGY_NAME))).
        thenReturn(true);
    Mockito.when(
        statemgr.setPackingPlan(
            Mockito.any(PackingPlans.PackingPlan.class), Mockito.eq(TOPOLOGY_NAME))).
        thenReturn(true);
    Mockito.when(
        statemgr.setExecutionState(
            Mockito.any(ExecutionEnvironment.ExecutionState.class), Mockito.eq(TOPOLOGY_NAME))).
        thenReturn(true);

    LaunchRunner launchRunner = new LaunchRunner(config, runtime);

    Assert.assertTrue(launchRunner.call());

    // Verify set && clean
    Mockito.verify(statemgr).setTopology(
        Mockito.any(TopologyAPI.Topology.class), Mockito.eq(TOPOLOGY_NAME));
    Mockito.verify(statemgr).setExecutionState(
        Mockito.any(ExecutionEnvironment.ExecutionState.class), Mockito.eq(TOPOLOGY_NAME));
    Mockito.verify(statemgr, Mockito.never()).deleteExecutionState(Mockito.eq(TOPOLOGY_NAME));
    Mockito.verify(statemgr, Mockito.never()).deleteTopology(Mockito.eq(TOPOLOGY_NAME));
  }
}
