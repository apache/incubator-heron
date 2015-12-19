package com.twitter.heron.scheduler.service;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.util.concurrent.SettableFuture;

import org.junit.Before;
import org.junit.Test;

import com.twitter.heron.api.Config;
import com.twitter.heron.api.HeronSubmitter;
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
import com.twitter.heron.scheduler.api.IConfigLoader;
import com.twitter.heron.scheduler.api.ILauncher;
import com.twitter.heron.scheduler.api.IPackingAlgorithm;
import com.twitter.heron.scheduler.api.IUploader;
import com.twitter.heron.scheduler.api.PackingPlan;
import com.twitter.heron.scheduler.api.SchedulerStateManagerAdaptor;
import com.twitter.heron.scheduler.api.context.LaunchContext;
import com.twitter.heron.scheduler.util.Nullity;
import com.twitter.heron.scheduler.util.TopologyUtilityTest;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.notNull;
import static org.mockito.Mockito.*;

public class LaunchRunnerTest {
  private TopologyAPI.Topology topology;
  private LaunchRunner launchRunner;
  private IUploader uploader;
  private ILauncher launcher;
  private LaunchContext context;
  private IConfigLoader config;
  private IPackingAlgorithm packingAlgorithm;
  private SchedulerStateManagerAdaptor stateManager;
  private Map<String, List<String>> packingInfo;

  private static TopologyAPI.Config.KeyValue getConfig(String key, String value) {
    return TopologyAPI.Config.KeyValue.newBuilder().setKey(key).setValue(value).build();
  }

  public static TopologyAPI.Topology createTopology(Config heronConfig) {
    TopologyBuilder builder = new TopologyBuilder();
    builder.setSpout("spout-1", new BaseRichSpout() {
      public void declareOutputFields(OutputFieldsDeclarer declarer) {
      }

      public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
      }

      public void nextTuple() {
      }
    }, 2);
    builder.setBolt("bolt-1", new BaseBasicBolt() {
      public void execute(Tuple input, BasicOutputCollector collector) {
      }

      public void declareOutputFields(OutputFieldsDeclarer declarer) {
      }
    }, 1);
    HeronTopology heronTopology = builder.createTopology();
    try {
      HeronSubmitter.submitTopology("testTopology", heronConfig, heronTopology);
    } catch (Exception e) {
    }

    return heronTopology.
        setName("testTopology").
        setConfig(heronConfig).
        setState(TopologyAPI.TopologyState.RUNNING).
        getTopology();
  }

  @Before
  public void setUp() throws Exception {
    uploader = mock(IUploader.class);
    config = mock(IConfigLoader.class);
    packingAlgorithm = mock(IPackingAlgorithm.class);
    launcher = mock(ILauncher.class);
    stateManager = mock(SchedulerStateManagerAdaptor.class);
    SettableFuture<Boolean> trueFuture = SettableFuture.create();
    trueFuture.set(true);
    packingInfo = new HashMap<>();
    topology = createTopology(new Config());
    Map<String, List<String>> packing = new HashMap<>();
    packing.put("1", Arrays.asList("spout-1:1:0", "spout-1:3:1", "bolt-1:2:0"));

    context = spy(new LaunchContext(config, topology));

    when(packingAlgorithm.pack(eq(context))).thenReturn(
        TopologyUtilityTest.generatePacking(packing));
    when(stateManager.setExecutionState(any(ExecutionEnvironment.ExecutionState.class)))
        .thenReturn(trueFuture);
    when(stateManager.setTopology(any(TopologyAPI.Topology.class)))
        .thenReturn(trueFuture);
    when(stateManager.setTopology(eq(topology))).thenReturn(trueFuture);
    when(context.getStateManagerAdaptor()).thenReturn(stateManager);
    when(config.getSchedulerClass()).thenReturn(Nullity.NullScheduler.class.getName());
    when(launcher.launchTopology(any(PackingPlan.class))).thenReturn(true);
    when(launcher.prepareLaunch(any(PackingPlan.class))).thenReturn(true);
    when(launcher.postLaunch(any(PackingPlan.class))).thenReturn(true);
    launchRunner = new LaunchRunner(launcher, context, packingAlgorithm);
  }

  @Test
  public void testLaunchRunner() {
    assertTrue(launchRunner.call());
    verify(launcher).initialize(eq(context));
    verify(launcher).updateExecutionState(notNull(ExecutionEnvironment.ExecutionState.class));
    verify(packingAlgorithm).pack(eq(context));
    verify(launcher).launchTopology(eq(packingAlgorithm.pack(context)));
  }

  @Test
  public void testExecutionStateUpdateFail() {
    SettableFuture<Boolean> falseFuture = SettableFuture.create();
    falseFuture.set(false);
    when(stateManager.setExecutionState(any(ExecutionEnvironment.ExecutionState.class)))
        .thenReturn(falseFuture);
    assertFalse(launchRunner.call());
    // Verify that topologies state don't get called.
    verify(stateManager, never()).setTopology(any(TopologyAPI.Topology.class));
    verify(launcher, never()).launchTopology(any(PackingPlan.class));
  }

  @Test
  public void testTopologyUpdateFail() {
    SettableFuture<Boolean> falseFuture = SettableFuture.create();
    falseFuture.set(false);
    when(stateManager.setTopology(any(TopologyAPI.Topology.class)))
        .thenReturn(falseFuture);
    assertFalse(launchRunner.call());
    // Verify that topologies state don't get called.
    verify(stateManager).clearExecutionState();
    verify(launcher, never()).launchTopology(any(PackingPlan.class));
  }
}
