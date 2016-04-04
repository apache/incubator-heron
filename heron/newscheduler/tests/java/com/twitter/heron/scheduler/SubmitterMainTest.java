package com.twitter.heron.scheduler.service;

import java.nio.charset.Charset;
import java.util.Map;

import javax.xml.bind.DatatypeConverter;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

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

import com.twitter.heron.spi.scheduler.IConfigLoader;
import com.twitter.heron.spi.scheduler.ILauncher;
import com.twitter.heron.spi.scheduler.NullLauncher;

import com.twitter.heron.spi.scheduler.NullScheduler;

import com.twitter.heron.spi.uploader.IUploader;
import com.twitter.heron.spi.uploader.NullUploader;

import com.twitter.heron.spi.common.PackingPlan;
import com.twitter.heron.spi.packing.NullPackingAlgorithm;

import com.twitter.heron.spi.scheduler.context.LaunchContext;
import com.twitter.heron.spi.util.Factory;

import com.twitter.heron.scheduler.util.DefaultConfigLoader;
import com.twitter.heron.spi.statemgr.IStateManager;
import com.twitter.heron.statemgr.NullStateManager;

import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

@RunWith(PowerMockRunner.class)
@PrepareForTest({SubmitterMain.class, Factory.class})
public class SubmitterMainTest {

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

  private IConfigLoader createConfig() {
    IConfigLoader config = mock(DefaultConfigLoader.class);
    when(config.getUploaderClass()).thenReturn(NullUploader.class.getName());
    when(config.getLauncherClass()).thenReturn(NullLauncher.class.getName());
    when(config.getSchedulerClass()).thenReturn(NullScheduler.class.getName());
    when(config.getPackingAlgorithmClass()).thenReturn(NullPackingAlgorithm.class.getName());
    when(config.getStateManagerClass()).thenReturn(NullStateManager.class.getName());
    when(config.load(anyString(), anyString())).thenReturn(true);
    return config;
  }

  @Test
  public void testSubmitterMainWorkflow() throws Exception {
    String topologyPkg = "some-file.tar";
    IConfigLoader config = createConfig();
    String configLoader = config.getClass().getName();
    String submitterConfigFile = "";
    String configOverride = "";
    PowerMockito.spy(SubmitterMain.class);
    PowerMockito.spy(Factory.class);

    PowerMockito.doReturn(config).when(Factory.class, "makeConfigLoader", eq(configLoader));
    PowerMockito.doReturn(new NullStateManager()).when(Factory.class, "makeStateManager", anyString());

    assertTrue(SubmitterMain.submitTopology(
        topologyPkg, configLoader, submitterConfigFile, configOverride, createTopology(new Config())));
  }

  @Test
  public void testUploadFailed() throws Exception {
    String topologyPkg = "some-file.tar";
    IConfigLoader config = createConfig();
    String configLoader = config.getClass().getName();
    String submitterConfigFile = "";
    String configOverride = DatatypeConverter.printBase64Binary(
        "key:value".getBytes(Charset.forName("UTF-8")));
    IUploader failUploader = mock(IUploader.class);
    ILauncher mockLauncher = mock(ILauncher.class);
    PowerMockito.spy(SubmitterMain.class);
    PowerMockito.spy(Factory.class);
    PowerMockito.doReturn(mock(IStateManager.class)).when(Factory.class, "makeStateManager", anyString());
    PowerMockito.doReturn(config).when(Factory.class, "makeConfigLoader", eq(configLoader));
    PowerMockito.doReturn(failUploader).when(Factory.class, "makeUploader", anyString());
    PowerMockito.doReturn(mockLauncher).when(Factory.class, "makeLauncher", anyString());
    when(failUploader.uploadPackage()).thenReturn(null);

    assertTrue(!SubmitterMain.submitTopology(
        topologyPkg, configLoader, submitterConfigFile, configOverride, createTopology(new Config())));
    verify(mockLauncher, never()).initialize(any(LaunchContext.class));
    verify(mockLauncher, never()).launchTopology(any(PackingPlan.class));
  }

  @Test
  public void testLaunchFailed() throws Exception {
    String topologyPkg = "some-file.tar";
    IConfigLoader config = createConfig();
    String configLoader = config.getClass().getName();
    String submitterConfigFile = "";
    String configOverride = "";
    IUploader mockUploader = spy(new NullUploader());
    ILauncher failLauncher = spy(new NullLauncher());
    IStateManager dummyStateManager = spy(new NullStateManager());
    when(failLauncher.launchTopology(any(PackingPlan.class))).thenReturn(false);
    PowerMockito.spy(SubmitterMain.class);
    PowerMockito.spy(Factory.class);
    PowerMockito.doReturn(config).when(Factory.class, "makeConfigLoader", eq(configLoader));
    PowerMockito.doReturn(dummyStateManager).when(Factory.class, "makeStateManager", anyString());
    PowerMockito.doReturn(mockUploader).when(Factory.class, "makeUploader", anyString());
    PowerMockito.doReturn(failLauncher).when(Factory.class, "makeLauncher", anyString());

    assertTrue(!SubmitterMain.submitTopology(
        topologyPkg, configLoader, submitterConfigFile, configOverride, createTopology(new Config())));

    inOrder(mockUploader, dummyStateManager, failLauncher);
    verify(mockUploader, times(1)).initialize(any(LaunchContext.class));
    verify(mockUploader, times(1)).uploadPackage(anyString());

    verify(failLauncher, times(1)).initialize(any(LaunchContext.class));
    verify(dummyStateManager, times(1))
        .setExecutionState(any(ExecutionEnvironment.ExecutionState.class), anyString());
    verify(dummyStateManager, times(1)).setTopology(any(TopologyAPI.Topology.class), anyString());
    verify(failLauncher, times(1)).launchTopology(any(PackingPlan.class));
    verify(dummyStateManager, times(1)).deleteExecutionState(anyString());
    verify(dummyStateManager, times(1)).deleteTopology(anyString());
    verify(mockUploader, times(1)).undo();
  }
}
