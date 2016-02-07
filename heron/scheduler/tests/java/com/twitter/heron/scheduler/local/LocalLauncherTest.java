package com.twitter.heron.scheduler.local;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.twitter.heron.api.Config;
import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.common.basics.FileUtility;

import com.twitter.heron.spi.scheduler.context.LaunchContext;
import com.twitter.heron.scheduler.util.DefaultConfigLoader;
import com.twitter.heron.scheduler.util.RoundRobinPacking;
import com.twitter.heron.scheduler.util.ShellUtility;
import com.twitter.heron.scheduler.util.TopologyUtility;
import com.twitter.heron.scheduler.util.TopologyUtilityTest;

import junit.framework.Assert;

/**
 * LocalLauncher Tester.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({TopologyUtility.class, FileUtility.class, ShellUtility.class})

public class LocalLauncherTest {

  DefaultConfigLoader createRequiredConfig() throws Exception {
    DefaultConfigLoader schedulerConfig = DefaultConfigLoader.class.newInstance();
    schedulerConfig.properties.setProperty(LocalConfig.WORKING_DIRECTORY,
        LocalConfig.WORKING_DIRECTORY);
    schedulerConfig.addDefaultProperties();
    return schedulerConfig;
  }

  @Before
  public void before() throws Exception {
  }

  @After
  public void after() throws Exception {
  }

  /**
   * Method: launchTopology(Map<String, List<String>> packing)
   */
  @Test
  public void testLaunchTopology() throws Exception {
    int componentParallelism = 2;
    float cpuRequested = 8.0f;

    DefaultConfigLoader config = createRequiredConfig();

    RoundRobinPacking packing = new RoundRobinPacking();
    Config topologyConfig = new Config();
    topologyConfig.setNumStmgrs(2);
    topologyConfig.setContainerCpuRequested(cpuRequested);
    Map<String, Integer> spouts = new HashMap<>();
    spouts.put("spout", componentParallelism);
    Map<String, Integer> bolts = new HashMap<>();
    bolts.put("bolt", componentParallelism);
    TopologyAPI.Topology topology = TopologyUtilityTest.createTopology(
        "test", topologyConfig, spouts, bolts);

    LocalLauncher launcher = Mockito.spy(LocalLauncher.class.newInstance());
    LaunchContext context =
        new LaunchContext(config, topology);

    launcher.initialize(context);

    PowerMockito.doReturn(true).when(launcher).localSetup(new LocalConfig(context));
    PowerMockito.mockStatic(ShellUtility.class);
    Mockito.when(ShellUtility.runSyncProcess(Matchers.anyBoolean(), Matchers.anyBoolean(),
        Matchers.anyString(), Matchers.any(StringBuilder.class),
        Matchers.any(StringBuilder.class), Matchers.any(File.class))).thenReturn(0);

    PowerMockito.spy(TopologyUtility.class);
    PowerMockito.doReturn("").
        when(TopologyUtility.class, "makeClasspath", Matchers.any(TopologyAPI.Topology.class));

    PowerMockito.mockStatic(FileUtility.class);
    PowerMockito.
        when(FileUtility.getBaseName(Matchers.anyString())).thenReturn("");

    Assert.assertTrue(launcher.launchTopology(packing.pack(context)));

    PowerMockito.verifyStatic();
    ShellUtility.runSyncProcess(Matchers.anyBoolean(), Matchers.anyBoolean(),
        Matchers.startsWith("java -cp"), Matchers.any(StringBuilder.class),
        Matchers.any(StringBuilder.class), Matchers.any(File.class));
  }


  /**
   * Method: untarPackage(String packageName, String targetFolder)
   */
  @Test
  public void testUntarPackage() throws Exception {
    PowerMockito.mockStatic(ShellUtility.class);
    Mockito.when(ShellUtility.runSyncProcess(Matchers.anyBoolean(), Matchers.anyBoolean(),
        Matchers.anyString(), Matchers.any(StringBuilder.class),
        Matchers.any(StringBuilder.class), Matchers.any(File.class))).thenReturn(0);

    String packageName = "someTopologyPkg.tar";

    LocalLauncher launcher = Mockito.spy(LocalLauncher.class.newInstance());
    Assert.assertTrue(launcher.untarPackage(packageName, ""));

    String expectedUntarCmd = String.format("tar -xvf %s", packageName);

    PowerMockito.verifyStatic();
    ShellUtility.runSyncProcess(Matchers.anyBoolean(), Matchers.anyBoolean(),
        Matchers.eq(expectedUntarCmd), Matchers.any(StringBuilder.class),
        Matchers.any(StringBuilder.class), Matchers.any(File.class));
  }
} 
