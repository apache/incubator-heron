package com.twitter.heron.scheduler.aurora;

import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import com.twitter.heron.api.Config;
import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.scheduler.api.Constants;
import com.twitter.heron.scheduler.api.context.LaunchContext;
import com.twitter.heron.scheduler.util.DefaultConfigLoader;
import com.twitter.heron.scheduler.util.RoundRobinPacking;
import com.twitter.heron.scheduler.util.TopologyUtilityTest;

public class AuroraLauncherTest {
  DefaultConfigLoader createRequiredConfig() throws Exception {
    DefaultConfigLoader schedulerConfig = DefaultConfigLoader.class.newInstance();
    schedulerConfig.addDefaultProperties();
    schedulerConfig.properties.setProperty(Constants.DC, "dc");
    schedulerConfig.properties.setProperty(Constants.ROLE, "me");
    schedulerConfig.properties.setProperty(Constants.ENVIRON, "environ");
    schedulerConfig.properties.setProperty(Constants.HERON_RELEASE_PACKAGE_ROLE, "me");
    schedulerConfig.properties.getProperty(Constants.HERON_RELEASE_PACKAGE_NAME, "some-pkg");
    schedulerConfig.properties.getProperty(Constants.HERON_RELEASE_PACKAGE_VERSION, "live");
    schedulerConfig.properties.setProperty(Constants.HERON_UPLOADER_VERSION, "1");
    schedulerConfig.properties.setProperty(RoundRobinPacking.INSTANCE_CPU_DEFAULT, "1.0");
    schedulerConfig.properties.setProperty(RoundRobinPacking.INSTANCE_RAM_DEFAULT,
        Long.toString(1 * Constants.GB));
    schedulerConfig.properties.setProperty(RoundRobinPacking.INSTANCE_DISK_DEFAULT,
        Long.toString(1 * Constants.GB));
    schedulerConfig.properties.setProperty(RoundRobinPacking.STMGR_RAM_DEFAULT,
        Long.toString(1 * Constants.GB));
    return schedulerConfig;
  }

  @Test
  public void testContainerCpuWhenConfigIsSet() throws Exception {
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
    AuroraLauncher launcher = AuroraLauncher.class.newInstance();
    LaunchContext context = new LaunchContext(config, topology);
    launcher.initialize(context);
    packing.context = context;
    packing.topology = topology;
    Assert.assertEquals(
        cpuRequested, packing.getContainerCpuHint(packing.getBasePacking()), 0.1);
  }

  @Test
  public void testContainerCpuWhenConfigIsNotSet() throws Exception {
    int componentParallelism = 2;
    DefaultConfigLoader config = createRequiredConfig();
    RoundRobinPacking packing = new RoundRobinPacking();
    Config topologyConfig = new Config();
    topologyConfig.setNumStmgrs(2);
    Map<String, Integer> spouts = new HashMap<>();
    spouts.put("spout", componentParallelism);
    Map<String, Integer> bolts = new HashMap<>();
    bolts.put("bolt", componentParallelism);
    TopologyAPI.Topology topology = TopologyUtilityTest.createTopology(
        "test", topologyConfig, spouts, bolts);
    AuroraLauncher launcher = AuroraLauncher.class.newInstance();
    LaunchContext context = new LaunchContext(config, topology);
    launcher.initialize(context);
    float expectedCpu = 3;  // 1 + (1 for spout and 1 for bolt). default is 1.
    packing.context = context;
    packing.topology = topology;
    Assert.assertEquals(
        expectedCpu, packing.getContainerCpuHint(packing.getBasePacking()), 0.1);
  }

  @Test
  public void testContainerDisk() throws Exception {
    int componentParallelism = 2;
    DefaultConfigLoader config = createRequiredConfig();
    RoundRobinPacking packing = new RoundRobinPacking();
    Config topologyConfig = new Config();
    topologyConfig.setNumStmgrs(3);
    Map<String, Integer> spouts = new HashMap<>();
    spouts.put("spout", componentParallelism);
    Map<String, Integer> bolts = new HashMap<>();
    bolts.put("bolt", componentParallelism);
    TopologyAPI.Topology topology = TopologyUtilityTest.createTopology(
        "test", topologyConfig, spouts, bolts);
    AuroraLauncher launcher = AuroraLauncher.class.newInstance();
    LaunchContext context = new LaunchContext(config, topology);
    launcher.initialize(context);
    // largest component has 2 instance. We add 2 for stmgr and metricsmgr and
    // request 10GB extra for Core dumps and logs
    long expectedDisk = (2 + 2) * Constants.GB + 10 * Constants.GB;
    packing.context = context;
    packing.topology = topology;
    Assert.assertEquals(
        expectedDisk, packing.getContainerDiskHint(packing.getBasePacking()));
  }

  @Test
  public void testRamDefaultForNoHint() throws Exception {
    int componentParallelism = 2;
    Config topologyConfig = new Config();
    topologyConfig.setNumStmgrs(3);
    Map<String, Integer> spouts = new HashMap<>();
    spouts.put("spout", componentParallelism);
    Map<String, Integer> bolts = new HashMap<>();
    bolts.put("bolt", componentParallelism);
    TopologyAPI.Topology topology = TopologyUtilityTest.createTopology(
        "test", topologyConfig, spouts, bolts);
    DefaultConfigLoader config = createRequiredConfig();
    RoundRobinPacking packing = new RoundRobinPacking();

    AuroraLauncher launcher = AuroraLauncher.class.newInstance();
    LaunchContext context = new LaunchContext(config, topology);
    packing.context = context;
    packing.topology = topology;
    launcher.initialize(context);
    // Verify that instance default is 1GB as configured in schedulerConfig
    Assert.assertEquals(1 * Constants.GB, packing.getDefaultInstanceRam(packing.getBasePacking()));
    // Verify total container ram is 2GB for instances and 1 GB for stmgr
    Assert.assertEquals(
        1 * Constants.GB + 2 * Constants.GB, packing.getContainerRamHint(packing.getBasePacking()));
  }

  @Test
  public void testRamDefaultForContainerRamHintGiven() throws Exception {
    int componentParallelism = 2;
    long containerRamRequested = 5 * Constants.GB;
    int numStmgr = 3;
    Config topologyConfig = new Config();
    topologyConfig.setNumStmgrs(numStmgr);
    topologyConfig.setContainerRamRequested(containerRamRequested);
    Map<String, Integer> spouts = new HashMap<>();
    spouts.put("spout", componentParallelism);
    Map<String, Integer> bolts = new HashMap<>();
    bolts.put("bolt", componentParallelism);
    TopologyAPI.Topology topology = TopologyUtilityTest.createTopology(
        "test", topologyConfig, spouts, bolts);
    AuroraLauncher launcher = AuroraLauncher.class.newInstance();
    DefaultConfigLoader config = createRequiredConfig();
    RoundRobinPacking packing = new RoundRobinPacking();
    LaunchContext context = new LaunchContext(config, topology);
    packing.context = context;
    packing.topology = topology;
    launcher.initialize(context);
    // Verify that each instance get equal ram.
    long expectedRam = 2 * Constants.GB;  // Most loaded container has 1 stmgr 1 spout & 1 bolt
    Assert.assertEquals(
        expectedRam, packing.getDefaultInstanceRam(packing.getBasePacking()));
    // Verify total container Ram takes precedance.
    Assert.assertEquals(
        containerRamRequested, packing.getContainerRamHint(packing.getBasePacking()));
  }

  @Test
  public void testRamDefaultForSomeComponentRamHintGiven() throws Exception {
    int componentParallelism = 2;
    long containerRamRequested = 5 * Constants.GB;
    int numStmgr = 3;
    Config topologyConfig = new Config();
    topologyConfig.setNumStmgrs(numStmgr);
    topologyConfig.setContainerRamRequested(containerRamRequested);
    topologyConfig.setComponentRam("spout", 1 * Constants.GB);
    Map<String, Integer> spouts = new HashMap<>();
    spouts.put("spout", componentParallelism);
    Map<String, Integer> bolts = new HashMap<>();
    bolts.put("bolt", componentParallelism);
    TopologyAPI.Topology topology = TopologyUtilityTest.createTopology(
        "test", topologyConfig, spouts, bolts);
    AuroraLauncher launcher = AuroraLauncher.class.newInstance();
    DefaultConfigLoader config = createRequiredConfig();
    RoundRobinPacking packing = new RoundRobinPacking();
    LaunchContext context = new LaunchContext(config, topology);
    packing.context = context;
    packing.topology = topology;
    launcher.initialize(context);
    // Verify that most loaded component will have spout takes 1 GB of ram and bolts get all
    // remaining ram. i.e 3 * GB.
    long expectedRam = 3 * Constants.GB;  // 5GB for remaining bolts
    Assert.assertEquals(
        expectedRam, packing.getDefaultInstanceRam(packing.getBasePacking()));
  }
}
