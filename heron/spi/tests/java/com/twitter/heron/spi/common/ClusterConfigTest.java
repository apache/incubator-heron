package com.twitter.heron.spi.common;

import java.nio.file.Paths;
import java.util.Properties;
import java.util.logging.Logger;
import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

public class ClusterConfigTest {
  private static final Logger LOG = Logger.getLogger(ClusterConfigTest.class.getName());

  @Test
  public void testClusterFile() throws Exception {
    String configPath = Paths.get(System.getenv("JAVA_RUNFILES"), TestConstants.TEST_DATA_PATH).toString();
    Config props = ClusterConfig.loadClusterConfig("local", configPath);

    Assert.assertEquals(4, props.size());

    Assert.assertEquals(
        "com.twitter.heron.uploader.localfs.FileSystemUploader", 
        Context.uploaderClass(props)
    );

    Assert.assertEquals(
        "com.twitter.heron.scheduler.local.LocalScheduler", 
        Context.schedulerClass(props)
    );

    Assert.assertEquals(
        "com.twitter.heron.packing.roundrobin.RoundRobinPacking", 
        Context.packingClass(props)
    );

    Assert.assertEquals(
        "com.twitter.heron.state.localfs.LocalFileStateManager", 
        Context.stateManagerClass(props)
    );
  }

  @Test
  public void testDefaultsFile() throws Exception {
    String configPath = Paths.get(System.getenv("JAVA_RUNFILES"), TestConstants.TEST_DATA_PATH).toString();
    Config props = ClusterConfig.loadDefaultsConfig("local", configPath);

    Assert.assertEquals(10, props.size());

    Assert.assertEquals(
        "heron-executor", 
        Context.executorBinary(props)
    );

    Assert.assertEquals(
        "heron-stmgr", 
        Context.stmgrBinary(props)
    );

    Assert.assertEquals(
        "heron-tmaster",
        Context.tmasterBinary(props)
    );

    Assert.assertEquals(
        "heron-shell",
        Context.shellBinary(props)
    );

    Assert.assertEquals(
        "heron-scheduler.jar",
        Context.schedulerJar(props)
    );

    Assert.assertEquals(
        Double.valueOf(1),
        Context.instanceCpu(props),
        0.001
    );

    Assert.assertEquals(
        Long.valueOf(128 * Constants.MB),
        Context.instanceRam(props)
    );

    Assert.assertEquals(
        Long.valueOf(256 * Constants.MB),
        Context.instanceDisk(props)
    );

    Assert.assertEquals(
        Long.valueOf(512 * Constants.MB),
        Context.stmgrRam(props)
    );
  }

  @Test
  public void testSchedulerFile() throws Exception {
    String configPath = Paths.get(System.getenv("JAVA_RUNFILES"), TestConstants.TEST_DATA_PATH).toString();
    Config props = ClusterConfig.loadSchedulerConfig("local", configPath);

    Assert.assertEquals(2, props.size());

    Assert.assertEquals(
        "com.twitter.heron.scheduler.local.LocalLauncher",
        Context.launcherClass(props)
    );

    Assert.assertEquals(
        "com.twitter.heron.scheduler.local.LocalRuntimeManager",
        Context.runtimeManagerClass(props)
    );
  }

  @Test
  public void testPackingFile() throws Exception {
    String configPath = Paths.get(System.getenv("JAVA_RUNFILES"), TestConstants.TEST_DATA_PATH).toString();
    Config props = ClusterConfig.loadPackingConfig("local", configPath);

    Assert.assertEquals(0, props.size());
  }

  @Test
  public void testUploaderFile() throws Exception {
    String configPath = Paths.get(System.getenv("JAVA_RUNFILES"), TestConstants.TEST_DATA_PATH).toString();
    Config props = ClusterConfig.loadUploaderConfig("local", configPath);

    Assert.assertEquals(1, props.size());
    Assert.assertEquals(
        "/vagrant/heron/jobs",
        props.getStringValue("heron.uploader.file.system.path")
    );
  }
}
