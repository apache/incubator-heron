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
    Context props = ClusterConfig.loadClusterConfig("local", configPath);

    Assert.assertEquals(4, props.size());

    Assert.assertEquals(
        "com.twitter.heron.uploader.localfs.FileSystemUploader", 
        props.getStringValue(Keys.UPLOADER_CLASS)
    );

    Assert.assertEquals(
        "com.twitter.heron.scheduler.local.LocalScheduler", 
        props.getStringValue(Keys.SCHEDULER_CLASS)
    );

    Assert.assertEquals(
        "com.twitter.heron.packing.roundrobin.RoundRobinPacking", 
        props.getStringValue(Keys.PACKING_CLASS)
    );

    Assert.assertEquals(
        "com.twitter.heron.state.localfs.LocalFileStateManager", 
        props.getStringValue(Keys.STATE_MANAGER_CLASS)
    );
  }

  @Test
  public void testDefaultsFile() throws Exception {
    String configPath = Paths.get(System.getenv("JAVA_RUNFILES"), TestConstants.TEST_DATA_PATH).toString();
    Context props = ClusterConfig.loadDefaultsConfig("local", configPath);

    Assert.assertEquals(10, props.size());

    Assert.assertEquals(
        "heron-executor", 
        props.getStringValue(Keys.EXECUTOR_BINARY)
    );

    Assert.assertEquals(
        "heron-stmgr", 
        props.getStringValue(Keys.STMGR_BINARY)
    );

    Assert.assertEquals(
        "heron-tmaster",
        props.getStringValue(Keys.TMASTER_BINARY)
    );

    Assert.assertEquals(
        "heron-shell",
        props.getStringValue(Keys.SHELL_BINARY)
    );

    Assert.assertEquals(
        "heron-scheduler.jar",
        props.getStringValue(Keys.SCHEDULER_JAR)
    );

    Assert.assertEquals(
        "heron-scheduler.jar",
        props.getStringValue(Keys.SCHEDULER_JAR)
    );

    Assert.assertEquals(
        Long.valueOf(1),
        props.getLongValue(Keys.INSTANCE_CPU)
    );

    Assert.assertEquals(
        Long.valueOf(128 * Constants.MB),
        props.getLongValue(Keys.INSTANCE_RAM)
    );

    Assert.assertEquals(
        Long.valueOf(256 * Constants.MB),
        props.getLongValue(Keys.INSTANCE_DISK)
    );

    Assert.assertEquals(
        Long.valueOf(512 * Constants.MB),
        props.getLongValue(Keys.STMGR_RAM)
    );
  }

  @Test
  public void testSchedulerFile() throws Exception {
    String configPath = Paths.get(System.getenv("JAVA_RUNFILES"), TestConstants.TEST_DATA_PATH).toString();
    Context props = ClusterConfig.loadSchedulerConfig("local", configPath);

    Assert.assertEquals(2, props.size());

    Assert.assertEquals(
        "com.twitter.heron.scheduler.local.LocalLauncher",
        props.getStringValue(Keys.LAUNCHER_CLASS)
    );

    Assert.assertEquals(
        "com.twitter.heron.scheduler.local.LocalRuntimeManager",
        props.getStringValue(Keys.RUNTIME_MANAGER_CLASS)
    );
  }

  @Test
  public void testPackingFile() throws Exception {
    String configPath = Paths.get(System.getenv("JAVA_RUNFILES"), TestConstants.TEST_DATA_PATH).toString();
    Context props = ClusterConfig.loadPackingConfig("local", configPath);

    Assert.assertEquals(0, props.size());
  }

  @Test
  public void testUploaderFile() throws Exception {
    String configPath = Paths.get(System.getenv("JAVA_RUNFILES"), TestConstants.TEST_DATA_PATH).toString();
    Context props = ClusterConfig.loadUploaderConfig("local", configPath);

    Assert.assertEquals(1, props.size());
    Assert.assertEquals(
        "/vagrant/heron/jobs",
        props.getStringValue("heron.uploader.file.system.path")
    );
  }
}
