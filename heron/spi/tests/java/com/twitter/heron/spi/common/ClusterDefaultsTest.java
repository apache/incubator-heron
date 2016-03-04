package com.twitter.heron.spi.common;

import java.nio.file.Paths;
import java.util.Properties;
import java.util.logging.Logger;
import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ClusterDefaultsTest {
  private static final Logger LOG = Logger.getLogger(ClusterDefaultsTest.class.getName());

  Config home;
  Config props;

  @Before
  public void initialize() {
    home = ClusterDefaults.getDefaultHome();
    props = Config.newBuilder()
        .putAll(ClusterDefaults.getDefaultBinaries())
        .putAll(ClusterDefaults.getDefaultJars())
        .build();
  }

  @Test
  public void testDefaultBinaries() throws Exception {

    Assert.assertEquals(
        Defaults.get("EXECUTOR_BINARY"),
        Context.executorBinary(props)
    );

    Assert.assertEquals(
        Defaults.get("STMGR_BINARY"),
        Context.stmgrBinary(props)
    );

    Assert.assertEquals(
        Defaults.get("TMASTER_BINARY"),
        Context.tmasterBinary(props)
    );

    Assert.assertEquals(
        Defaults.get("SHELL_BINARY"),
        Context.shellBinary(props)
    );
  }

  @Test
  public void testDefaultJars() throws Exception {
    Assert.assertEquals(
        Defaults.get("SCHEDULER_JAR"),
        Context.schedulerJar(props)
    );
  }

  @Test
  public void testDefaultResources() throws Exception {
    Config props = ClusterDefaults.getDefaultResources();

    Assert.assertEquals(
        Defaults.getLong("STMGR_RAM"),
        Context.stmgrRam(props)
    );

    Assert.assertEquals(
        Defaults.getLong("INSTANCE_CPU"),
        Context.instanceCpu(props),
        0.001
    );

    Assert.assertEquals(
        Defaults.getLong("INSTANCE_RAM"),
        Context.instanceRam(props)
    );

    Assert.assertEquals(
        Defaults.getLong("INSTANCE_DISK"),
        Context.instanceDisk(props)
    );
  }
}
