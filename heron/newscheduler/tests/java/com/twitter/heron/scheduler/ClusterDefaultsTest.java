package com.twitter.heron.scheduler;

import java.nio.file.Paths;
import java.util.Properties;
import java.util.logging.Logger;
import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import com.twitter.heron.spi.common.Keys;
import com.twitter.heron.spi.common.Defaults;
import com.twitter.heron.spi.common.Context;

public class ClusterDefaultsTest {
  private static final Logger LOG = Logger.getLogger(ClusterDefaultsTest.class.getName());

  @Test
  public void testDefaultBinaries() throws Exception {
    Context props = ClusterDefaults.getDefaultBinaries();

    Assert.assertEquals(
        Defaults.Config.EXECUTOR_BINARY,
        props.getStringValue(Keys.Config.EXECUTOR_BINARY)
    );

    Assert.assertEquals(
        Defaults.Config.STMGR_BINARY,
        props.getStringValue(Keys.Config.STMGR_BINARY)
    );

    Assert.assertEquals(
        Defaults.Config.TMASTER_BINARY,
        props.getStringValue(Keys.Config.TMASTER_BINARY)
    );

    Assert.assertEquals(
        Defaults.Config.SHELL_BINARY,
        props.getStringValue(Keys.Config.SHELL_BINARY)
    );
  }

  @Test
  public void testDefaultJars() throws Exception {
    Context props = ClusterDefaults.getDefaultJars();

    Assert.assertEquals(
        Defaults.Config.SCHEDULER_JAR,
        props.getStringValue(Keys.Config.SCHEDULER_JAR)
    );
  }

  @Test
  public void testDefaultResources() throws Exception {
    Context props = ClusterDefaults.getDefaultResources();

    Assert.assertEquals(
        Defaults.Config.STMGR_RAM,
        (long)props.getLongValue(Keys.Config.STMGR_RAM)
    );

    Assert.assertEquals(
        Defaults.Config.INSTANCE_CPU,
        (long)props.getLongValue(Keys.Config.INSTANCE_CPU)
    );

    Assert.assertEquals(
        Defaults.Config.INSTANCE_RAM,
        (long)props.getLongValue(Keys.Config.INSTANCE_RAM)
    );

    Assert.assertEquals(
        Defaults.Config.INSTANCE_DISK,
        (long)props.getLongValue(Keys.Config.INSTANCE_DISK)
    );
  }
}
