package com.twitter.heron.spi.common;

import java.nio.file.Paths;
import java.util.Properties;
import java.util.logging.Logger;
import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

public class ClusterDefaultsTest {
  private static final Logger LOG = Logger.getLogger(ClusterDefaultsTest.class.getName());

  @Test
  public void testDefaultBinaries() throws Exception {
    Config home = ClusterDefaults.getDefaultHome();
    Config props = ClusterDefaults.getDefaultBinaries();

    Assert.assertEquals(
        Misc.substitute(home, Defaults.EXECUTOR_BINARY),
        Context.executorBinary(props)
    );

    Assert.assertEquals(
        Misc.substitute(home, Defaults.STMGR_BINARY),
        Context.stmgrBinary(props)
    );

    Assert.assertEquals(
        Misc.substitute(home, Defaults.TMASTER_BINARY),
        Context.tmasterBinary(props)
    );

    Assert.assertEquals(
        Misc.substitute(home, Defaults.SHELL_BINARY),
        Context.shellBinary(props)
    );
  }

  @Test
  public void testDefaultJars() throws Exception {
    Config home = ClusterDefaults.getDefaultHome();
    Config props = ClusterDefaults.getDefaultJars();

    Assert.assertEquals(
        Misc.substitute(home, Defaults.SCHEDULER_JAR),
        Context.schedulerJar(props)
    );
  }

  @Test
  public void testDefaultResources() throws Exception {
    Config props = ClusterDefaults.getDefaultResources();

    Assert.assertEquals(
        Long.valueOf(Defaults.STMGR_RAM),
        Context.stmgrRam(props)
    );

    Assert.assertEquals(
        Double.valueOf(Defaults.INSTANCE_CPU),
        Context.instanceCpu(props),
        0.001
    );

    Assert.assertEquals(
        Long.valueOf(Defaults.INSTANCE_RAM),
        Context.instanceRam(props)
    );

    Assert.assertEquals(
        Long.valueOf(Defaults.INSTANCE_DISK),
        Context.instanceDisk(props)
    );
  }
}
