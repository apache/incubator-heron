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
  Config sandbox;
  Config props;

  @Before
  public void initialize() {
    home = ClusterDefaults.getDefaultHome();
    sandbox = ClusterDefaults.getSandboxHome();
    props = Config.newBuilder()
        .putAll(ClusterDefaults.getDefaultJars())
        .putAll(ClusterDefaults.getSandboxBinaries())
        .build();
  }

  @Test
  public void testSandboxBinaries() throws Exception {

    Assert.assertEquals(
        Defaults.executorSandboxBinary(),
        Context.executorSandboxBinary(props)
    );

    Assert.assertEquals(
        Defaults.stmgrSandboxBinary(),
        Context.stmgrSandboxBinary(props)
    );

    Assert.assertEquals(
        Defaults.tmasterSandboxBinary(),
        Context.tmasterSandboxBinary(props)
    );

    Assert.assertEquals(
        Defaults.shellSandboxBinary(),
        Context.shellSandboxBinary(props)
    );
  }

  @Test
  public void testDefaultJars() throws Exception {
    Assert.assertEquals(
        Defaults.schedulerJar(),
        Context.schedulerJar(props)
    );
  }

  @Test
  public void testDefaultResources() throws Exception {
    Config props = ClusterDefaults.getDefaultResources();

    Assert.assertEquals(
        Defaults.stmgrRam(),
        Context.stmgrRam(props)
    );

    Assert.assertEquals(
        Defaults.instanceCpu(),
        Context.instanceCpu(props),
        0.001
    );

    Assert.assertEquals(
        Defaults.instanceRam(),
        Context.instanceRam(props)
    );

    Assert.assertEquals(
        Defaults.instanceDisk(),
        Context.instanceDisk(props)
    );
  }
}
