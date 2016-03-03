package com.twitter.heron.common.config;

import java.nio.file.Paths;
import java.util.Properties;
import java.util.logging.Logger;
import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

public class SystemConfigTest {
  private static final Logger LOG = Logger.getLogger(SystemConfigTest.class.getName());

  @Test
  public void testReadConfig() throws Exception {
    String file = Paths.get(System.getenv("JAVA_RUNFILES"), Constants.TEST_DATA_PATH, "sysconfig.yaml").toString();
    SystemConfig sysconfig = new SystemConfig(file);

    Assert.assertEquals("log-files", sysconfig.getHeronLoggingDirectory());
    Assert.assertEquals(100, sysconfig.getHeronLoggingMaximumSizeMb());
    Assert.assertEquals(5, sysconfig.getHeronLoggingMaximumFiles());
    Assert.assertEquals(60, sysconfig.getHeronMetricsExportIntervalSec());
  }
}
