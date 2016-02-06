package com.twitter.heron.common.config;

import java.nio.file.Paths;
import java.util.Properties;
import java.util.logging.Logger;
import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

public class ConfigReaderTest {
  private static final Logger LOG = Logger.getLogger(ConfigReaderTest.class.getName());

  @Test
  public void testLoadFile() throws Exception {
    ConfigReader configReader = ConfigReader.class.newInstance();

    String file = Paths.get(System.getenv("JAVA_RUNFILES"), Constants.TEST_DATA_PATH, "defaults.yaml").toString();
    Map props = configReader.loadFile(file);

    Assert.assertEquals("role", props.get(Constants.ROLE_KEY));
    Assert.assertEquals("environ", props.get(Constants.ENVIRON_KEY));
    Assert.assertEquals("com.twitter.heron.scheduler.aurora.AuroraLauncher", props.get(Constants.LAUNCHER_CLASS_KEY)); 

    Assert.assertNull(props.get(Constants.USER_KEY));
  }
}
