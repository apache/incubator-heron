package com.twitter.heron.common.config;

import java.nio.file.Paths;
import java.util.Properties;
import java.util.logging.Logger;
import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

public class HeronConfigReaderTest {
  private static final Logger LOG = Logger.getLogger(HeronConfigReaderTest.class.getName());

  @Test
  public void testLoadClusterAndDefaultFiles() throws Exception {
    HeronConfigReader configReader = HeronConfigReader.class.newInstance();

    String configPath = Paths.get(System.getenv("JAVA_RUNFILES"), Constants.TEST_DATA_PATH).toString();
    Assert.assertTrue(configReader.load("cluster", configPath, "defaults.yaml"));

    Map props = configReader.getConfig();
    Assert.assertEquals("role1", props.get(Constants.ROLE_KEY));
    Assert.assertEquals("environ", props.get(Constants.ENVIRON_KEY));
    Assert.assertEquals("user1", props.get(Constants.USER_KEY));
    Assert.assertEquals("com.twitter.heron.scheduler.local.LocalLauncher", props.get(Constants.LAUNCHER_CLASS_KEY)); 

    Assert.assertNull(props.get(Constants.VERSION_KEY));
  }

  @Test
  public void testLoadDefaultFile() throws Exception {
    HeronConfigReader configReader = HeronConfigReader.class.newInstance();

    String configPath = Paths.get(System.getenv("JAVA_RUNFILES"), Constants.TEST_DATA_PATH).toString();
    Assert.assertTrue(configReader.load("cluster", configPath, "defaults1.yaml"));

    Map props = configReader.getConfig();
    Assert.assertEquals("role", props.get(Constants.ROLE_KEY));
    Assert.assertEquals("environ", props.get(Constants.ENVIRON_KEY));
    Assert.assertEquals("group", props.get(Constants.GROUP_KEY));
    Assert.assertEquals("com.twitter.heron.scheduler.aurora.AuroraLauncher", props.get(Constants.LAUNCHER_CLASS_KEY)); 

    Assert.assertNull(props.get(Constants.USER_KEY));
    Assert.assertNull(props.get(Constants.VERSION_KEY));
  }

  @Test
  public void testLoadClusterFile() throws Exception {
    HeronConfigReader configReader = HeronConfigReader.class.newInstance();

    String configPath = Paths.get(System.getenv("JAVA_RUNFILES"), Constants.TEST_DATA_PATH).toString();
    Assert.assertTrue(configReader.load("cluster", configPath, "defaults2.yaml"));

    Map props = configReader.getConfig();
    Assert.assertEquals("role1", props.get(Constants.ROLE_KEY));
    Assert.assertEquals("10.1", props.get(Constants.VERSION_KEY).toString());
    Assert.assertEquals("com.twitter.heron.scheduler.local.LocalLauncher", props.get(Constants.LAUNCHER_CLASS_KEY)); 

    Assert.assertNull(props.get(Constants.ENVIRON_KEY));
    Assert.assertNull(props.get(Constants.GROUP_KEY));
  }
}
