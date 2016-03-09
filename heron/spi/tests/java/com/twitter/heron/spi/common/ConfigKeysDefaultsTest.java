package com.twitter.heron.spi.common;

import java.nio.file.Paths;
import java.util.Properties;
import java.util.logging.Logger;
import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ConfigKeysDefaultsTest {
  private static final Logger LOG = Logger.getLogger(ConfigKeysDefaultsTest.class.getName());

  /*
   * Test to check if all the default keys have a corresponding config key
   */
  @Test
  public void testConfigKeysDefaults() throws Exception {
    Map<String, String> keys = ConfigKeys.keys;
    Map<String, Object> defaults = ConfigDefaults.defaults;

    for (Map.Entry<String, Object> entry : defaults.entrySet()) {
      String key = entry.getKey();
      System.out.println(key);
      Assert.assertTrue(keys.containsKey(key));
    }
  }
}
