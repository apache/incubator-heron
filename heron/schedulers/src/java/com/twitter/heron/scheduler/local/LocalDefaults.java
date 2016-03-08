package com.twitter.heron.scheduler.local;

import java.util.logging.Logger;
import java.util.logging.Level;

import java.util.HashMap;
import java.util.Map;

import java.lang.ClassNotFoundException;
import java.io.InputStream;
import java.nio.file.Paths;

import com.twitter.heron.spi.common.Resource;

public class LocalDefaults {
  private static final Logger LOG = Logger.getLogger(LocalDefaults.class.getName());

  // holds the mapping between the config keys and their default values
  private static Map defaults;

  // load the resource for default config key values
  static {
    try {
      defaults = Resource.load(
          "com.twitter.heron.scheduler.local.LocalDefaults",
          LocalConstants.DEFAULTS_YAML);
    }
    catch (ClassNotFoundException e) {
      LOG.severe("Unable to load the Defaults class " + e);
      System.exit(1);
    }
  }

  /*
   * Get the default value for the given config key
   *
   * @param key, the config key
   * @return String, the default value for the config key
   */
  public static String get(String key) {
    return (String) defaults.get(key);
  }
}
