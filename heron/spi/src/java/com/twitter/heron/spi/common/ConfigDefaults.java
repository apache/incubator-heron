package com.twitter.heron.spi.common;

import java.util.logging.Logger;
import java.util.logging.Level;

import java.util.HashMap;
import java.util.Map;

import java.lang.ClassNotFoundException;
import java.io.InputStream;
import java.nio.file.Paths;

public class ConfigDefaults {
  private static final Logger LOG = Logger.getLogger(ConfigDefaults.class.getName());

  // holds the mapping between the config keys and their default values
  protected static Map defaults; 

  // load the resource for default config key values
  static {
    try {
      defaults = Resource.load(
          "com.twitter.heron.spi.common.Defaults", 
          Constants.DEFAULTS_YAML);
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

  public static Long getLong(String key) {
    return Convert.getLong(defaults.get(key));
  }

  public static Double getDouble(String key) {
    return Convert.getDouble(defaults.get(key));
  }
}
