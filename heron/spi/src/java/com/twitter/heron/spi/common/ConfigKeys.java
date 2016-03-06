package com.twitter.heron.spi.common;

import java.util.logging.Logger;
import java.util.logging.Level;
import java.util.HashMap;
import java.util.Map;

import java.lang.ClassNotFoundException;

import java.io.InputStream;
import java.nio.file.Paths;

public class ConfigKeys {
  private static final Logger LOG = Logger.getLogger(ConfigKeys.class.getName());

  // holds the mapping of keys to their corresponding key strings
  protected static Map keys;

   // load the resource for config keys
  static {
    try {
      keys = Resource.load(
          "com.twitter.heron.spi.common.Keys", Constants.KEYS_YAML);
    }
    catch (ClassNotFoundException e) {
      LOG.severe("Unable to load the config Keys class " + e);
      System.exit(1);
    }
  }

  /*
   * Get the key string value for the given key
   *
   * @param key, the key
   * @return String, the key string value for the key
   */
  public static String get(String key) {
    return (String) keys.get(key); 
  }
}
