package com.twitter.heron.statemgr.localfs;

import java.util.logging.Logger;
import java.util.logging.Level;
import java.util.Map;

import java.lang.ClassNotFoundException;
import com.twitter.heron.spi.common.Resource;

public class LocalFileSystemConfigKeys {
  private static final Logger LOG = Logger.getLogger(LocalFileSystemConfigKeys.class.getName());

  // holds the mapping of keys to their corresponding key strings
  private static Map keys;

  // load the resource for config keys
  static {
    try {
      keys = Resource.load(
          "com.twitter.heron.statemgr.localfs.LocalFileSystemKeys",
          LocalFileSystemConstants.KEYS_YAML);
    }
    catch (ClassNotFoundException e) {
      LOG.severe("Unable to load the local file system uploader keys class " + e);
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
