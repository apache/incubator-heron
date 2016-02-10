package com.twitter.heron.common.config;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Paths;

import java.util.Map;
import java.util.HashMap;
import java.util.logging.Logger;
import java.util.logging.Level;

/**
 * Loads config file in Java properties file format.
 */
public final class HeronConfigReader extends ConfigReader {
  private static final Logger LOG = Logger.getLogger(ConfigReader.class.getName());

  private final Map config = new HashMap<String, Object>();

  public boolean load(String cluster, String configPath, String fileName) {
    // First, read the defaults file
    String file = Paths.get(configPath, fileName).toString();
    Map props1 = loadFile(file);

    // Read the cluster specific file and override
    file = Paths.get(configPath, cluster, fileName).toString();
    Map props2 = loadFile(file);

    // If both files have errors, return false
    if (props1.isEmpty() && props2.isEmpty())
      return false;

    // Save the properties and return successfully
    config.putAll(props1);
    config.putAll(props2);
    return true;
  }

  public Map getConfig() {
    return config;
  }
}
