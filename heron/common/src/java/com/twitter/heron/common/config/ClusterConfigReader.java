package com.twitter.heron.common.config;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Paths;

import java.util.Map;
import java.util.HashMap;
import java.util.logging.Logger;
import java.util.logging.Level;

/**
 * Loads config file in Yaml properties file format based 
 * on cluster and config path
 *
 */
public final class ClusterConfigReader extends ConfigReader {
  private static final Logger LOG = Logger.getLogger(ClusterConfigReader.class.getName());

  public static Map load(String cluster, String configPath, String fileName) {
     Map config = new HashMap<String, Object>();

    // Read the defaults file, first
    String file1 = Paths.get(configPath, fileName).toString();
    Map props1 = loadFile(file1);
    if (props1.isEmpty()) {
      LOG.info("Config file " + file1 + " is empty");
    }

    // Read the cluster specific file and override
    String file2 = Paths.get(configPath, cluster, fileName).toString();
    Map props2 = loadFile(file2);
    if (props2.isEmpty()) {
      LOG.info("Config file " + file2 + " is empty");
    }

    // If both files have errors, return false
    if (props1.isEmpty() && props2.isEmpty()) {
      LOG.info("Files " + file1 + " and " + file2 + " are not present");
      return config;
    }

    // Save the properties and return successfully
    config.putAll(props1);
    config.putAll(props2);
    return config;
  }
}
