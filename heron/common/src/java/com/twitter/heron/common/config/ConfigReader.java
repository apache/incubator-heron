package com.twitter.heron.common.config;

import java.util.Properties;
import java.util.logging.Logger;
import java.util.logging.Level;
import java.util.HashMap;
import java.util.Map;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import org.yaml.snakeyaml.Yaml;

/**
 * Loads config file in Java properties file format.
 */
public class ConfigReader {
  private static final Logger LOG = Logger.getLogger(ConfigReader.class.getName());

  /**
   * Load properties from the given YAML file
   *
   * @return <code>true</code> only if the operation succeeds.
   */
  public static Map loadFile(String propFileName) {
    Map props = new HashMap();
    if (propFileName == null || propFileName.isEmpty()) {
      LOG.warning("Config file " + propFileName + " not found\n");
       return props;
    } else {
      try {
        FileInputStream fin = new FileInputStream(new File(propFileName));
        Yaml yaml = new Yaml();
        props = (Map) yaml.load(fin);
      } catch (IOException e) {
        LOG.log(Level.SEVERE, "Failed to load config file: " + propFileName, e); 
      }
    }
    return props;
  }
}
