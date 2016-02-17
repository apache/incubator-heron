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
 * Loads config file in Yaml file format.
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

  public static Integer getInt(Object o) {
    if (o instanceof Long) {
      return ((Long) o).intValue();
    } else if (o instanceof Integer) {
      return (Integer) o;
    } else if (o instanceof Short) {
      return ((Short) o).intValue();
    } else {
      try {
        return Integer.parseInt(o.toString());
      } catch (NumberFormatException nfe) {
        throw new IllegalArgumentException("Don't know how to convert " + o + " + to int");
      }
    }
  }

  public static Long getLong(Object o) {
    if (o instanceof Long) {
      return (Long) o;
    } else if (o instanceof Integer) {
      return ((Integer) o).longValue();
    } else if (o instanceof Short) {
      return ((Short) o).longValue();
    } else {
      try {
        return Long.parseLong(o.toString());
      } catch (NumberFormatException nfe) {
        throw new IllegalArgumentException("Don't know how to convert " + o + " + to long");
      }
    }
  }
}
