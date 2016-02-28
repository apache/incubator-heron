package com.twitter.heron.common.config;

import java.util.Properties;
import java.util.logging.Logger;
import java.util.logging.Level;
import java.util.HashMap;
import java.util.Map;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.Files;

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
    if (propFileName == null) {
      LOG.warning("Config file name cannot be null\n"); 
      return props;
    }
    else if (propFileName.isEmpty()) {
      LOG.warning("Config file name is empty\n");
      return props;
    } else {

      // check if the file exists and also it is a regular file
      Path path = Paths.get(propFileName);

      if (!Files.exists(path)) {
        LOG.warning("Config file " + propFileName + " does not exist.\n");
        return props;
      }
     
      if (!Files.isRegularFile(path)) {
        LOG.warning("Config file " + propFileName + " might be a directory.\n");
        return props;
      }
      
      LOG.info("Reading config file " + propFileName);

      Map props_yaml = null;
      try {
        FileInputStream fin = new FileInputStream(new File(propFileName));
        Yaml yaml = new Yaml();
        props_yaml = (Map) yaml.load(fin);
        LOG.info("Successfully read config file " + propFileName);
      } catch (IOException e) {
        LOG.log(Level.SEVERE, "Failed to load config file: " + propFileName, e); 
      }

      return props_yaml != null ? props_yaml : props;
    }
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
