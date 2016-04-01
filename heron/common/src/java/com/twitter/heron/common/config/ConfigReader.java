package com.twitter.heron.common.config;

import java.util.Properties;
import java.util.logging.Logger;
import java.util.logging.Level;
import java.util.HashMap;
import java.util.Map;

import java.io.File;
import java.io.InputStream;
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
   * @param fileName, the name of YAML file to read
   *
   * @return Map, contains the key value pairs of config
   */
  public static Map loadFile(String fileName) {
    Map props = new HashMap();
    if (fileName == null) {
      LOG.warning("Config file name cannot be null\n");
      return props;
    }
    else if (fileName.isEmpty()) {
      LOG.warning("Config file name is empty\n");
      return props;
    } else {

      // check if the file exists and also it is a regular file
      Path path = Paths.get(fileName);

      if (!Files.exists(path)) {
        LOG.warning("Config file " + fileName + " does not exist.\n");
        return props;
      }

      if (!Files.isRegularFile(path)) {
        LOG.warning("Config file " + fileName + " might be a directory.\n");
        return props;
      }

      LOG.fine("Reading config file " + fileName);

      Map props_yaml = null;
      try {
        FileInputStream fin = new FileInputStream(new File(fileName));
        Yaml yaml = new Yaml();
        props_yaml = (Map) yaml.load(fin);
        LOG.fine("Successfully read config file " + fileName);
      } catch (IOException e) {
        LOG.log(Level.SEVERE, "Failed to load config file: " + fileName, e);
      }

      return props_yaml != null ? props_yaml : props;
    }
  }

  /**
   * Load config from the given YAML stream
   *
   * @param inputStream, the name of YAML stream to read
   *
   * @return Map, contains the key value pairs of config
   */
  public static Map loadStream(InputStream inputStream) {
    LOG.fine("Reading config stream");

    Map props_yaml = null;
    Yaml yaml = new Yaml();
    props_yaml = (Map) yaml.load(inputStream);
    LOG.fine("Successfully read config");

    return props_yaml != null ? props_yaml : new HashMap();
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
