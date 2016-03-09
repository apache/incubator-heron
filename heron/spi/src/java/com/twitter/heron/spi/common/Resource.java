package com.twitter.heron.spi.common;

import java.util.logging.Logger;
import java.util.logging.Level;

import java.util.HashMap;
import java.util.Map;

import java.lang.ClassNotFoundException;
import java.io.InputStream;
import java.nio.file.Paths;

import com.twitter.heron.common.config.ConfigReader;

public class Resource {
  private static final Logger LOG = Logger.getLogger(Resource.class.getName());

  /*
   * Loads the YAML file specified as a resource
   *
   * @param className, the name of the class
   * @param resName, the name of the resource
   *
   * @return Map, a map of key value pairs
   */
  public static Map load(String className, String resName) throws ClassNotFoundException {
    // get the current class
    Class cls = Class.forName(className);

    // get the class loader for current class
    ClassLoader cLoader = cls.getClassLoader();

    // open the resource file and parse the yaml
    InputStream fileStream = cLoader.getResourceAsStream(resName);
    Map kvPairs = ConfigReader.loadStream(fileStream);

    // if nothing there exit, since this config is mandatory
    if (kvPairs.isEmpty()) {
      LOG.severe("Config keys cannot be empty ");
      System.exit(1);
    }
    
    return kvPairs;
  }
}
