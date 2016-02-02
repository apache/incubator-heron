package com.twitter.heron.scheduler.util;

import java.util.logging.Logger;

/**
 * Loads config file in Java properties file format.
 */
public class DefaultConfigLoader extends AbstractPropertiesConfigLoader {
  private static final Logger LOG = Logger.getLogger(DefaultConfigLoader.class.getName());

  public boolean load(String configFile, String configOverride) {
    PropertiesFileConfigLoader baseLoader = new PropertiesFileConfigLoader();
    String propertyOverride = preparePropertyOverride(configOverride);
    if (baseLoader.load(configFile, propertyOverride)) {
      properties.putAll(baseLoader.getProperties());
      return true;
    } else {
      return false;
    }
  }
}
