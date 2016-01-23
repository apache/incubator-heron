package com.twitter.heron.scheduler.util;

import java.util.Properties;
import java.util.logging.Logger;

/**
 * Loads config file in Java properties file format.
 */
public final class PropertiesFileConfigLoader extends AbstractPropertiesConfigLoader {
  private static final Logger LOG = Logger.getLogger(PropertiesFileConfigLoader.class.getName());

  @Override
  public boolean load(String propertiesFile, String propertyOverride) {
    Properties workingCopy = new Properties();

    if (ConfigLoaderUtils.loadPropertiesFile(workingCopy, propertiesFile) &&
        ConfigLoaderUtils.applyPropertyOverride(workingCopy, propertyOverride) &&
        ConfigLoaderUtils.applyConfigPropertyOverride(workingCopy)) {
      properties.putAll(workingCopy);

      addDefaultProperties();

      if (isVerbose()) {
        LOG.info("Config properties parsed: \n" + properties);
      }

      return true;
    }

    return false;
  }
}
