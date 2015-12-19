package com.twitter.heron.scheduler.util;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.twitter.heron.scheduler.api.Constants;
import com.twitter.heron.scheduler.api.IConfigLoader;

/**
 * Loads config file in Java properties file format.
 */
public class DefaultConfigLoader implements IConfigLoader {
  private static final Logger LOG = Logger.getLogger(DefaultConfigLoader.class.getName());

  public Properties properties;

  public static String convertSpaceToEOL(String configOverride) {
    // Tokenize strings by spaces. Ignore spaces inside quotes. Ignore \" while parsing.
    StringBuilder token = new StringBuilder();
    ArrayList<String> tokens = new ArrayList<String>();
    boolean escaped = false;
    boolean inQuotes = false;
    for (char ch : configOverride.toCharArray()) {
      if (!escaped && ch == '\\') {
        escaped = true;
      } else if (escaped) {
        escaped = false;
        token.append(ch);
      } else {
        switch (ch) {
          // Token boundaries
          case '=':
          case ' ': {
            if (!inQuotes) {
              tokens.add(token.toString());
              token = new StringBuilder();
            } else {
              token.append(ch);
            }
            break;
          }
          // Non-boundaries
          case '\"':
            inQuotes = !inQuotes;
          default:
            token.append(ch);
        }
      }
    }
    tokens.add(token.toString());
    // Merge two consecutive non-empty pairs as key and value.
    boolean inKey = true;
    StringBuilder formattedString = new StringBuilder();
    for (String s : tokens) {
      if (!s.trim().isEmpty()) {
        formattedString.append(String.format(inKey ? "%s=" : "%s\r\n", s));
        inKey = !inKey;
      }
    }
    return formattedString.toString();
  }

  /**
   * Default override is expected to be using format of java properties file like
   * "key1:value1 key2=value2 ..."
   */
  public boolean applyConfigOverride(String configOverride) {
    if (configOverride == null || configOverride.isEmpty()) {
      return true;
    }

    Properties overrides = new Properties();
    try {
      overrides.load(
          new ByteArrayInputStream(convertSpaceToEOL(configOverride).getBytes()));

      for (Enumeration e = overrides.propertyNames(); e.hasMoreElements(); ) {
        String key = (String) e.nextElement();
        // Trim leading and ending \" in the string.
        if (overrides.getProperty(key).startsWith("\"")
            && overrides.getProperty(key).endsWith("\"")) {
          properties.setProperty(key, overrides.getProperty(key).replaceAll("^\"|\"$", ""));
        } else {
          properties.setProperty(key, overrides.getProperty(key));
        }
      }
    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Failed to apply config override " + configOverride, e);
      return false;
    }
    return true;
  }

  public boolean load(String configFile, String configOverride) {
    Properties oldProperties = this.properties;
    properties = new Properties();

    if (configFile == null || configFile.isEmpty()) {
      LOG.info("Config file is not provided. Load config from override cmd arguments.");
    } else {
      try {
        properties.load(new FileInputStream(configFile));
      } catch (IOException e) {
        properties = oldProperties;
        LOG.log(Level.SEVERE, "Failed to load properties config file", e);
        return false;
      }
    }

    if (!applyConfigOverride(configOverride)) {
      return false;
    }

    addDefaultProperties();
    if (Boolean.parseBoolean(properties.getProperty(Constants.HERON_VERBOSE).toString())) {
      LOG.info("Config parsed: \n" + properties);
    }
    return true;
  }

  public void addDefaultProperties() {
    addPropertyIfNotPresent(Constants.HERON_VERBOSE, Boolean.FALSE.toString());
    addPropertyIfNotPresent(Constants.DC, Constants.DC);
    addPropertyIfNotPresent(Constants.ROLE, Constants.ROLE);
    addPropertyIfNotPresent(Constants.ENVIRON, Constants.ENVIRON);
  }

  protected void addPropertyIfNotPresent(String key, String value) {
    if (!properties.containsKey(key)) {
      properties.setProperty(key, value);
    }
  }

  @Override
  public String getUploaderClass() {
    return properties.getProperty(Constants.UPLOADER_CLASS);
  }

  @Override
  public String getLauncherClass() {
    return properties.getProperty(Constants.LAUNCHER_CLASS);
  }

  @Override
  public String getSchedulerClass() {
    return properties.getProperty(Constants.SCHEDULER_CLASS);
  }

  @Override
  public String getRuntimeManagerClass() {
    return properties.getProperty(Constants.RUNTIME_MANAGER_CLASS);
  }

  @Override
  public String getPackingAlgorithmClass() {
    return properties.getProperty(Constants.PACKING_ALGORITHM_CLASS);
  }

  @Override
  public String getStateManagerClass() {
    return properties.getProperty(Constants.STATE_MANAGER_CLASS);
  }

  @Override
  public boolean isVerbose() {
    return Boolean.parseBoolean(properties.getProperty(Constants.HERON_VERBOSE));
  }

  @Override
  public Map<Object, Object> getConfig() {
    return properties;
  }
}