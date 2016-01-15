package com.twitter.heron.scheduler.util;

import java.io.ByteArrayInputStream;
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
 * Loads config in the Java properties file format.
 */
public abstract class AbstractPropertiesConfigLoader implements IConfigLoader {
  private static final Logger LOG = Logger.getLogger(AbstractPropertiesConfigLoader.class.getName());

  public final Properties properties = new Properties();

  public static String convertSpaceToEOL(String propertyOverride) {
    // Tokenize strings by spaces. Ignore spaces inside quotes. Ignore \" while parsing.
    StringBuilder token = new StringBuilder();
    ArrayList<String> tokens = new ArrayList<String>();
    boolean escaped = false;
    boolean inQuotes = false;
    for (char ch : propertyOverride.toCharArray()) {
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
   * Update the underlying Properties using the specified configOverride in the format of Java properties file.
   *
   * Default override is expected to be using format of java properties file like
   *   "key1:value1 key2=value2 ..."
   */
  public final boolean applyOverride(String propertyOverride) {
    Properties p = new Properties();

    if (applyOverride(p, propertyOverride)) {
      for (String key: p.stringPropertyNames()) {
        properties.setProperty(key, p.getProperty(key));
      }
      return true;
    } else {
      return false;
    }
  }

  /**
   * Update a target Properties using the specified configOverride in the format of Java properties file.
   *
   * The configOverride is expected to be using the format of Java properties file like.
   *   "key1:value1 key2=value2 ..."
   *
   * The properties parsed from configOverride are added to the specified target.
   */
  public final boolean applyOverride(Properties target, String configOverride) {
    if (configOverride == null || configOverride.isEmpty()) {
      return true;
    }

    Properties overrides = new Properties();

    try {
      overrides.load(new ByteArrayInputStream(convertSpaceToEOL(configOverride).getBytes()));

      for (Enumeration e = overrides.propertyNames(); e.hasMoreElements(); ) {
        String key = (String) e.nextElement();
        // Trim leading and ending \" in the string.
        if (overrides.getProperty(key).startsWith("\"") && overrides.getProperty(key).endsWith("\"")) {
          target.setProperty(key, overrides.getProperty(key).replaceAll("^\"|\"$", ""));
        } else {
          target.setProperty(key, overrides.getProperty(key));
        }
      }
    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Failed to apply config override " + configOverride, e);
      return false;
    }

    return true;
  }

  public final boolean applyConfigPropertyOverride() {
    if (properties.containsKey(Constants.CONFIG_PROPERTY)) {
      String configOverride = properties.getProperty(Constants.CONFIG_PROPERTY);
      return applyOverride(properties, configOverride);
    }
    return true;
  }

  protected void addPropertyIfNotPresent(String key, String value) {
    if (!properties.containsKey(key)) {
      properties.setProperty(key, value);
    }
  }

  @Override
  public Map<Object, Object> getConfig() {
    return properties;
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

  public final String getHeronDir() {
    return properties.getProperty(Constants.HERON_DIR);
  }

  public final String getHeronConfigPath() {
    return properties.getProperty(Constants.HERON_CONFIG_PATH);
  }

}
