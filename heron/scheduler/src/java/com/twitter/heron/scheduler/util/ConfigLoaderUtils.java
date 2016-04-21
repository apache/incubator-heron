// Copyright 2016 Twitter. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.twitter.heron.scheduler.util;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.twitter.heron.spi.common.Constants;

/**
 * Heron config utilities.
 */
public final class ConfigLoaderUtils {
  private static final Logger LOG = Logger.getLogger(ConfigLoaderUtils.class.getName());

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
   * Load properties from the specified properties file into the target properties.
   *
   * @return <code>true</code> only if the operation succeeds.
   */
  public static boolean loadPropertiesFile(Properties p, String propertiesFile) {
    if (propertiesFile == null || propertiesFile.isEmpty()) {
      LOG.info("Properties file not found: " + propertiesFile);
    } else {
      try {
        p.load(new FileInputStream(propertiesFile));
      } catch (IOException e) {
        LOG.log(Level.SEVERE, "Failed to load properties file: " + propertiesFile, e);
        return false;
      }
    }
    return true;
  }

  /**
   * Update a target Properties using the specified propertyOverride in the format of Java properties file.
   * <p>
   * The configOverride is expected to be using the format of Java properties file like.
   * "key1:value1 key2=value2 ..."
   * <p>
   * The properties parsed from configOverride are added to the specified target.
   */
  public static boolean applyPropertyOverride(Properties target, String propertyOverride) {
    if (propertyOverride == null || propertyOverride.isEmpty()) {
      return true;
    }

    Properties overrides = new Properties();

    try {
      overrides.load(new ByteArrayInputStream(convertSpaceToEOL(propertyOverride).getBytes()));

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
      LOG.log(Level.SEVERE, "Failed to apply property override " + propertyOverride, e);
      return false;
    }

    return true;
  }

  public static boolean applyConfigPropertyOverride(Properties target) {
    if (target.containsKey(Constants.CONFIG_PROPERTY)) {
      String configOverride = target.getProperty(Constants.CONFIG_PROPERTY);
      return applyPropertyOverride(target, configOverride);
    }
    return true;
  }
}
