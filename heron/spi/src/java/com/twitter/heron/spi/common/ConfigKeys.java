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

package com.twitter.heron.spi.common;

import java.util.Map;
import java.util.logging.Logger;

import com.google.common.annotations.VisibleForTesting;

/**
 * Loads valid config keys from com/twitter/heron/spi/common/keys.yaml into a map using the
 * classloader from com.twitter.heron.spi.common.Keys
 */
public final class ConfigKeys {
  private static final Logger LOG = Logger.getLogger(ConfigKeys.class.getName());

  // name of the resource file that holds the config keys
  private static final String KEYS_YAML = "com/twitter/heron/spi/common/keys.yaml";

  // holds the mapping of keys to their corresponding key strings
  @VisibleForTesting
  static Map<String, Object> keys;

  // load the resource for config keys
  static {
    try {
      keys = Resource.load("com.twitter.heron.spi.common.Keys", KEYS_YAML);
    } catch (ClassNotFoundException e) {
      LOG.severe("Unable to load the config Keys class " + e);
      throw new RuntimeException("Failed to load ConfigKeys class");
    }
  }

  private ConfigKeys() {
  }

  /*
   * Get the key string value for the given key
   *
   * @param key, the key
   * @return String, the key string value for the key
   */
  public static String get(String key) {
    return (String) keys.get(key);
  }
}
