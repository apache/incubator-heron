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

import com.twitter.heron.common.basics.ByteAmount;
import com.twitter.heron.common.basics.DryRunFormatType;
import com.twitter.heron.common.basics.PackageType;
import com.twitter.heron.common.basics.TypeUtils;

/**
 * Loads default configs from com/twitter/heron/spi/common/defaults.yaml into a map using the
 * classloader from com.twitter.heron.spi.common.Defaults
 */
final class ConfigDefaults {
  private static final Logger LOG = Logger.getLogger(ConfigDefaults.class.getName());

  // name of the resource file that holds the default values for config keys
  private static final String DEFAULTS_YAML = "com/twitter/heron/spi/common/defaults.yaml";

  // holds the mapping between the config keys and their default values
  @VisibleForTesting
  static Map<String, Object> defaults;

  // load the resource for default config key values
  static {
    try {
      defaults = Resource.load("com.twitter.heron.spi.common.Defaults", DEFAULTS_YAML);
    } catch (ClassNotFoundException e) {
      LOG.severe("Unable to load the defaults class " + e);
      throw new RuntimeException("Failed to load the ConfigDefaults class");
    }
  }

  private ConfigDefaults() {
  }

  /*
   * Get the default value for the given config key
   *
   * @param key, the config key
   * @return String, the default value for the config key
   */
  static String get(String key) {
    return (String) defaults.get(key);
  }

  static Long getLong(String key) {
    return TypeUtils.getLong(defaults.get(key));
  }

  static ByteAmount getByteAmount(String key) {
    return ByteAmount.fromBytes(getLong(key));
  }

  static PackageType getPackageType(String key) {
    return (PackageType) defaults.get(key);
  }

  static DryRunFormatType getDryRunFormatType(String key) {
    return (DryRunFormatType) defaults.get(key);
  }

  static Double getDouble(String key) {
    return TypeUtils.getDouble(defaults.get(key));
  }

  static Boolean getBoolean(String key) {
    return TypeUtils.getBoolean(defaults.get(key));
  }
}
