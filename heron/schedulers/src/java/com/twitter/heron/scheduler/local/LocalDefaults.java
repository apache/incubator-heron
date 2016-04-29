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

package com.twitter.heron.scheduler.local;

import java.util.Map;
import java.util.logging.Logger;

import com.twitter.heron.spi.common.Resource;

public final class LocalDefaults {
  private LocalDefaults() {

  }

  private static final Logger LOG = Logger.getLogger(LocalDefaults.class.getName());

  // holds the mapping between the config keys and their default values
  private static Map defaults;

  // load the resource for default config key values
  static {
    try {
      defaults = Resource.load(
          "com.twitter.heron.scheduler.local.LocalDefaults",
          LocalConstants.DEFAULTS_YAML);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("Failed to load the Defaults class ", e);
    }
  }

  /*
   * Get the default value for the given config key
   *
   * @param key, the config key
   * @return String, the default value for the config key
   */
  public static String get(String key) {
    return (String) defaults.get(key);
  }
}
