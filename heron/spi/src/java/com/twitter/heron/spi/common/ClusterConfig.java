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

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.google.common.annotations.VisibleForTesting;

import com.twitter.heron.common.config.ConfigReader;

public final class ClusterConfig {

  private ClusterConfig() {
  }

  private static final Set<Key> NO_SUB_KEYS = new HashSet<>(Arrays.asList(
      Key.HERON_HOME, Key.HERON_CONF, Key.HERON_SANDBOX_HOME, Key.HERON_SANDBOX_CONF));

  private static Config loadDefaults(String heronHome, String configPath) {
    Config defaults = Config.newBuilder(true)
        .put(Key.HERON_HOME, heronHome)
        .put(Key.HERON_CONF, configPath)
        .put(Key.HERON_SANDBOX_HOME, heronHome)
        .put(Key.HERON_SANDBOX_CONF, configPath)
        .build();

    Config.Builder cb = Config.newBuilder().putAll(defaults);
    for (Key key : Key.values()) {
      if (!NO_SUB_KEYS.contains(key) && key.getDefault() != null) {
        if (key.getType() == Key.Type.STRING) {
          cb.put(key, Misc.substitute(defaults, key.getDefaultString()));
        } else {
          cb.put(key, key.getDefault());
        }
      }
    }

    return cb.build();
  }

  @VisibleForTesting
  static Config loadConfig(String file) {
    Map<String, Object> readConfig = ConfigReader.loadFile(file);
    return Config.newBuilder().putAll(readConfig).build();
  }

  public static Config loadConfig(String heronHome, String configPath,
                                  String releaseFile, String overrideConfigFile) {
    Config defaultConfig = loadDefaults(heronHome, configPath);

    Config.Builder cb = Config.newBuilder()
        .putAll(defaultConfig)
        .putAll(loadConfig(Context.clusterFile(defaultConfig)))
        .putAll(loadConfig(Context.clientFile(defaultConfig)))
        .putAll(loadConfig(Context.packingFile(defaultConfig)))
        .putAll(loadConfig(Context.schedulerFile(defaultConfig)))
        .putAll(loadConfig(Context.stateManagerFile(defaultConfig)))
        .putAll(loadConfig(Context.uploaderFile(defaultConfig)))
        .putAll(loadConfig(releaseFile))
        .putAll(loadConfig(overrideConfigFile));
    return cb.build();
  }

  public static Config loadSandboxConfig() {
    String homePath = Key.HERON_SANDBOX_HOME.getDefaultString();
    String configPath = Key.HERON_SANDBOX_CONF.getDefaultString();

    Config defaultConfig = loadDefaults(homePath, configPath);

    Config.Builder cb = Config.newBuilder()
        .putAll(defaultConfig)
        .putAll(loadConfig(Context.packingFile(defaultConfig)))
        .putAll(loadConfig(Context.schedulerFile(defaultConfig)))
        .putAll(loadConfig(Context.stateManagerFile(defaultConfig)))
        .putAll(loadConfig(Context.uploaderFile(defaultConfig)));

    // Add the override config at the end to replace any existing configs
    cb.putAll(loadConfig(Context.overrideFile(defaultConfig)));

    return cb.build();
  }
}
