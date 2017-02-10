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

import com.google.common.annotations.VisibleForTesting;

import com.twitter.heron.common.config.ConfigReader;

public final class ClusterConfig {

  private ClusterConfig() {
  }

  private static Config loadDefaults(String heronHome, String configPath) {
    return Config.newBuilder(true)
        .put(Key.HERON_HOME, heronHome)
        .put(Key.HERON_CONF, configPath)
        .put(Key.HERON_SANDBOX_HOME, Key.HERON_SANDBOX_HOME.getDefaultString())
        .put(Key.HERON_SANDBOX_CONF, Key.HERON_SANDBOX_CONF.getDefaultString())
        .build();
  }

  @VisibleForTesting
  static Config loadConfig(String file) {
    Map<String, Object> readConfig = ConfigReader.loadFile(file);
    return addFromFile(readConfig);
  }

  @VisibleForTesting
  static Config addFromFile(Map<String, Object> readConfig) {
    return Config.newBuilder().putAll(readConfig).build();
  }

  public static Config loadConfig(String heronHome, String configPath,
                                  String releaseFile, String overrideConfigFile) {
    Config defaultConfig = loadDefaults(heronHome, configPath);
    Config localConfig = Config.toLocalMode(defaultConfig);

    Config.Builder cb = Config.newBuilder()
        .putAll(defaultConfig)
        .putAll(loadConfig(Context.clusterFile(localConfig)))
        .putAll(loadConfig(Context.clientFile(localConfig)))
        .putAll(loadConfig(Context.packingFile(localConfig)))
        .putAll(loadConfig(Context.schedulerFile(localConfig)))
        .putAll(loadConfig(Context.stateManagerFile(localConfig)))
        .putAll(loadConfig(Context.uploaderFile(localConfig)))
        .putAll(loadConfig(releaseFile))
        .putAll(loadConfig(overrideConfigFile));
    return cb.build();
  }

  public static Config loadSandboxConfig() {
    String homePath = Key.HERON_SANDBOX_HOME.getDefaultString();
    String configPath = Key.HERON_SANDBOX_CONF.getDefaultString();

    Config defaultConfig = loadDefaults(homePath, configPath);
    Config remoteConfig = Config.toRemoteMode(defaultConfig);

    Config.Builder cb = Config.newBuilder()
        .putAll(defaultConfig)
        .putAll(loadConfig(Context.packingFile(remoteConfig)))
        .putAll(loadConfig(Context.schedulerFile(remoteConfig)))
        .putAll(loadConfig(Context.stateManagerFile(remoteConfig)))
        .putAll(loadConfig(Context.uploaderFile(remoteConfig)));

    // Add the override config at the end to replace any existing configs
    cb.putAll(loadConfig(Context.overrideFile(remoteConfig)));

    return cb.build();
  }
}
