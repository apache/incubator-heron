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

  private static Config loadHeronHome(String heronHome, String configPath) {
    Config.Builder cb = Config.newBuilder()
        .put(Key.HERON_HOME, heronHome)
        .put(Key.HERON_CONF, configPath);

    return putDefaults(cb, Key.HERON_HOME, heronHome,
        Key.HERON_BIN,
        Key.HERON_DIST,
        Key.HERON_ETC,
        Key.HERON_LIB,
        Key.JAVA_HOME).build();
  }

  private static Config loadSandboxHome(String heronSandboxHome, String configPath) {
    Config.Builder cb = Config.newBuilder()
        .put(Key.HERON_SANDBOX_HOME, heronSandboxHome)
        .put(Key.HERON_SANDBOX_CONF, configPath);

    return putDefaults(cb, Key.HERON_SANDBOX_HOME, heronSandboxHome,
        Key.HERON_SANDBOX_BIN,
        Key.HERON_SANDBOX_LIB,
        Key.HERON_SANDBOX_JAVA_HOME).build();
  }

  private static Config loadConfigHome(String heronHome, String configPath) {
    return putDefaults(Config.newBuilder(), Key.HERON_HOME, heronHome, Key.HERON_CONF, configPath,
        Key.CLUSTER_YAML,
        Key.CLIENT_YAML,
        Key.METRICS_YAML,
        Key.PACKING_YAML,
        Key.SCHEDULER_YAML,
        Key.STATEMGR_YAML,
        Key.SYSTEM_YAML,
        Key.UPLOADER_YAML).build();
  }

  private static Config loadSandboxConfigHome(String heronSandboxHome, String configPath) {
    return putDefaults(Config.newBuilder(),
        Key.HERON_SANDBOX_HOME, heronSandboxHome, Key.HERON_SANDBOX_CONF, configPath,
        Key.SANDBOX_CLUSTER_YAML,
        Key.SANDBOX_METRICS_YAML,
        Key.SANDBOX_PACKING_YAML,
        Key.SANDBOX_SCHEDULER_YAML,
        Key.SANDBOX_STATEMGR_YAML,
        Key.SANDBOX_SYSTEM_YAML,
        Key.SANDBOX_UPLOADER_YAML,
        Key.SANDBOX_OVERRIDE_YAML).build();
  }

  private static Config.Builder putDefaults(Config.Builder cb,
                                            Key homeKey, String heronHome,
                                            Key... keys) {
    for (Key key : keys) {
      if (key.getType() == Key.Type.STRING) {
        cb.put(key, Misc.substitute(homeKey, heronHome, key.getDefaultString()));
      } else {
        cb.put(key, key.getDefault());
      }
    }
    return cb;
  }

  private static Config.Builder putDefaults(Config.Builder cb,
                                            Key homeKey, String heronHome,
                                            Key configPathKey, String configPath,
                                            Key... keys) {
    for (Key key : keys) {
      if (key.getType() == Key.Type.STRING) {
        cb.put(key, Misc.substitute(
            homeKey, heronHome, configPathKey, configPath, key.getDefaultString()));
      } else {
        cb.put(key, key.getDefault());
      }
    }
    return cb;
  }

  private static Config loadBasicConfig(String heronHome, String configPath) {
    return Config.newBuilder()
        .putAll(loadHeronHome(heronHome, configPath))
        .putAll(loadConfigHome(heronHome, configPath))
        .build();
  }

  private static Config loadBasicSandboxConfig() {
    String defaultHome = Key.HERON_SANDBOX_HOME.getDefaultString();
    String defaultConfig = Key.HERON_SANDBOX_CONF.getDefaultString();

    return Config.newBuilder()
        .putAll(loadSandboxHome(defaultHome, defaultConfig))
        .putAll(loadSandboxConfigHome(defaultHome, defaultConfig))
        .build();
  }

  @VisibleForTesting
  static Config loadConfig(String file) {
    Map<String, Object> readConfig = ConfigReader.loadFile(file);
    return Config.newBuilder().putAll(readConfig).build();
  }

  public static Config loadConfig(String heronHome, String configPath,
                                  String releaseFile, String overrideConfigFile) {
    Config homeConfig = loadBasicConfig(heronHome, configPath);
    Config sandboxConfig = loadBasicSandboxConfig();

    Config.Builder cb = Config.newBuilder(true)
        .putAll(homeConfig)
        .putAll(sandboxConfig)
        .putAll(loadConfig(Context.clusterFile(homeConfig)))
        .putAll(loadConfig(Context.clientFile(homeConfig)))
        .putAll(loadConfig(Context.packingFile(homeConfig)))
        .putAll(loadConfig(Context.schedulerFile(homeConfig)))
        .putAll(loadConfig(Context.stateManagerFile(homeConfig)))
        .putAll(loadConfig(Context.uploaderFile(homeConfig)))
        .putAll(loadConfig(releaseFile))
        .putAll(loadConfig(overrideConfigFile));
    return cb.build();
  }

  public static Config loadSandboxConfig() {
    Config sandboxConfig = loadBasicSandboxConfig();

    Config.Builder cb = Config.newBuilder(true)
        .putAll(sandboxConfig)
        .putAll(loadConfig(Context.packingSandboxFile(sandboxConfig)))
        .putAll(loadConfig(Context.schedulerSandboxFile(sandboxConfig)))
        .putAll(loadConfig(Context.stateManagerSandboxFile(sandboxConfig)))
        .putAll(loadConfig(Context.uploaderSandboxFile(sandboxConfig)));

    // Add the override config at the end to replace any existing configs
    cb.putAll(loadConfig(Context.overrideSandboxFile(sandboxConfig)));

    return cb.build();
  }
}
