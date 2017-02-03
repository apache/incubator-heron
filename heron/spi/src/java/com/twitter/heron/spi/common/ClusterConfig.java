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

  @VisibleForTesting
  static Config loadHeronHome(String heronHome, String configPath) {
    Config.Builder cb = Config.newBuilder()
        .put(Key.HERON_HOME, heronHome)
        .put(Key.HERON_BIN, Misc.substitute(heronHome, Defaults.heronBin()))
        .put(Key.HERON_CONF, configPath)
        .put(Key.HERON_DIST, Misc.substitute(heronHome, Defaults.heronDist()))
        .put(Key.HERON_ETC, Misc.substitute(heronHome, Defaults.heronEtc()))
        .put(Key.HERON_LIB, Misc.substitute(heronHome, Defaults.heronLib()))
        .put(Key.JAVA_HOME, Misc.substitute(heronHome, Defaults.javaHome()));
    return cb.build();
  }

  private static Config loadSandboxHome(String heronSandboxHome, String configPath) {
    Config.Builder cb = Config.newBuilder()
        .put(Key.HERON_SANDBOX_HOME, heronSandboxHome)
        .put(Key.HERON_SANDBOX_BIN,
            Misc.substituteSandbox(heronSandboxHome, Defaults.heronSandboxBin()))
        .put(Key.HERON_SANDBOX_CONF, configPath)
        .put(Key.HERON_SANDBOX_LIB,
            Misc.substituteSandbox(heronSandboxHome, Defaults.heronSandboxLib()))
        .put(Key.HERON_SANDBOX_JAVA_HOME,
            Misc.substituteSandbox(heronSandboxHome, Defaults.javaSandboxHome()));
    return cb.build();
  }

  @VisibleForTesting
  static Config loadConfigHome(String heronHome, String configPath) {
    Config.Builder cb = Config.newBuilder()
        .put(Key.CLUSTER_YAML,
            Misc.substitute(heronHome, configPath, Defaults.clusterFile()))
        .put(Key.CLIENT_YAML,
            Misc.substitute(heronHome, configPath, Defaults.clientFile()))
        .put(Key.DEFAULTS_YAML,
            Misc.substitute(heronHome, configPath, Defaults.defaultsFile()))
        .put(Key.METRICS_YAML,
            Misc.substitute(heronHome, configPath, Defaults.metricsSinksFile()))
        .put(Key.PACKING_YAML,
            Misc.substitute(heronHome, configPath, Defaults.packingFile()))
        .put(Key.SCHEDULER_YAML,
            Misc.substitute(heronHome, configPath, Defaults.schedulerFile()))
        .put(Key.STATEMGR_YAML,
            Misc.substitute(heronHome, configPath, Defaults.stateManagerFile()))
        .put(Key.SYSTEM_YAML,
            Misc.substitute(heronHome, configPath, Defaults.systemFile()))
        .put(Key.UPLOADER_YAML,
            Misc.substitute(heronHome, configPath, Defaults.uploaderFile()));
    return cb.build();
  }

  private static Config loadSandboxConfigHome(String heronSandboxHome, String configPath) {
    Config.Builder cb = Config.newBuilder()
        .put(Key.SANDBOX_CLUSTER_YAML,
            Misc.substituteSandbox(heronSandboxHome, configPath, Defaults.clusterSandboxFile()))
        .put(Key.SANDBOX_DEFAULTS_YAML,
            Misc.substituteSandbox(heronSandboxHome, configPath, Defaults.defaultsSandboxFile()))
        .put(Key.SANDBOX_METRICS_YAML,
            Misc.substituteSandbox(
                heronSandboxHome, configPath, Defaults.metricsSinksSandboxFile()))
        .put(Key.SANDBOX_PACKING_YAML,
            Misc.substituteSandbox(heronSandboxHome, configPath, Defaults.packingSandboxFile()))
        .put(Key.SANDBOX_SCHEDULER_YAML,
            Misc.substituteSandbox(heronSandboxHome, configPath, Defaults.schedulerSandboxFile()))
        .put(Key.SANDBOX_SCHEDULER_YAML,
            Misc.substituteSandbox(
                heronSandboxHome, configPath, Defaults.stateManagerSandboxFile()))
        .put(Key.SANDBOX_SYSTEM_YAML,
            Misc.substituteSandbox(heronSandboxHome, configPath, Defaults.systemSandboxFile()))
        .put(Key.SANDBOX_UPLOADER_YAML,
            Misc.substituteSandbox(heronSandboxHome, configPath, Defaults.uploaderSandboxFile()))
        .put(Key.SANDBOX_OVERRIDE_YAML,
            Misc.substituteSandbox(heronSandboxHome, configPath, Defaults.overrideSandboxFile()));
    return cb.build();
  }

  @VisibleForTesting
  static Config loadConfig(String file) {
    Map<String, Object> readConfig = ConfigReader.loadFile(file);
    return Config.newBuilder().putAll(readConfig).build();
  }

  private static Config loadBasicConfig(String heronHome, String configPath) {
    return Config.newBuilder()
        .putAll(loadHeronHome(heronHome, configPath))
        .putAll(loadConfigHome(heronHome, configPath))
        .build();
  }

  private static Config loadBasicSandboxConfig() {
    return Config.newBuilder()
        .putAll(ClusterDefaults.getSandboxDefaults())
        .putAll(loadSandboxHome(Defaults.heronSandboxHome(), Defaults.heronSandboxConf()))
        .putAll(loadSandboxConfigHome(Defaults.heronSandboxHome(), Defaults.heronSandboxConf()))
        .build();
  }

  public static Config loadConfig(String heronHome, String configPath,
                                  String releaseFile, String overrideConfigFile) {
    Config homeConfig = loadBasicConfig(heronHome, configPath);
    Config sandboxConfig = loadBasicSandboxConfig();

    Config.Builder cb = Config.newBuilder()
        .putAll(ClusterDefaults.getDefaults())
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

    Config.Builder cb = Config.newBuilder()
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
