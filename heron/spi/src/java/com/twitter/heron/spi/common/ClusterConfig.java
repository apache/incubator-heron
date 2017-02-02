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
        .put(Keys.heronHome(), heronHome)
        .put(Keys.heronBin(), Misc.substitute(heronHome, Defaults.heronBin()))
        .put(Keys.heronConf(), configPath)
        .put(Keys.heronDist(), Misc.substitute(heronHome, Defaults.heronDist()))
        .put(Keys.heronEtc(), Misc.substitute(heronHome, Defaults.heronEtc()))
        .put(Keys.heronLib(), Misc.substitute(heronHome, Defaults.heronLib()))
        .put(Keys.javaHome(), Misc.substitute(heronHome, Defaults.javaHome()));
    return cb.build();
  }

  private static Config loadSandboxHome(String heronSandboxHome, String configPath) {
    Config.Builder cb = Config.newBuilder()
        .put(Keys.heronSandboxHome(), heronSandboxHome)
        .put(Keys.heronSandboxBin(),
            Misc.substituteSandbox(heronSandboxHome, Defaults.heronSandboxBin()))
        .put(Keys.heronSandboxConf(), configPath)
        .put(Keys.heronSandboxLib(),
            Misc.substituteSandbox(heronSandboxHome, Defaults.heronSandboxLib()))
        .put(Keys.javaSandboxHome(),
            Misc.substituteSandbox(heronSandboxHome, Defaults.javaSandboxHome()));
    return cb.build();
  }

  @VisibleForTesting
  static Config loadConfigHome(String heronHome, String configPath) {
    Config.Builder cb = Config.newBuilder()
        .put(Keys.clusterFile(),
            Misc.substitute(heronHome, configPath, Defaults.clusterFile()))
        .put(Keys.clientFile(),
            Misc.substitute(heronHome, configPath, Defaults.clientFile()))
        .put(Keys.defaultsFile(),
            Misc.substitute(heronHome, configPath, Defaults.defaultsFile()))
        .put(Keys.metricsSinksFile(),
            Misc.substitute(heronHome, configPath, Defaults.metricsSinksFile()))
        .put(Keys.packingFile(),
            Misc.substitute(heronHome, configPath, Defaults.packingFile()))
        .put(Keys.schedulerFile(),
            Misc.substitute(heronHome, configPath, Defaults.schedulerFile()))
        .put(Keys.stateManagerFile(),
            Misc.substitute(heronHome, configPath, Defaults.stateManagerFile()))
        .put(Keys.systemFile(),
            Misc.substitute(heronHome, configPath, Defaults.systemFile()))
        .put(Keys.uploaderFile(),
            Misc.substitute(heronHome, configPath, Defaults.uploaderFile()));
    return cb.build();
  }

  private static Config loadSandboxConfigHome(String heronSandboxHome, String configPath) {
    Config.Builder cb = Config.newBuilder()
        .put(Keys.clusterSandboxFile(),
            Misc.substituteSandbox(heronSandboxHome, configPath, Defaults.clusterSandboxFile()))
        .put(Keys.defaultsSandboxFile(),
            Misc.substituteSandbox(heronSandboxHome, configPath, Defaults.defaultsSandboxFile()))
        .put(Keys.metricsSinksSandboxFile(),
            Misc.substituteSandbox(
                heronSandboxHome, configPath, Defaults.metricsSinksSandboxFile()))
        .put(Keys.packingSandboxFile(),
            Misc.substituteSandbox(heronSandboxHome, configPath, Defaults.packingSandboxFile()))
        .put(Keys.schedulerSandboxFile(),
            Misc.substituteSandbox(heronSandboxHome, configPath, Defaults.schedulerSandboxFile()))
        .put(Keys.stateManagerSandboxFile(),
            Misc.substituteSandbox(
                heronSandboxHome, configPath, Defaults.stateManagerSandboxFile()))
        .put(Keys.systemSandboxFile(),
            Misc.substituteSandbox(heronSandboxHome, configPath, Defaults.systemSandboxFile()))
        .put(Keys.uploaderSandboxFile(),
            Misc.substituteSandbox(heronSandboxHome, configPath, Defaults.uploaderSandboxFile()))
        .put(Keys.overrideSandboxFile(),
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
