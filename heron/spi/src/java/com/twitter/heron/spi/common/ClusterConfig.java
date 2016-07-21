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

import com.twitter.heron.common.config.ConfigReader;

public final class ClusterConfig {

  private ClusterConfig() {
  }

  protected static SpiCommonConfig loadHeronHome(String heronHome, String configPath) {
    SpiCommonConfig.Builder cb = SpiCommonConfig.newBuilder()
        .put(Keys.heronHome(), heronHome)
        .put(Keys.heronBin(), Misc.substitute(heronHome, Defaults.heronBin()))
        .put(Keys.heronConf(), configPath)
        .put(Keys.heronDist(), Misc.substitute(heronHome, Defaults.heronDist()))
        .put(Keys.heronEtc(), Misc.substitute(heronHome, Defaults.heronEtc()))
        .put(Keys.heronLib(), Misc.substitute(heronHome, Defaults.heronLib()))
        .put(Keys.javaHome(), Misc.substitute(heronHome, Defaults.javaHome()));
    return cb.build();
  }

  protected static SpiCommonConfig loadSandboxHome(String heronSandboxHome, String configPath) {
    SpiCommonConfig.Builder cb = SpiCommonConfig.newBuilder()
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

  protected static SpiCommonConfig loadConfigHome(String heronHome, String configPath) {
    SpiCommonConfig.Builder cb = SpiCommonConfig.newBuilder()
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

  protected static SpiCommonConfig loadSandboxConfigHome(
      String heronSandboxHome, String configPath) {
    SpiCommonConfig.Builder cb = SpiCommonConfig.newBuilder()
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

  protected static SpiCommonConfig loadClusterConfig(String clusterFile) {
    Map<String, Object> readConfig = ConfigReader.loadFile(clusterFile);
    return SpiCommonConfig.newBuilder().putAll(readConfig).build();
  }

  protected static SpiCommonConfig loadClientConfig(String clientFile) {
    Map<String, Object> readConfig = ConfigReader.loadFile(clientFile);
    return SpiCommonConfig.newBuilder().putAll(readConfig).build();
  }

  protected static SpiCommonConfig loadDefaultsConfig(String defaultsFile) {
    Map<String, Object> readConfig = ConfigReader.loadFile(defaultsFile);
    return SpiCommonConfig.newBuilder().putAll(readConfig).build();
  }

  protected static SpiCommonConfig loadPackingConfig(String packingFile) {
    Map<String, Object> readConfig = ConfigReader.loadFile(packingFile);
    return SpiCommonConfig.newBuilder().putAll(readConfig).build();
  }

  protected static SpiCommonConfig loadSchedulerConfig(String schedulerFile) {
    Map<String, Object> readConfig = ConfigReader.loadFile(schedulerFile);
    return SpiCommonConfig.newBuilder().putAll(readConfig).build();
  }

  protected static SpiCommonConfig loadStateManagerConfig(String stateMgrFile) {
    Map<String, Object> readConfig = ConfigReader.loadFile(stateMgrFile);
    return SpiCommonConfig.newBuilder().putAll(readConfig).build();
  }

  protected static SpiCommonConfig loadUploaderConfig(String uploaderFile) {
    Map<String, Object> readConfig = ConfigReader.loadFile(uploaderFile);
    return SpiCommonConfig.newBuilder().putAll(readConfig).build();
  }

  public static SpiCommonConfig loadOverrideConfig(String overrideConfigFile) {
    Map<String, Object> readConfig = ConfigReader.loadFile(overrideConfigFile);
    return SpiCommonConfig.newBuilder().putAll(readConfig).build();
  }

  protected static SpiCommonConfig loadReleaseConfig(String releaseFile) {
    Map<String, Object> readConfig = ConfigReader.loadFile(releaseFile);
    return SpiCommonConfig.newBuilder().putAll(readConfig).build();
  }

  public static SpiCommonConfig loadBasicConfig(String heronHome, String configPath) {
    return SpiCommonConfig.newBuilder()
        .putAll(loadHeronHome(heronHome, configPath))
        .putAll(loadConfigHome(heronHome, configPath))
        .build();
  }

  public static SpiCommonConfig loadBasicSandboxConfig() {
    return SpiCommonConfig.newBuilder()
        .putAll(loadSandboxHome(Defaults.heronSandboxHome(), Defaults.heronSandboxConf()))
        .putAll(loadSandboxConfigHome(Defaults.heronSandboxHome(), Defaults.heronSandboxConf()))
        .build();
  }

  public static SpiCommonConfig loadConfig(String heronHome, String configPath,
      String releaseFile) {
    SpiCommonConfig homeConfig = loadBasicConfig(heronHome, configPath);
    SpiCommonConfig sandboxConfig = loadBasicSandboxConfig();

    SpiCommonConfig.Builder cb = SpiCommonConfig.newBuilder()
        .putAll(homeConfig)
        .putAll(sandboxConfig)
        .putAll(loadClusterConfig(Context.clusterFile(homeConfig)))
        .putAll(loadClientConfig(Context.clientFile(homeConfig)))
        .putAll(loadPackingConfig(Context.packingFile(homeConfig)))
        .putAll(loadSchedulerConfig(Context.schedulerFile(homeConfig)))
        .putAll(loadStateManagerConfig(Context.stateManagerFile(homeConfig)))
        .putAll(loadUploaderConfig(Context.uploaderFile(homeConfig)))
        .putAll(loadReleaseConfig(releaseFile));
    return cb.build();
  }

  public static SpiCommonConfig loadSandboxConfig() {
    SpiCommonConfig sandboxConfig = loadBasicSandboxConfig();

    SpiCommonConfig.Builder cb = SpiCommonConfig.newBuilder()
        .putAll(sandboxConfig)
        .putAll(loadPackingConfig(Context.packingSandboxFile(sandboxConfig)))
        .putAll(loadSchedulerConfig(Context.schedulerSandboxFile(sandboxConfig)))
        .putAll(loadStateManagerConfig(Context.stateManagerSandboxFile(sandboxConfig)))
        .putAll(loadUploaderConfig(Context.uploaderSandboxFile(sandboxConfig)));

    // Add the override config at the end to replace any exisiting configs
    cb.putAll(loadOverrideConfig(Context.overrideSandboxFile(sandboxConfig)));

    return cb.build();
  }
}
