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

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Paths;

import java.util.Map;
import java.util.HashMap;
import java.util.logging.Logger;
import java.util.logging.Level;

import com.twitter.heron.common.config.ConfigReader;

public final class ClusterConfig {

  protected static Config loadHeronHome(String heronHome, String configPath) {
    Config.Builder cb = Config.newBuilder()
        .put(Keys.heronHome(), heronHome) 
        .put(Keys.heronBin(),  Misc.substitute(heronHome, Defaults.heronBin()))
        .put(Keys.heronConf(), configPath)
        .put(Keys.heronDist(), Misc.substitute(heronHome, Defaults.heronDist()))
        .put(Keys.heronEtc(),  Misc.substitute(heronHome, Defaults.heronEtc()))
        .put(Keys.heronLib(),  Misc.substitute(heronHome, Defaults.heronLib()))
        .put(Keys.javaHome(),  Misc.substitute(heronHome, Defaults.javaHome()));
    return cb.build();
  }

  protected static Config loadSandboxHome(String heronSandboxHome, String configPath) {
    Config.Builder cb = Config.newBuilder()
        .put(Keys.heronSandboxHome(), heronSandboxHome) 
        .put(Keys.heronSandboxBin(),  Misc.substituteSandbox(heronSandboxHome, Defaults.heronSandboxBin()))
        .put(Keys.heronSandboxConf(), configPath)
        .put(Keys.heronSandboxLib(),  Misc.substituteSandbox(heronSandboxHome, Defaults.heronSandboxLib()))
        .put(Keys.javaSandboxHome(),  Misc.substituteSandbox(heronSandboxHome, Defaults.javaSandboxHome()));
    return cb.build();
  }

  protected static Config loadConfigHome(String heronHome, String configPath) {
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

  protected static Config loadSandboxConfigHome(String heronSandboxHome, String configPath) {
    Config.Builder cb = Config.newBuilder()
        .put(Keys.clusterSandboxFile(),
            Misc.substituteSandbox(heronSandboxHome, configPath, Defaults.clusterSandboxFile()))
        .put(Keys.defaultsSandboxFile(),
            Misc.substituteSandbox(heronSandboxHome, configPath, Defaults.defaultsSandboxFile()))
        .put(Keys.metricsSinksSandboxFile(),
            Misc.substituteSandbox(heronSandboxHome, configPath, Defaults.metricsSinksSandboxFile()))
        .put(Keys.packingSandboxFile(),
            Misc.substituteSandbox(heronSandboxHome, configPath, Defaults.packingSandboxFile()))
        .put(Keys.schedulerSandboxFile(),
            Misc.substituteSandbox(heronSandboxHome, configPath, Defaults.schedulerSandboxFile()))
        .put(Keys.stateManagerSandboxFile(),
            Misc.substituteSandbox(heronSandboxHome, configPath, Defaults.stateManagerSandboxFile()))
        .put(Keys.systemSandboxFile(),
            Misc.substituteSandbox(heronSandboxHome, configPath, Defaults.systemSandboxFile()))
        .put(Keys.uploaderSandboxFile(), 
            Misc.substituteSandbox(heronSandboxHome, configPath, Defaults.uploaderSandboxFile()));
    return cb.build();
  }

  protected static Config loadClusterConfig(String clusterFile) {
    Map readConfig = ConfigReader.loadFile(clusterFile);
    return Config.newBuilder().putAll(readConfig).build();
  }

  protected static Config loadClientConfig(String clientFile) {
    Map readConfig = ConfigReader.loadFile(clientFile);
    return Config.newBuilder().putAll(readConfig).build();
  }

  protected static Config loadDefaultsConfig(String defaultsFile) {
    Map readConfig = ConfigReader.loadFile(defaultsFile);
    return Config.newBuilder().putAll(readConfig).build();
  }

  protected static Config loadPackingConfig(String packingFile) {
    Map readConfig = ConfigReader.loadFile(packingFile);
    return Config.newBuilder().putAll(readConfig).build();
  }

  protected static Config loadSchedulerConfig(String schedulerFile) {
    Map readConfig = ConfigReader.loadFile(schedulerFile);
    return Config.newBuilder().putAll(readConfig).build();
  }

  protected static Config loadStateManagerConfig(String stateMgrFile) {
    Map readConfig = ConfigReader.loadFile(stateMgrFile);
    return Config.newBuilder().putAll(readConfig).build();
  }

  protected static Config loadUploaderConfig(String uploaderFile) {
    Map readConfig = ConfigReader.loadFile(uploaderFile);
    return Config.newBuilder().putAll(readConfig).build();
  }
  
  public static Config loadBasicConfig(String heronHome, String configPath) {
    Config config = Config.newBuilder()
        .putAll(loadHeronHome(heronHome, configPath))
        .putAll(loadConfigHome(heronHome, configPath))
        .build();
    return config;
  }

  public static Config loadBasicSandboxConfig() {
    Config config = Config.newBuilder()
        .putAll(loadSandboxHome(Defaults.heronSandboxHome(), Defaults.heronSandboxConf()))
        .putAll(loadSandboxConfigHome(Defaults.heronSandboxHome(), Defaults.heronSandboxConf()))
        .build();
    return config;
  }

  public static Config loadConfig(String heronHome, String configPath) {
    Config homeConfig = loadBasicConfig(heronHome, configPath); 
    Config sandboxConfig = loadBasicSandboxConfig();

    Config.Builder cb = Config.newBuilder()
        .putAll(homeConfig)
        .putAll(sandboxConfig)
        .putAll(loadClusterConfig(Context.clusterFile(homeConfig)))
        .putAll(loadClientConfig(Context.clientFile(homeConfig)))
        .putAll(loadPackingConfig(Context.packingFile(homeConfig)))
        .putAll(loadSchedulerConfig(Context.schedulerFile(homeConfig)))
        .putAll(loadStateManagerConfig(Context.stateManagerFile(homeConfig)))
        .putAll(loadUploaderConfig(Context.uploaderFile(homeConfig)));
    return cb.build();
  }

  public static Config loadSandboxConfig() {
    Config sandboxConfig = loadBasicSandboxConfig(); 

    Config.Builder cb = Config.newBuilder()
        .putAll(sandboxConfig)
        .putAll(loadPackingConfig(Context.packingSandboxFile(sandboxConfig)))
        .putAll(loadSchedulerConfig(Context.schedulerSandboxFile(sandboxConfig)))
        .putAll(loadStateManagerConfig(Context.stateManagerSandboxFile(sandboxConfig)))
        .putAll(loadUploaderConfig(Context.uploaderSandboxFile(sandboxConfig)));
    return cb.build();
  }
}
