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
    System.out.println(heronHome + " " + Keys.javaHome() + " " + Defaults.javaHome());
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

  protected static Config loadConfigHome(String heronHome, String configPath) {
    Config.Builder cb = Config.newBuilder()
        .put(Keys.clusterFile(),
            Misc.substitute(heronHome, configPath, Defaults.clusterFile()))
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

  protected static Config loadClusterConfig(Config config) {
    String clusterFile = config.getStringValue(Keys.clusterFile()); 
    Map readConfig = ConfigReader.loadFile(clusterFile);
    return Config.newBuilder().putAll(readConfig).build();
  }

  protected static Config loadDefaultsConfig(Config config) {
    String defaultsFile = config.getStringValue(Keys.defaultsFile());
    Map readConfig = ConfigReader.loadFile(defaultsFile);
    return Config.newBuilder().putAll(readConfig).build();
  }

  protected static Config loadPackingConfig(Config config) {
    String packingFile = config.getStringValue(Keys.packingFile());
    Map readConfig = ConfigReader.loadFile(packingFile);
    return Config.newBuilder().putAll(readConfig).build();
  }

  protected static Config loadSchedulerConfig(Config config) {
    String schedulerFile = config.getStringValue(Keys.schedulerFile());
    Map readConfig = ConfigReader.loadFile(schedulerFile);
    return Config.newBuilder().putAll(readConfig).build();
  }

  protected static Config loadStateManagerConfig(Config config) {
    String stateMgrFile = config.getStringValue(Keys.stateManagerFile());
    Map readConfig = ConfigReader.loadFile(stateMgrFile);
    return Config.newBuilder().putAll(readConfig).build();
  }

  protected static Config loadUploaderConfig(Config config) {
    String uploaderFile = config.getStringValue(Keys.uploaderFile());
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

  public static Config loadConfig(String heronHome, String configPath) {
    Config homeConfig = loadBasicConfig(heronHome, configPath); 

    Config.Builder cb = Config.newBuilder()
        .putAll(homeConfig)
        .putAll(loadDefaultsConfig(homeConfig))
        .putAll(loadPackingConfig(homeConfig))
        .putAll(loadSchedulerConfig(homeConfig))
        .putAll(loadStateManagerConfig(homeConfig))
        .putAll(loadUploaderConfig(homeConfig));
    return cb.build();
  }

  public static Config loadSandboxConfig() {
    String configPath = Misc.substitute(
        Defaults.sandboxHome(), Defaults.sandboxConf());
    return loadConfig(Defaults.sandboxHome(), configPath);
  }
}
