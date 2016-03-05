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
        .put(Keys.get("HERON_HOME"), heronHome) 
        .put(Keys.get("HERON_BIN"),  Misc.substitute(heronHome, Defaults.get("HERON_BIN")))
        .put(Keys.get("HERON_CONF"), configPath)
        .put(Keys.get("HERON_DIST"), Misc.substitute(heronHome, Defaults.get("HERON_DIST")))
        .put(Keys.get("HERON_ETC"),  Misc.substitute(heronHome, Defaults.get("HERON_ETC")))
        .put(Keys.get("HERON_LIB"),  Misc.substitute(heronHome, Defaults.get("HERON_LIB")));
    return cb.build();
  }

  protected static Config loadConfigHome(String heronHome, String configPath) {
    Config.Builder cb = Config.newBuilder()
        .put(Keys.get("CLUSTER_YAML"), 
            Misc.substitute(heronHome, configPath, Defaults.get("CLUSTER_YAML")))
        .put(Keys.get("DEFAULTS_YAML"), 
            Misc.substitute(heronHome, configPath, Defaults.get("DEFAULTS_YAML")))
        .put(Keys.get("METRICS_YAML"), 
            Misc.substitute(heronHome, configPath, Defaults.get("METRICS_YAML")))
        .put(Keys.get("PACKING_YAML"), 
            Misc.substitute(heronHome, configPath, Defaults.get("PACKING_YAML")))
        .put(Keys.get("SCHEDULER_YAML"), 
            Misc.substitute(heronHome, configPath, Defaults.get("SCHEDULER_YAML")))
        .put(Keys.get("STATEMGR_YAML"), 
            Misc.substitute(heronHome, configPath, Defaults.get("STATEMGR_YAML")))
        .put(Keys.get("SYSTEM_YAML"), 
            Misc.substitute(heronHome, configPath, Defaults.get("SYSTEM_YAML")))
        .put(Keys.get("UPLOADER_YAML"), 
            Misc.substitute(heronHome, configPath, Defaults.get("UPLOADER_YAML")));
    return cb.build();
  }

  protected static Config loadClusterConfig(Config config) {
    String clusterFile = config.getStringValue(Keys.get("CLUSTER_YAML")); 
    Map readConfig = ConfigReader.loadFile(clusterFile);
    return Config.newBuilder().putAll(readConfig).build();
  }

  protected static Config loadDefaultsConfig(Config config) {
    String defaultsFile = config.getStringValue(Keys.get("DEFAULTS_YAML"));
    Map readConfig = ConfigReader.loadFile(defaultsFile);
    return Config.newBuilder().putAll(readConfig).build();
  }

  protected static Config loadPackingConfig(Config config) {
    String packingFile = config.getStringValue(Keys.get("PACKING_YAML"));
    Map readConfig = ConfigReader.loadFile(packingFile);
    return Config.newBuilder().putAll(readConfig).build();
  }

  protected static Config loadSchedulerConfig(Config config) {
    String schedulerFile = config.getStringValue(Keys.get("SCHEDULER_YAML"));
    Map readConfig = ConfigReader.loadFile(schedulerFile);
    return Config.newBuilder().putAll(readConfig).build();
  }

  protected static Config loadStateManagerConfig(Config config) {
    String stateMgrFile = config.getStringValue(Keys.get("STATEMGR_YAML"));
    Map readConfig = ConfigReader.loadFile(stateMgrFile);
    return Config.newBuilder().putAll(readConfig).build();
  }

  protected static Config loadUploaderConfig(Config config) {
    String uploaderFile = config.getStringValue(Keys.get("UPLOADER_YAML"));
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

  /*
  public static Config loadConfig(String heronHome, String configPath) {
    String configPath = Misc.substitute(heronHome, Defaults.get("HERON_CONF"));
    return loadConfig(heronHome, configPath);
  }
  */

  public static Config loadSandboxConfig() {
    String configPath = Misc.substitute(
        Defaults.get("HERON_SANDBOX_HOME"), Defaults.get("HERON_SANDBOX_CONF"));
    return loadConfig(Defaults.get("HERON_SANDBOX_HOME"), configPath);
  }
}
