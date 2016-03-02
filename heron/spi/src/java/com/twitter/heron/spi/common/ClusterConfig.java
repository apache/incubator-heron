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
        .put(Keys.HERON_HOME, heronHome) 
        .put(Keys.HERON_BIN,  Misc.substitute(heronHome, Defaults.HERON_BIN))
        .put(Keys.HERON_CONF, configPath)
        .put(Keys.HERON_DIST, Misc.substitute(heronHome, Defaults.HERON_DIST))
        .put(Keys.HERON_ETC,  Misc.substitute(heronHome, Defaults.HERON_ETC))
        .put(Keys.HERON_LIB,  Misc.substitute(heronHome, Defaults.HERON_LIB));
    return cb.build();
  }

  protected static Config loadConfigHome(String heronHome, String configPath) {
    Config.Builder cb = Config.newBuilder()
        .put(Keys.CLUSTER_YAML, Misc.substitute(heronHome, configPath, Defaults.CLUSTER_YAML))
        .put(Keys.DEFAULTS_YAML, Misc.substitute(heronHome, configPath, Defaults.DEFAULTS_YAML))
        .put(Keys.METRICS_YAML, Misc.substitute(heronHome, configPath, Defaults.METRICS_YAML))
        .put(Keys.PACKING_YAML, Misc.substitute(heronHome, configPath, Defaults.PACKING_YAML))
        .put(Keys.SCHEDULER_YAML, Misc.substitute(heronHome, configPath, Defaults.SCHEDULER_YAML))
        .put(Keys.STATEMGR_YAML, Misc.substitute(heronHome, configPath, Defaults.STATEMGR_YAML))
        .put(Keys.SYSTEM_YAML, Misc.substitute(heronHome, configPath, Defaults.SYSTEM_YAML))
        .put(Keys.UPLOADER_YAML, Misc.substitute(heronHome, configPath, Defaults.UPLOADER_YAML));
    return cb.build();
  }

  protected static Config loadClusterConfig(Config config) {
    String clusterFile = config.getStringValue(Keys.CLUSTER_YAML); 
    Map readConfig = ConfigReader.loadFile(clusterFile);
    return Config.newBuilder().putAll(readConfig).build();
  }

  protected static Config loadDefaultsConfig(Config config) {
    String defaultsFile = config.getStringValue(Keys.DEFAULTS_YAML);
    Map readConfig = ConfigReader.loadFile(defaultsFile);
    return Config.newBuilder().putAll(readConfig).build();
  }

  protected static Config loadPackingConfig(Config config) {
    String packingFile = config.getStringValue(Keys.PACKING_YAML);
    Map readConfig = ConfigReader.loadFile(packingFile);
    return Config.newBuilder().putAll(readConfig).build();
  }

  protected static Config loadSchedulerConfig(Config config) {
    String schedulerFile = config.getStringValue(Keys.SCHEDULER_YAML);
    Map readConfig = ConfigReader.loadFile(schedulerFile);
    return Config.newBuilder().putAll(readConfig).build();
  }

  protected static Config loadStateManagerConfig(Config config) {
    String stateMgrFile = config.getStringValue(Keys.STATEMGR_YAML);
    Map readConfig = ConfigReader.loadFile(stateMgrFile);
    return Config.newBuilder().putAll(readConfig).build();
  }

  protected static Config loadUploaderConfig(Config config) {
    String uploaderFile = config.getStringValue(Keys.UPLOADER_YAML);
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
    String configPath = Misc.substitute(heronHome, Defaults.HERON_CONF);
    return loadConfig(heronHome, configPath);
  }
  */

  public static Config loadSandboxConfig(String cluster) {
    String configPath = Misc.substitute(Defaults.HERON_SANDBOX_HOME, Defaults.HERON_SANDBOX_CONF);
    return loadConfig(Defaults.HERON_SANDBOX_HOME, configPath);
  }

  public static Config loadSchedulerConfig() {
    String configPath = Misc.substitute(Defaults.HERON_SANDBOX_HOME, Defaults.HERON_SANDBOX_CONF);
    return loadHeronHome(Defaults.HERON_SANDBOX_HOME, configPath);  
  }
}
