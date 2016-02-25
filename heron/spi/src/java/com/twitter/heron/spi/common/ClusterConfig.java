package com.twitter.heron.spi.common;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Paths;

import java.util.Map;
import java.util.HashMap;
import java.util.logging.Logger;
import java.util.logging.Level;

import com.twitter.heron.common.config.ClusterConfigReader;

public final class ClusterConfig {

  protected static Config loadHeronHome(String heronHome, String configPath) {
    Config.Builder cb = Config.newBuilder()
        .put(Keys.HERON_HOME, heronHome) 
        .put(Keys.HERON_BINS, Misc.substitute(heronHome, Defaults.HERON_BINS))
        .put(Keys.HERON_CONF, configPath)
        .put(Keys.HERON_DIST, Misc.substitute(heronHome, Defaults.HERON_DIST))
        .put(Keys.HERON_ETC,  Misc.substitute(heronHome, Defaults.HERON_ETC))
        .put(Keys.HERON_LIBS, Misc.substitute(heronHome, Defaults.HERON_LIBS));

    return cb.build();
  }

  protected static Config loadClusterConfig(String cluster, String configPath) {
    Map config = ClusterConfigReader.load(cluster, configPath, Defaults.CLUSTER_YAML);
    return Config.newBuilder().putAll(config).build();
  }

  protected static Config loadDefaultsConfig(String cluster, String configPath) {
    Map config = ClusterConfigReader.load(cluster, configPath, Defaults.DEFAULTS_YAML);
    return Config.newBuilder().putAll(config).build();
  }

  protected static Config loadPackingConfig(String cluster, String configPath) {
    Map config = ClusterConfigReader.load(cluster, configPath, Defaults.PACKING_YAML);
    return Config.newBuilder().putAll(config).build();
  }

  protected static Config loadSchedulerConfig(String cluster, String configPath) {
    Map config = ClusterConfigReader.load(cluster, configPath, Defaults.SCHEDULER_YAML);
    return Config.newBuilder().putAll(config).build();
  }

  protected static Config loadStateManagerConfig(String cluster, String configPath) {
    Map config = ClusterConfigReader.load(cluster, configPath, Defaults.STATEMGR_YAML);
    return Config.newBuilder().putAll(config).build();
  }

  protected static Config loadUploaderConfig(String cluster, String configPath) {
    Map config = ClusterConfigReader.load(cluster, configPath, Defaults.UPLOADER_YAML);
    return Config.newBuilder().putAll(config).build();
  }
  
  public static Config loadConfig(String heronHome, String cluster, String configPath) {
    Config.Builder cb = Config.newBuilder()
        .putAll(loadHeronHome(heronHome, configPath))
        .putAll(loadClusterConfig(cluster, configPath))
        .putAll(loadDefaultsConfig(cluster, configPath))
        .putAll(loadPackingConfig(cluster, configPath))
        .putAll(loadSchedulerConfig(cluster, configPath))
        .putAll(loadStateManagerConfig(cluster, configPath))
        .putAll(loadUploaderConfig(cluster, configPath));

    return cb.build();
  }

  public static Config loadConfig(String heronHome, String cluster) {
    String configPath = Misc.substitute(heronHome, Defaults.HERON_CONF);
    return loadConfig(heronHome, cluster, configPath);
  }
}
