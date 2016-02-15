package com.twitter.heron.scheduler;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Paths;

import java.util.Map;
import java.util.HashMap;
import java.util.logging.Logger;
import java.util.logging.Level;

import com.twitter.heron.spi.common.Keys;
import com.twitter.heron.spi.common.Defaults;
import com.twitter.heron.spi.common.Context;
import com.twitter.heron.common.config.ClusterConfigReader;

public final class ClusterConfig {
  protected static Context loadClusterConfig(String cluster, String configPath) {
    Map config = ClusterConfigReader.load(cluster, configPath, Defaults.Files.CLUSTER_YAML);
    return Context.newBuilder().putAll(config).build();
  }

  protected static Context loadDefaultsConfig(String cluster, String configPath) {
    Map config = ClusterConfigReader.load(cluster, configPath, Defaults.Files.DEFAULTS_YAML);
    return Context.newBuilder().putAll(config).build();
  }

  protected static Context loadPackingConfig(String cluster, String configPath) {
    Map config = ClusterConfigReader.load(cluster, configPath, Defaults.Files.PACKING_YAML);
    return Context.newBuilder().putAll(config).build();
  }

  protected static Context loadSchedulerConfig(String cluster, String configPath) {
    Map config = ClusterConfigReader.load(cluster, configPath, Defaults.Files.SCHEDULER_YAML);
    return Context.newBuilder().putAll(config).build();
  }

  protected static Context loadStateManagerConfig(String cluster, String configPath) {
    Map config = ClusterConfigReader.load(cluster, configPath, Defaults.Files.STATEMGR_YAML);
    return Context.newBuilder().putAll(config).build();
  }

  protected static Context loadUploaderConfig(String cluster, String configPath) {
    Map config = ClusterConfigReader.load(cluster, configPath, Defaults.Files.UPLOADER_YAML);
    return Context.newBuilder().putAll(config).build();
  }
  
  public static Context loadConfig(String cluster, String configPath) {
    Context.Builder cb = Context.newBuilder()
        .putAll(loadClusterConfig(cluster, configPath))
        .putAll(loadDefaultsConfig(cluster, configPath))
        .putAll(loadPackingConfig(cluster, configPath))
        .putAll(loadSchedulerConfig(cluster, configPath))
        .putAll(loadStateManagerConfig(cluster, configPath))
        .putAll(loadUploaderConfig(cluster, configPath));

    return cb.build();
  }
}
