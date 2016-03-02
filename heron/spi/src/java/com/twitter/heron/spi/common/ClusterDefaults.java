package com.twitter.heron.spi.common;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Paths;

import java.util.Map;
import java.util.HashMap;
import java.util.logging.Logger;
import java.util.logging.Level;

import com.twitter.heron.common.config.ClusterConfigReader;

public final class ClusterDefaults {

  public static Config getDefaultHome() {
    Config.Builder cb = Config.newBuilder();

    cb.put(Keys.HERON_HOME, Defaults.HERON_HOME);
    cb.put(Keys.HERON_BIN,  Defaults.HERON_BIN);
    cb.put(Keys.HERON_CONF, Defaults.HERON_CONF);
    cb.put(Keys.HERON_DIST, Defaults.HERON_DIST);
    cb.put(Keys.HERON_ETC,  Defaults.HERON_ETC);
    cb.put(Keys.HERON_LIB,  Defaults.HERON_LIB);
    return cb.build();
  }

  public static Config getDefaultFiles() {
    Config.Builder cb = Config.newBuilder();

    cb.put(Keys.CLUSTER_YAML,   Defaults.CLUSTER_YAML);
    cb.put(Keys.DEFAULTS_YAML,  Defaults.DEFAULTS_YAML);
    cb.put(Keys.METRICS_YAML,   Defaults.METRICS_YAML);
    cb.put(Keys.PACKING_YAML,   Defaults.PACKING_YAML);
    cb.put(Keys.SCHEDULER_YAML, Defaults.SCHEDULER_YAML);
    cb.put(Keys.STATEMGR_YAML,  Defaults.STATEMGR_YAML);
    cb.put(Keys.SYSTEM_YAML,    Defaults.SYSTEM_YAML);
    cb.put(Keys.UPLOADER_YAML,  Defaults.UPLOADER_YAML);
    return cb.build();
  }

  public static Config getDefaultBinaries() {
    Config.Builder cb = Config.newBuilder();

    cb.put(Keys.EXECUTOR_BINARY, Defaults.EXECUTOR_BINARY);
    cb.put(Keys.STMGR_BINARY, Defaults.STMGR_BINARY);
    cb.put(Keys.TMASTER_BINARY, Defaults.TMASTER_BINARY);
    cb.put(Keys.SHELL_BINARY, Defaults.SHELL_BINARY);
    return cb.build();
  }

  public static Config getDefaultJars() {
    Config.Builder cb = Config.newBuilder();

    cb.put(Keys.SCHEDULER_JAR, Defaults.SCHEDULER_JAR);
    return cb.build();
  }

  public static Config getDefaultFilesAndPaths() {
    Config.Builder cb = Config.newBuilder();

    cb.put(Keys.CORE_PACKAGE_URI, Defaults.CORE_PACKAGE_URI);
    cb.put(Keys.LOGGING_DIRECTORY, Defaults.LOGGING_DIRECTORY);
    cb.put(Keys.METRICS_MANAGER_CLASSPATH, Defaults.METRICS_MANAGER_CLASSPATH);
    return cb.build();
  }

  public static Config getDefaultResources() {
    Config.Builder cb = Config.newBuilder();

    cb.put(Keys.STMGR_RAM, Long.valueOf(Defaults.STMGR_RAM));
    cb.put(Keys.INSTANCE_CPU, Long.valueOf(Defaults.INSTANCE_CPU));
    cb.put(Keys.INSTANCE_RAM, Long.valueOf(Defaults.INSTANCE_RAM));
    cb.put(Keys.INSTANCE_DISK, Long.valueOf(Defaults.INSTANCE_DISK));
    return cb.build();
  }
     
  public static Config getDefaults() {
    Config.Builder cb = Config.newBuilder();
   
    cb.putAll(getDefaultHome());
    cb.putAll(getDefaultBinaries());
    cb.putAll(getDefaultJars());
    cb.putAll(getDefaultFilesAndPaths());
    cb.putAll(getDefaultResources());
    return cb.build();
  }
}
