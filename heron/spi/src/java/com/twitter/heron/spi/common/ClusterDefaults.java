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

    cb.put(Keys.get("HERON_HOME"), Defaults.get("HERON_HOME"));
    cb.put(Keys.get("HERON_BIN"),  Defaults.get("HERON_BIN"));
    cb.put(Keys.get("HERON_CONF"), Defaults.get("HERON_CONF"));
    cb.put(Keys.get("HERON_DIST"), Defaults.get("HERON_DIST"));
    cb.put(Keys.get("HERON_ETC"),  Defaults.get("HERON_ETC"));
    cb.put(Keys.get("HERON_LIB"),  Defaults.get("HERON_LIB"));
    return cb.build();
  }

  public static Config getDefaultFiles() {
    Config.Builder cb = Config.newBuilder();

    cb.put(Keys.get("CLUSTER_YAML"),   Defaults.get("CLUSTER_YAML"));
    cb.put(Keys.get("DEFAULTS_YAML"),  Defaults.get("DEFAULTS_YAML"));
    cb.put(Keys.get("METRICS_YAML"),   Defaults.get("METRICS_YAML"));
    cb.put(Keys.get("PACKING_YAML"),   Defaults.get("PACKING_YAML"));
    cb.put(Keys.get("SCHEDULER_YAML"), Defaults.get("SCHEDULER_YAML"));
    cb.put(Keys.get("STATEMGR_YAML"),  Defaults.get("STATEMGR_YAML"));
    cb.put(Keys.get("SYSTEM_YAML"),    Defaults.get("SYSTEM_YAML"));
    cb.put(Keys.get("UPLOADER_YAML"),  Defaults.get("UPLOADER_YAML"));
    return cb.build();
  }

  public static Config getDefaultBinaries() {
    Config.Builder cb = Config.newBuilder();

    cb.put(Keys.get("EXECUTOR_BINARY"), Defaults.get("EXECUTOR_BINARY"));
    cb.put(Keys.get("STMGR_BINARY"), Defaults.get("STMGR_BINARY"));
    cb.put(Keys.get("TMASTER_BINARY"), Defaults.get("TMASTER_BINARY"));
    cb.put(Keys.get("SHELL_BINARY"), Defaults.get("SHELL_BINARY"));
    return cb.build();
  }

  public static Config getDefaultJars() {
    Config.Builder cb = Config.newBuilder();

    cb.put(Keys.get("SCHEDULER_JAR"), Defaults.get("SCHEDULER_JAR"));
    return cb.build();
  }

  public static Config getDefaultFilesAndPaths() {
    Config.Builder cb = Config.newBuilder();

    cb.put(Keys.get("CORE_PACKAGE_URI"), Defaults.get("CORE_PACKAGE_URI"));
    cb.put(Keys.get("LOGGING_DIRECTORY"), Defaults.get("LOGGING_DIRECTORY"));
    cb.put(Keys.get("METRICS_MANAGER_CLASSPATH"), Defaults.get("METRICS_MANAGER_CLASSPATH"));
    return cb.build();
  }

  public static Config getDefaultResources() {
    Config.Builder cb = Config.newBuilder();

    cb.put(Keys.get("STMGR_RAM"), Defaults.getLong("STMGR_RAM"));
    cb.put(Keys.get("INSTANCE_CPU"), Defaults.getDouble("INSTANCE_CPU"));
    cb.put(Keys.get("INSTANCE_RAM"), Defaults.getLong("INSTANCE_RAM"));
    cb.put(Keys.get("INSTANCE_DISK"), Defaults.getLong("INSTANCE_DISK"));
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
