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

  protected static Config getDefaultHome() {
    Config.Builder cb = Config.newBuilder().put(Keys.HERON_HOME, Defaults.HERON_HOME);

    cb.put(Keys.HERON_BINS, Misc.substitute(Defaults.HERON_HOME, Defaults.HERON_BINS));
    cb.put(Keys.HERON_CONF, Misc.substitute(Defaults.HERON_HOME, Defaults.HERON_CONF));
    cb.put(Keys.HERON_DIST, Misc.substitute(Defaults.HERON_HOME, Defaults.HERON_DIST));
    cb.put(Keys.HERON_ETC, Misc.substitute(Defaults.HERON_HOME, Defaults.HERON_ETC));
    cb.put(Keys.HERON_LIBS, Misc.substitute(Defaults.HERON_HOME, Defaults.HERON_LIBS));

    return cb.build();
  }

  protected static Config getDefaultBinaries() {
    Config homeConfig = getDefaultHome();
    Config.Builder cb = Config.newBuilder();

    cb.put(Keys.EXECUTOR_BINARY, Misc.substitute(homeConfig, Defaults.EXECUTOR_BINARY));
    cb.put(Keys.STMGR_BINARY, Misc.substitute(homeConfig, Defaults.STMGR_BINARY));
    cb.put(Keys.TMASTER_BINARY, Misc.substitute(homeConfig, Defaults.TMASTER_BINARY));
    cb.put(Keys.SHELL_BINARY, Misc.substitute(homeConfig, Defaults.SHELL_BINARY));

    return cb.build();
  }

  protected static Config getDefaultJars() {
    Config homeConfig = getDefaultHome();
    Config.Builder cb = Config.newBuilder();

    cb.put(Keys.SCHEDULER_JAR, Misc.substitute(homeConfig, Defaults.SCHEDULER_JAR));

    return cb.build();
  }

  protected static Config getDefaultFilesAndPaths() {
    Config homeConfig = getDefaultHome();
    Config.Builder cb = Config.newBuilder();

    cb.put(Keys.CORE_PACKAGE_URI, Misc.substitute(homeConfig, Defaults.CORE_PACKAGE_URI));
    cb.put(Keys.METRICS_MANAGER_CLASSPATH, Defaults.METRICS_MANAGER_CLASSPATH);

    return cb.build();
  }

  protected static Config getDefaultResources() {
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
