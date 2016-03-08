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

    cb.put(Keys.heronHome(), Defaults.heronHome());
    cb.put(Keys.heronBin(),  Defaults.heronBin());
    cb.put(Keys.heronConf(), Defaults.heronConf());
    cb.put(Keys.heronDist(), Defaults.heronDist());
    cb.put(Keys.heronEtc(),  Defaults.heronEtc());
    cb.put(Keys.heronLib(),  Defaults.heronLib());
    cb.put(Keys.javaHome(),  Defaults.javaHome());
    return cb.build();
  }

  public static Config getDefaultFiles() {
    Config.Builder cb = Config.newBuilder();

    cb.put(Keys.clusterFile(),   Defaults.clusterFile());
    cb.put(Keys.defaultsFile(),  Defaults.defaultsFile());
    cb.put(Keys.metricsSinksFile(), Defaults.metricsSinksFile());
    cb.put(Keys.packingFile(),   Defaults.packingFile());
    cb.put(Keys.schedulerFile(), Defaults.schedulerFile());
    cb.put(Keys.stateManagerFile(), Defaults.stateManagerFile());
    cb.put(Keys.systemFile(),    Defaults.systemFile());
    cb.put(Keys.uploaderFile(),  Defaults.uploaderFile());
    return cb.build();
  }

  public static Config getDefaultBinaries() {
    Config.Builder cb = Config.newBuilder();

    cb.put(Keys.executorBinary(), Defaults.executorBinary());
    cb.put(Keys.stmgrBinary(),    Defaults.stmgrBinary());
    cb.put(Keys.tmasterBinary(),  Defaults.tmasterBinary());
    cb.put(Keys.shellBinary(),    Defaults.shellBinary());
    return cb.build();
  }

  public static Config getDefaultJars() {
    Config.Builder cb = Config.newBuilder();

    cb.put(Keys.schedulerJar(),   Defaults.schedulerJar());
    return cb.build();
  }

  public static Config getDefaultFilesAndPaths() {
    Config.Builder cb = Config.newBuilder();

    cb.put(Keys.corePackageUri(), Defaults.corePackageUri());
    cb.put(Keys.logDirectory(), Defaults.logDirectory());

    cb.put(Keys.instanceClassPath(), Defaults.instanceClassPath());
    cb.put(Keys.metricsManagerClassPath(), Defaults.metricsManagerClassPath());
    cb.put(Keys.packingClassPath(), Defaults.packingClassPath());
    cb.put(Keys.schedulerClassPath(), Defaults.schedulerClassPath());
    cb.put(Keys.stateManagerClassPath(), Defaults.stateManagerClassPath());
    cb.put(Keys.uploaderClassPath(), Defaults.uploaderClassPath());
    return cb.build();
  }

  public static Config getDefaultResources() {
    Config.Builder cb = Config.newBuilder();

    cb.put(Keys.stmgrRam(), Defaults.stmgrRam());
    cb.put(Keys.instanceCpu(), Defaults.instanceCpu());
    cb.put(Keys.instanceRam(), Defaults.instanceRam());
    cb.put(Keys.instanceDisk(), Defaults.instanceDisk());
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
