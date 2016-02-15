package com.twitter.heron.scheduler;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Paths;

import java.util.Map;
import java.util.HashMap;
import java.util.logging.Logger;
import java.util.logging.Level;

import com.twitter.heron.spi.common.Defaults;
import com.twitter.heron.spi.common.Keys;
import com.twitter.heron.spi.common.Context;
import com.twitter.heron.common.config.ClusterConfigReader;

public final class ClusterDefaults {

  protected static Context getDefaultBinaries() {
     Context.Builder cb = Context.newBuilder()
         .put(Keys.Config.EXECUTOR_BINARY, Defaults.Config.EXECUTOR_BINARY)
         .put(Keys.Config.STMGR_BINARY, Defaults.Config.STMGR_BINARY)
         .put(Keys.Config.TMASTER_BINARY, Defaults.Config.TMASTER_BINARY)
         .put(Keys.Config.SHELL_BINARY, Defaults.Config.SHELL_BINARY);

     return cb.build();
  }

  protected static Context getDefaultJars() {
     Context.Builder cb = Context.newBuilder()
         .put(Keys.Config.SCHEDULER_JAR, Defaults.Config.SCHEDULER_JAR);

     return cb.build();
  }

  protected static Context getDefaultResources() {
     Context.Builder cb = Context.newBuilder()
         .put(Keys.Config.STMGR_RAM, Long.valueOf(Defaults.Config.STMGR_RAM))
         .put(Keys.Config.INSTANCE_CPU, Long.valueOf(Defaults.Config.INSTANCE_CPU))
         .put(Keys.Config.INSTANCE_RAM, Long.valueOf(Defaults.Config.INSTANCE_RAM))
         .put(Keys.Config.INSTANCE_DISK, Long.valueOf(Defaults.Config.INSTANCE_DISK));

     return cb.build();
  }
     
  public static Context getDefaults() {
     Context.Builder cb = Context.newBuilder()
         .putAll(getDefaultBinaries())
         .putAll(getDefaultJars())
         .putAll(getDefaultResources());

     return cb.build();
  }
}
