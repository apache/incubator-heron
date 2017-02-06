// Copyright 2016 Twitter. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.twitter.heron.spi.common;

import com.google.common.annotations.VisibleForTesting;

public final class ClusterDefaults {

  private ClusterDefaults() {
  }

  private static Config getDefaultHome() {
    Config.Builder cb = Config.newBuilder();

    cb.put(Key.HERON_HOME, Defaults.heronHome());
    cb.put(Key.HERON_BIN, Defaults.heronBin());
    cb.put(Key.HERON_CONF, Defaults.heronConf());
    cb.put(Key.HERON_DIST, Defaults.heronDist());
    cb.put(Key.HERON_ETC, Defaults.heronEtc());
    cb.put(Key.HERON_LIB, Defaults.heronLib());
    cb.put(Key.JAVA_HOME, Defaults.javaHome());
    return cb.build();
  }

  private static Config getSandboxHome() {
    Config.Builder cb = Config.newBuilder();

    cb.put(Key.HERON_SANDBOX_HOME, Defaults.heronSandboxHome());
    cb.put(Key.HERON_SANDBOX_BIN, Defaults.heronSandboxBin());
    cb.put(Key.HERON_SANDBOX_CONF, Defaults.heronSandboxConf());
    cb.put(Key.HERON_SANDBOX_LIB, Defaults.heronSandboxLib());
    cb.put(Key.HERON_SANDBOX_JAVA_HOME, Defaults.javaSandboxHome());
    return cb.build();
  }

  @VisibleForTesting
  static Config getSandboxBinaries() {
    Config.Builder cb = Config.newBuilder();

    cb.put(Key.SANDBOX_EXECUTOR_BINARY, Defaults.executorSandboxBinary());
    cb.put(Key.SANDBOX_STMGR_BINARY, Defaults.stmgrSandboxBinary());
    cb.put(Key.SANDBOX_TMASTER_BINARY, Defaults.tmasterSandboxBinary());
    cb.put(Key.SANDBOX_SHELL_BINARY, Defaults.shellSandboxBinary());
    cb.put(Key.SANDBOX_PYTHON_INSTANCE_BINARY, Defaults.pythonInstanceSandboxBinary());
    return cb.build();
  }

  @VisibleForTesting
  static Config getDefaultJars() {
    Config.Builder cb = Config.newBuilder();

    cb.put(Key.SCHEDULER_JAR, Defaults.schedulerJar());
    return cb.build();
  }

  private static Config getSandboxJars() {
    Config.Builder cb = Config.newBuilder();

    cb.put(Key.SANDBOX_SCHEDULER_JAR, Defaults.schedulerSandboxJar());
    return cb.build();
  }

  private static Config getDefaultFilesAndPaths() {
    Config.Builder cb = Config.newBuilder();

    cb.put(Key.CORE_PACKAGE_URI, Defaults.corePackageUri());
    // cb.put(Keys.logDirectory(), Defaults.logDirectory());

    cb.put(Key.INSTANCE_CLASSPATH, Defaults.instanceClassPath());
    cb.put(Key.METRICSMGR_CLASSPATH, Defaults.metricsManagerClassPath());
    cb.put(Key.PACKING_CLASSPATH, Defaults.packingClassPath());
    cb.put(Key.SCHEDULER_CLASSPATH, Defaults.schedulerClassPath());
    cb.put(Key.STATEMGR_CLASSPATH, Defaults.stateManagerClassPath());
    cb.put(Key.UPLOADER_CLASSPATH, Defaults.uploaderClassPath());
    return cb.build();
  }

  private static Config getSandboxFilesAndPaths() {
    Config.Builder cb = Config.newBuilder();

    cb.put(Key.SANDBOX_INSTANCE_CLASSPATH, Defaults.instanceSandboxClassPath());
    cb.put(Key.SANDBOX_METRICSMGR_CLASSPATH, Defaults.metricsManagerSandboxClassPath());
    cb.put(Key.SANDBOX_METRICSCACHEMGR_CLASSPATH, Defaults.metricsCacheManagerSandboxClassPath());
    cb.put(Key.SANDBOX_PACKING_CLASSPATH, Defaults.packingSandboxClassPath());
    cb.put(Key.SANDBOX_SCHEDULER_CLASSPATH, Defaults.schedulerSandboxClassPath());
    cb.put(Key.SANDBOX_STATEMGR_CLASSPATH, Defaults.stateManagerSandboxClassPath());
    cb.put(Key.SANDBOX_UPLOADER_CLASSPATH, Defaults.uploaderSandboxClassPath());
    return cb.build();
  }

  static Config getDefaultResources() {
    Config.Builder cb = Config.newBuilder();

    cb.put(Key.STMGR_RAM, Defaults.stmgrRam());
    cb.put(Key.INSTANCE_CPU, Defaults.instanceCpu());
    cb.put(Key.INSTANCE_RAM, Defaults.instanceRam());
    cb.put(Key.INSTANCE_DISK, Defaults.instanceDisk());
    return cb.build();
  }

  @VisibleForTesting
  static Config getDefaultMiscellaneous() {
    Config.Builder cb = Config.newBuilder();

    cb.put(Key.VERBOSE, Defaults.verbose());
    cb.put(Key.SCHEDULER_IS_SERVICE, Defaults.schedulerService());
    return cb.build();
  }

  public static Config getDefaults() {
    Config.Builder cb = Config.newBuilder();

    cb.putAll(getDefaultHome());
    cb.putAll(getDefaultJars());
    cb.putAll(getDefaultFilesAndPaths());
    cb.putAll(getDefaultResources());
    cb.putAll(getDefaultMiscellaneous());
    return cb.build();
  }

  static Config getSandboxDefaults() {
    Config.Builder cb = Config.newBuilder();

    cb.putAll(getSandboxHome());
    cb.putAll(getSandboxBinaries());
    cb.putAll(getSandboxJars());
    cb.putAll(getSandboxFilesAndPaths());
    cb.putAll(getDefaultResources());
    return cb.build();
  }
}
