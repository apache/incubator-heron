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

    cb.put(Keys.heronHome(), Defaults.heronHome());
    cb.put(Keys.heronBin(), Defaults.heronBin());
    cb.put(Keys.heronConf(), Defaults.heronConf());
    cb.put(Keys.heronDist(), Defaults.heronDist());
    cb.put(Keys.heronEtc(), Defaults.heronEtc());
    cb.put(Keys.heronLib(), Defaults.heronLib());
    cb.put(Keys.javaHome(), Defaults.javaHome());
    return cb.build();
  }

  private static Config getSandboxHome() {
    Config.Builder cb = Config.newBuilder();

    cb.put(Keys.heronSandboxHome(), Defaults.heronSandboxHome());
    cb.put(Keys.heronSandboxBin(), Defaults.heronSandboxBin());
    cb.put(Keys.heronSandboxConf(), Defaults.heronSandboxConf());
    cb.put(Keys.heronSandboxLib(), Defaults.heronSandboxLib());
    cb.put(Keys.javaSandboxHome(), Defaults.javaSandboxHome());
    return cb.build();
  }

  @VisibleForTesting
  static Config getSandboxBinaries() {
    Config.Builder cb = Config.newBuilder();

    cb.put(Keys.executorSandboxBinary(), Defaults.executorSandboxBinary());
    cb.put(Keys.stmgrSandboxBinary(), Defaults.stmgrSandboxBinary());
    cb.put(Keys.tmasterSandboxBinary(), Defaults.tmasterSandboxBinary());
    cb.put(Keys.shellSandboxBinary(), Defaults.shellSandboxBinary());
    cb.put(Keys.pythonInstanceSandboxBinary(), Defaults.pythonInstanceSandboxBinary());
    return cb.build();
  }

  @VisibleForTesting
  static Config getDefaultJars() {
    Config.Builder cb = Config.newBuilder();

    cb.put(Keys.schedulerJar(), Defaults.schedulerJar());
    return cb.build();
  }

  private static Config getSandboxJars() {
    Config.Builder cb = Config.newBuilder();

    cb.put(Keys.schedulerSandboxJar(), Defaults.schedulerSandboxJar());
    return cb.build();
  }

  private static Config getDefaultFilesAndPaths() {
    Config.Builder cb = Config.newBuilder();

    cb.put(Keys.corePackageUri(), Defaults.corePackageUri());
    // cb.put(Keys.logDirectory(), Defaults.logDirectory());

    cb.put(Keys.instanceClassPath(), Defaults.instanceClassPath());
    cb.put(Keys.metricsManagerClassPath(), Defaults.metricsManagerClassPath());
    cb.put(Keys.packingClassPath(), Defaults.packingClassPath());
    cb.put(Keys.schedulerClassPath(), Defaults.schedulerClassPath());
    cb.put(Keys.stateManagerClassPath(), Defaults.stateManagerClassPath());
    cb.put(Keys.uploaderClassPath(), Defaults.uploaderClassPath());
    return cb.build();
  }

  private static Config getSandboxFilesAndPaths() {
    Config.Builder cb = Config.newBuilder();

    cb.put(Keys.instanceSandboxClassPath(), Defaults.instanceSandboxClassPath());
    cb.put(Keys.metricsManagerSandboxClassPath(), Defaults.metricsManagerSandboxClassPath());
    cb.put(Keys.packingSandboxClassPath(), Defaults.packingSandboxClassPath());
    cb.put(Keys.schedulerSandboxClassPath(), Defaults.schedulerSandboxClassPath());
    cb.put(Keys.stateManagerSandboxClassPath(), Defaults.stateManagerSandboxClassPath());
    cb.put(Keys.uploaderSandboxClassPath(), Defaults.uploaderSandboxClassPath());
    return cb.build();
  }

  static Config getDefaultResources() {
    Config.Builder cb = Config.newBuilder();

    cb.put(Keys.stmgrRam(), Defaults.stmgrRam());
    cb.put(Keys.instanceCpu(), Defaults.instanceCpu());
    cb.put(Keys.instanceRam(), Defaults.instanceRam());
    cb.put(Keys.instanceDisk(), Defaults.instanceDisk());
    return cb.build();
  }

  @VisibleForTesting
  static Config getDefaultMiscellaneous() {
    Config.Builder cb = Config.newBuilder();

    cb.put(Keys.verbose(), Defaults.verbose());
    cb.put(Keys.schedulerService(), Defaults.schedulerService());
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
