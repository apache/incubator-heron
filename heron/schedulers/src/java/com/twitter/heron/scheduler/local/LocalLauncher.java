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

package com.twitter.heron.scheduler.local;

import java.io.File;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.twitter.heron.common.basics.SysUtils;
import com.twitter.heron.scheduler.utils.Runtime;
import com.twitter.heron.scheduler.utils.SchedulerUtils;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Context;
import com.twitter.heron.spi.packing.PackingPlan;
import com.twitter.heron.spi.scheduler.ILauncher;
import com.twitter.heron.spi.utils.ShellUtils;

/**
 * Launch topology locally to a working directory.
 */
public class LocalLauncher implements ILauncher {
  protected static final Logger LOG = Logger.getLogger(LocalLauncher.class.getName());

  private Config config;
  private Config runtime;

  private String topologyWorkingDirectory;

  @Override
  public void initialize(Config mConfig, Config mRuntime) {
    this.config = mConfig;
    this.runtime = mRuntime;

    // get the topology working directory
    this.topologyWorkingDirectory = LocalContext.workingDirectory(config);
  }

  @Override
  public void close() {

  }

  /**
   * Launch the topology
   */
  @Override
  public boolean launch(PackingPlan packing) {
    LOG.log(Level.FINE, "Launching topology for local cluster {0}",
        LocalContext.cluster(config));

    // setup the working directory
    // mainly it downloads and extracts the heron-core-release and topology package
    if (!setupWorkingDirectory()) {
      LOG.severe("Failed to setup working directory");
      return false;
    }

    String[] schedulerCmd = getSchedulerCommand();

    Process p = startScheduler(schedulerCmd);

    if (p == null) {
      LOG.severe("Failed to start SchedulerMain using: " + Arrays.toString(schedulerCmd));
      return false;
    }

    LOG.log(Level.FINE, String.format(
        "To check the status and logs of the topology, use the working directory %s",
        LocalContext.workingDirectory(config)));

    return true;
  }

  ///////////////////////////////////////////////////////////////////////////////
  // Utils methods for unit tests
  ///////////////////////////////////////////////////////////////////////////////
  protected String[] getSchedulerCommand() {
    List<Integer> freePorts = new ArrayList<>(SchedulerUtils.PORTS_REQUIRED_FOR_SCHEDULER);
    for (int i = 0; i < SchedulerUtils.PORTS_REQUIRED_FOR_SCHEDULER; i++) {
      freePorts.add(SysUtils.getFreePort());
    }

    return SchedulerUtils.schedulerCommand(config, runtime, freePorts);
  }

  protected boolean setupWorkingDirectory() {
    // get the path of core release URI
    String coreReleasePackageURI = LocalContext.corePackageUri(config);

    // form the target dest core release file name
    String coreReleaseFileDestination = Paths.get(
        topologyWorkingDirectory, "heron-core.tar.gz").toString();

    // Form the topology package's URI
    String topologyPackageURI = Runtime.topologyPackageUri(runtime).toString();

    // form the target topology package file name
    String topologyPackageDestination = Paths.get(
        topologyWorkingDirectory, "topology.tar.gz").toString();

    return SchedulerUtils.setupWorkingDirectory(
        topologyWorkingDirectory,
        coreReleasePackageURI,
        coreReleaseFileDestination,
        topologyPackageURI,
        topologyPackageDestination,
        Context.verbose(config));
  }

  protected Process startScheduler(String[] schedulerCmd) {
    return ShellUtils.runASyncProcess(LocalContext.verbose(config), schedulerCmd,
        new File(topologyWorkingDirectory));
  }
}
