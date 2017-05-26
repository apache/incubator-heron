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

package com.twitter.heron.scheduler.kubernetes;

import java.nio.file.Paths;
import java.util.logging.Logger;

import com.twitter.heron.scheduler.utils.LauncherUtils;
import com.twitter.heron.scheduler.utils.SchedulerUtils;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Context;
import com.twitter.heron.spi.packing.PackingPlan;
import com.twitter.heron.spi.scheduler.ILauncher;
import com.twitter.heron.spi.scheduler.IScheduler;

/**
 * Submit topology to Marathon.
 */
public class KubernetesLauncher implements ILauncher {
  private static final Logger LOG = Logger.getLogger(KubernetesLauncher.class.getName());

  private Config config;
  private Config runtime;

  private String schedulerWorkingDirectory;

  @Override
  public void initialize(Config aConfig, Config aRuntime) {
    this.config = aConfig;
    this.runtime = aRuntime;

    // get the scheduler working directory
    this.schedulerWorkingDirectory = KubernetesContext.getSchedulerWorkingDirectory(config);
  }

  @Override
  public void close() {
    // Do nothing
  }

  @Override
  public boolean launch(PackingPlan packing) {

    // setup the working directory -- mainly just need to go and grab the topology jar
    if (!setupWorkingDirectory()) {
      LOG.severe("Failed to setup working directory");
      return false;
    }

    LauncherUtils launcherUtils = LauncherUtils.getInstance();
    Config ytruntime = launcherUtils.createConfigWithPackingDetails(runtime, packing);
    return launcherUtils.onScheduleAsLibrary(config, ytruntime, getScheduler(), packing);
  }

  // Get KubernetesScheduler
  protected IScheduler getScheduler() {
    return new KubernetesScheduler();
  }

  // Setup the working directory -- pull down the topology jar
  protected boolean setupWorkingDirectory() {
    String topologyPackageURL = String.format("file://%s", Context.topologyPackageFile(config));
    String topologyPackageDestination = Paths.get(
        schedulerWorkingDirectory, "topology.tar.gz").toString();

    return SchedulerUtils.curlAndExtractPackage(
        schedulerWorkingDirectory, topologyPackageURL,
        topologyPackageDestination, true, Context.verbose(config));
  }
}
