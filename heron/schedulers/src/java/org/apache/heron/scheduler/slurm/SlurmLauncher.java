/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.heron.scheduler.slurm;

import java.nio.file.Paths;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.heron.scheduler.utils.LauncherUtils;
import org.apache.heron.scheduler.utils.Runtime;
import org.apache.heron.scheduler.utils.SchedulerUtils;
import org.apache.heron.spi.common.Config;
import org.apache.heron.spi.common.Context;
import org.apache.heron.spi.packing.PackingPlan;
import org.apache.heron.spi.scheduler.ILauncher;

/**
 * Launches the Slurm Scheduler. The launcher assumes a shared directory between the nodes.
 * It first copies the heron distribution to the shared location every node can access.
 * Then it launches the scheduler. The scheduler runs the heron-executor on each of the
 * allocated nodes to start the topology components. The scheduler uses a node as a container.
 */
public class SlurmLauncher implements ILauncher {
  private static final Logger LOG = Logger.getLogger(SlurmLauncher.class.getName());

  private Config config;
  private Config runtime;

  private String topologyWorkingDirectory;

  @Override
  public void initialize(Config mConfig, Config mRuntime) {
    this.config = mConfig;
    this.runtime = mRuntime;

    // get the topology working directory
    this.topologyWorkingDirectory = SlurmContext.workingDirectory(mConfig);
  }

  @Override
  public void close() {

  }

  @Override
  public boolean launch(PackingPlan packing) {
    LOG.log(Level.FINE, "Launching topology for local cluster {0}",
        SlurmContext.cluster(config));

    // download the core and topology packages into the working directory
    // this working directory is a shared directory among the nodes
    if (!setupWorkingDirectory()) {
      LOG.log(Level.SEVERE, "Failed to download the core and topology packages");
      return false;
    }

    LauncherUtils launcherUtils = LauncherUtils.getInstance();
    Config ytruntime = launcherUtils.createConfigWithPackingDetails(runtime, packing);
    return launcherUtils.onScheduleAsLibrary(config, ytruntime,
        new SlurmScheduler(topologyWorkingDirectory), packing);
  }

  /**
   * setup the working directory mainly it downloads and extracts the heron-core-release
   * and topology package to the working directory
   * @return false if setup fails
   */
  protected boolean setupWorkingDirectory() {
    // get the path of core release URI
    String coreReleasePackageURI = SlurmContext.corePackageUri(config);

    // form the target dest core release file name
    String coreReleaseFileDestination = Paths.get(
        topologyWorkingDirectory, "heron-core.tar.gz").toString();

    // Form the topology package's URI
    String topologyPackageURI = Runtime.topologyPackageUri(runtime).toString();

    // form the target topology package file name
    String topologyPackageDestination = Paths.get(
        topologyWorkingDirectory, "topology.tar.gz").toString();

    if (!SchedulerUtils.createOrCleanDirectory(topologyWorkingDirectory)) {
      return false;
    }

    final boolean isVerbose = Context.verbose(config);
    if (!SchedulerUtils.extractPackage(topologyWorkingDirectory, coreReleasePackageURI,
        coreReleaseFileDestination, true, isVerbose)) {
      return false;
    }

    return SchedulerUtils.extractPackage(topologyWorkingDirectory, topologyPackageURI,
        topologyPackageDestination, true, isVerbose);
  }
}
