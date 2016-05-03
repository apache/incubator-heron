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
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.io.FileUtils;

import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Context;
import com.twitter.heron.spi.common.PackingPlan;
import com.twitter.heron.spi.common.ShellUtils;
import com.twitter.heron.spi.scheduler.ILauncher;
import com.twitter.heron.spi.statemgr.SchedulerStateManagerAdaptor;
import com.twitter.heron.spi.utils.NetworkUtils;
import com.twitter.heron.spi.utils.Runtime;
import com.twitter.heron.spi.utils.SchedulerUtils;

/**
 * Launch topology locally to a working directory.
 */
public class LocalLauncher implements ILauncher {
  protected static final Logger LOG = Logger.getLogger(LocalLauncher.class.getName());

  private Config config;
  private Config runtime;

  private String topologyWorkingDirectory;
  private String coreReleasePackage;
  private String targetCoreReleaseFile;
  private String targetTopologyPackageFile;

  @Override
  public void initialize(Config mConfig, Config mRuntime) {

    this.config = mConfig;
    this.runtime = mRuntime;

    // get the topology working directory
    this.topologyWorkingDirectory = LocalContext.workingDirectory(mConfig);

    // get the path of core release URI
    this.coreReleasePackage = LocalContext.corePackageUri(mConfig);

    // form the target dest core release file name
    this.targetCoreReleaseFile = Paths.get(
        topologyWorkingDirectory, "heron-core.tar.gz").toString();

    // form the target topology package file name
    this.targetTopologyPackageFile = Paths.get(
        topologyWorkingDirectory, "topology.tar.gz").toString();
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

    // download the core and topology packages into the working directory
    if (!downloadAndExtractPackages()) {
      LOG.severe("Failed to download the core and topology packages");
      return false;
    }

    String[] schedulerCmd = getSchedulerCommand();

    Process p = startScheduler(schedulerCmd);

    if (p == null) {
      LOG.severe("Failed to start SchedulerMain using: " + Arrays.toString(schedulerCmd));
      return false;
    }

    LOG.info(String.format(
        "For checking the status and logs of the topology, use the working directory %s",
        LocalContext.workingDirectory(config)));

    return true;
  }

  /**
   * Download heron core and the topology packages into topology working directory
   *
   * @return true if successful
   */
  protected boolean downloadAndExtractPackages() {

    // log the state manager being used, for visibility and debugging purposes
    SchedulerStateManagerAdaptor stateManager = Runtime.schedulerStateManagerAdaptor(runtime);
    LOG.log(Level.FINE, "State manager used: {0} ", stateManager.getClass().getName());

    // if the working directory does not exist, create it.
    File workingDirectory = new File(topologyWorkingDirectory);
    if (!workingDirectory.exists()) {
      LOG.fine("The working directory does not exist; creating it.");
      if (!workingDirectory.mkdirs()) {
        LOG.severe("Failed to create directory: " + workingDirectory.getPath());
        return false;
      }
    }

    // copy the heron core release package to the working directory and untar it
    LOG.log(Level.FINE, "Fetching heron core release {0}", coreReleasePackage);
    LOG.fine("If release package is already in the working directory");
    LOG.fine("the old one will be overwritten");
    if (!curlPackage(coreReleasePackage, targetCoreReleaseFile)) {
      LOG.severe("Failed to fetch the heron core release package.");
      return false;
    }

    // untar the heron core release package in the working directory
    LOG.log(Level.FINE, "Untar the heron core release {0}", coreReleasePackage);
    if (!untarPackage(targetCoreReleaseFile, topologyWorkingDirectory)) {
      LOG.severe("Failed to untar heron core release package.");
      return false;
    }

    // remove the core release package
    if (!FileUtils.deleteQuietly(new File(targetCoreReleaseFile))) {
      LOG.warning("Unable to delete the core release file: " + targetCoreReleaseFile);
    }

    // give warning for overwriting existing topology package
    String topologyPackage = Runtime.topologyPackageUri(runtime).toString();
    LOG.log(Level.FINE, "Fetching topology package {0}", Runtime.topologyPackageUri(runtime));
    LOG.fine("If topology package is already in the working directory");
    LOG.fine("the old one will be overwritten");

    // fetch the topology package
    if (!curlPackage(topologyPackage, targetTopologyPackageFile)) {
      LOG.severe("Failed to fetch the heron core release package.");
      return false;
    }

    // untar the topology package
    LOG.log(Level.FINE, "Untar the topology package: {0}", topologyPackage);

    if (!untarPackage(targetTopologyPackageFile, topologyWorkingDirectory)) {
      LOG.severe("Failed to untar topology package.");
      return false;
    }

    // remove the topology package
    if (!FileUtils.deleteQuietly(new File(targetTopologyPackageFile))) {
      LOG.warning("Unable to delete the core release file: " + targetTopologyPackageFile);
    }

    return true;
  }

  /**
   * Copy a URL package to a target folder
   *
   * @param corePackageURI the URI to download core release package
   * @param targetFile the target filename to download the release package to
   * @return true if successful
   */
  protected boolean curlPackage(String corePackageURI, String targetFile) {

    // get the directory containing the target file
    Path filePath = Paths.get(targetFile);
    File parentDirectory = filePath.getParent().toFile();

    // using curl copy the url to the target file
    String cmd = String.format("curl %s -o %s", corePackageURI, targetFile);
    int ret = ShellUtils.runSyncProcess(LocalContext.verbose(config), LocalContext.verbose(config),
        cmd, new StringBuilder(), new StringBuilder(), parentDirectory);

    return ret == 0 ? true : false;
  }

  /**
   * Untar a tar package to a target folder
   *
   * @param packageName the tar package
   * @param targetFolder the target folder
   * @return true if untar successfully
   */
  protected boolean untarPackage(String packageName, String targetFolder) {
    String cmd = String.format("tar -xvf %s", packageName);

    int ret = ShellUtils.runSyncProcess(LocalContext.verbose(config), LocalContext.verbose(config),
        cmd, new StringBuilder(), new StringBuilder(), new File(targetFolder));

    return ret == 0 ? true : false;
  }

  ///////////////////////////////////////////////////////////////////////////////
  // Utils methods for unit tests
  ///////////////////////////////////////////////////////////////////////////////
  protected String[] getSchedulerCommand() {
    String javaBinary = String.format("%s/%s", Context.javaHome(config), "bin/java");
    return SchedulerUtils.schedulerCommand(config, javaBinary, NetworkUtils.getFreePort());
  }

  protected Process startScheduler(String[] schedulerCmd) {
    return ShellUtils.runASyncProcess(LocalContext.verbose(config), schedulerCmd,
        new File(topologyWorkingDirectory));
  }
}
