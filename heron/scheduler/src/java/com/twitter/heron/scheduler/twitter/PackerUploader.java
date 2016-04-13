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

package com.twitter.heron.scheduler.twitter;

import java.util.logging.Logger;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.spi.common.Constants;
import com.twitter.heron.spi.uploader.IUploader;
import com.twitter.heron.spi.scheduler.context.LaunchContext;
import com.twitter.heron.scheduler.util.ShellUtility;

/**
 * A base class for all Packer based Uploaders.
 */
public class PackerUploader implements IUploader {
  private static final Logger LOG = Logger.getLogger(PackerUploader.class.getName());

  // Note: all protected fields should be initialized in the initialize method
  protected String cluster;
  protected String role;
  protected TopologyAPI.Topology topology;

  private LaunchContext context;

  public int runProcess(
      String cmdline, StringBuilder stdout) {
    return ShellUtility.runProcess(context.isVerbose(), cmdline, stdout, null);
  }

  protected String getTopologyURI(String jsonStr) {
    return PackerUtility.getURIFromPackerResponse(jsonStr);
  }

  @Override
  public void initialize(LaunchContext context) {
    this.topology = context.getTopology();
    this.context = context;
    this.cluster = context.getProperty(Constants.CLUSTER);
    this.role = context.getProperty(Constants.ROLE);

    if (cluster.isEmpty() || role.isEmpty()) {
      LOG.severe("cluster role & env not set properly");
      throw new RuntimeException("Bad config");
    }
    if (runProcess("which packer", null) != 0) {
      throw new RuntimeException("Packer is not installed");
    }
  }

  public String getTopologyPackageName() {
    String topologyName = topology.getName();

    String releaseTag =
        context.getProperty(Constants.HERON_RELEASE_PACKAGE_NAME, "live");
    return PackerUtility.getTopologyPackageName(topologyName, releaseTag);
  }

  @Override
  public boolean uploadPackage(String topologyPackageLocation) {
    LOG.info("Uploading packer package " + getTopologyPackageName());
    String packerUploadCmd = String.format(
        "packer add_version --cluster %s %s %s %s --json",
        cluster, role, getTopologyPackageName(), topologyPackageLocation);
    StringBuilder jsonStrBuilder = new StringBuilder();

    if (0 != runProcess(packerUploadCmd, jsonStrBuilder)) {
      LOG.severe("Failed to upload package to packer. Cmd: " + packerUploadCmd);
      return false;
    } else {
      String jsonStr = jsonStrBuilder.toString();

      // Add back into the context property
      String topologyURI = getTopologyURI(jsonStr);
      context.addProperty(Constants.TOPOLOGY_PKG_URI, topologyURI);
    }

    String packerLiveCmd = String.format(
        "packer set_live --cluster %s %s %s latest", cluster, role, getTopologyPackageName());
    LOG.info("Setting latest package to live");
    if (0 != runProcess(packerLiveCmd, null)) {
      LOG.severe("Failed to set latest package live. Cmd: " + packerLiveCmd);
      return false;
    }

    return true;
  }

  @Override
  public void undo() {
    LOG.info("Deleting package " + getTopologyPackageName());
    unsetLivePackage(getTopologyPackageName());
    deletePackage(getTopologyPackageName(), "latest");
  }

  private boolean deletePackage(String packageName, String version) {
    String deletePkgCmd = String.format(
        "packer delete_version --cluster %s %s %s %s", cluster, role, packageName, version);
    if (0 != runProcess(deletePkgCmd, null)) {
      LOG.severe("Failed to delete package " + packageName);
      return false;
    }
    return true;
  }

  private boolean unsetLivePackage(String packageName) {
    String deletePkgCmd = String.format(
        "packer unset_live --cluster %s %s %s", cluster, role, packageName);
    if (0 != runProcess(deletePkgCmd, null)) {
      LOG.severe("Failed to unset live package " + packageName);
      return false;
    }
    return true;
  }
}
