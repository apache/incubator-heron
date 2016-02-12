package com.twitter.heron.uploaders.packer;

import java.util.logging.Logger;

import com.twitter.heron.spi.common.Keys;
import com.twitter.heron.spi.common.Context;
import com.twitter.heron.spi.newuploader.IUploader;
import com.twitter.heron.spi.utils.ShellUtility;

/**
 * A base class for all Packer based Uploaders.
 */
public class PackerUploader implements IUploader {
  private static final Logger LOG = Logger.getLogger(PackerUploader.class.getName());

  // Note: all protected fields should be initialized in the initialize method
  protected String cluster;
  protected String role;
  protected String topology;
  protected String topologyURI;
  protected String releaseTag;

  protected boolean verbose;

  public int runProcess(String cmdline, StringBuilder stdout) {
    return ShellUtility.runProcess(verbose, cmdline, stdout, null);
  }

  protected String getTopologyURI(String jsonStr) {
    return PackerUtility.getURIFromPackerResponse(jsonStr);
  }

  @Override
  public void initialize(Context context) {
    this.cluster = context.getStringValue(Keys.Config.CLUSTER);
    this.role = context.getStringValue(Keys.Config.ROLE);
    this.topology = context.getStringValue(Keys.Config.TOPOLOGY_NAME);
    this.verbose = context.getBooleanValue(Keys.Config.VERBOSE);
    this.topologyURI = null;
    this.releaseTag = context.getStringValue(Keys.Runtime.HERON_RELEASE_PACKAGE_NAME, "live");

    if (cluster.isEmpty() || role.isEmpty()) {
      LOG.severe("cluster, role & env not set properly");
      throw new RuntimeException("Bad config");
    }
    if (runProcess("which packer", null) != 0) {
      throw new RuntimeException("Packer is not installed");
    }
  }

  public String getTopologyPackageName() {
    return PackerUtility.getTopologyPackageName(topology, releaseTag);
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
      topologyURI = getTopologyURI(jsonStr);
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
  public Context getContext() {
    Context.Builder builder = new Context.Builder().setStringValue(Keys.Runtime.TOPOLOGY_PKG_URI, topologyURI);
    return builder.build();
  }

  @Override
  public boolean undo() {
    LOG.info("Deleting package " + getTopologyPackageName());
    if (!unsetLivePackage(getTopologyPackageName()))
      return false;
    if (!deletePackage(getTopologyPackageName(), "latest"))
      return false;;
    return true;
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
 
  @Override
  public void cleanup() {
  }
}
