package com.twitter.heron.uploader.packer;

import java.net.URI;
import java.util.logging.Logger;

import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Context;
import com.twitter.heron.spi.common.Convert;
import com.twitter.heron.spi.common.ShellUtils;
import com.twitter.heron.spi.uploader.IUploader;

/**
 * A base class for all Packer based Uploaders.
 */
public class PackerUploader implements IUploader {
  private static final Logger LOG = Logger.getLogger(PackerUploader.class.getName());

  // Note: all protected fields should be initialized in the initialize method
  protected String cluster;
  protected String role;
  protected String topology;

  protected String releaseTag;
  private String topologyPackageLocation;

  protected boolean verbose;
  private Config config;

  public int runProcess(String cmdline, StringBuilder stdout) {
    return ShellUtils.runProcess(verbose, cmdline, stdout, null);
  }

  protected String getTopologyURI(String jsonStr) {
    return PackerUtils.getURIFromPackerResponse(jsonStr);
  }

  @Override
  public void initialize(Config config) {
    this.cluster = Context.cluster(config);
    this.role = Context.role(config);
    this.topology = Context.topologyName(config);
    this.verbose = Context.verbose(config);

    this.topologyPackageLocation = Context.topologyPackageFile(config);

    // TO DO
    // this.releaseTag = config.getStringValue(Keys.get("HERON_RELEASE_PACKAGE_NAME"), "live");

    // core pkg uri in form: packer://role/pkg/version, and we need the 'pkg' part
    this.releaseTag = Context.corePackageUri(config).split("/")[3];

    if (cluster.isEmpty() || role.isEmpty()) {
      LOG.severe("cluster, role & env not set properly");
      throw new RuntimeException("Bad config");
    }

    if (runProcess("which packer", null) != 0) {
      throw new RuntimeException("Packer is not installed");
    }
  }

  public String getTopologyPackageName() {
    return PackerUtils.getTopologyPackageName(topology, releaseTag);
  }

  @Override
  public URI uploadPackage() {
    LOG.info("Uploading packer package " + getTopologyPackageName());
    String packerUploadCmd = String.format(
        "packer add_version --cluster %s %s %s %s --json",
        cluster, role, getTopologyPackageName(), topologyPackageLocation);
    StringBuilder jsonStrBuilder = new StringBuilder();

    if (0 != runProcess(packerUploadCmd, jsonStrBuilder)) {
      LOG.severe("Failed to upload package to packer. Cmd: " + packerUploadCmd);
      return null;
    }

    String packerLiveCmd = String.format(
        "packer set_live --cluster %s %s %s latest", cluster, role, getTopologyPackageName());
    LOG.info("Setting latest package to live");
    if (0 != runProcess(packerLiveCmd, null)) {
      LOG.severe("Failed to set latest package live. Cmd: " + packerLiveCmd);
      return null;
    }

    String topologyURIStr = String.format("packer://%s/%s/%s", role, getTopologyPackageName(), "live");
    return Convert.getURI(topologyURIStr);
  }

  /*
  @Override
  public Config getConfig() {
    Config.Builder builder = new Config.Builder()
        .put(Keys.TOPOLOGY_PACKAGE_URI, topologyURI);
    return builder.build();
  }
  */

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
  public void close() {
  }
}
