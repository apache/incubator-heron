package com.twitter.heron.scheduler.local;

import java.util.logging.Logger;

import com.twitter.heron.common.core.base.FileUtility;
import com.twitter.heron.spi.uploader.IUploader;
import com.twitter.heron.spi.scheduler.context.LaunchContext;

public class LocalUploader implements IUploader {
  private static final Logger LOG = Logger.getLogger(LocalUploader.class.getName());

  private LocalConfig localConfig;
  private String targetTopologyPackage;

  @Override

  public void initialize(LaunchContext context) {
    this.localConfig = new LocalConfig(context);

    if (this.localConfig.getWorkingDirectory() == null) {
      throw new RuntimeException("The working is not set");
    }

    if (this.localConfig.getHeronCoreReleasePackage() == null) {
      LOG.info("The heron core release package is not set; " +
          "supposing it is already in working directory");
    }
  }

  @Override
  public boolean uploadPackage(String topologyPackageLocation) {
    LOG.info("Copying topology package to target working directory: " +
        localConfig.getWorkingDirectory());

    // If the working directory does not exist, create it.
    if (!FileUtility.isDirectoryExists(this.localConfig.getWorkingDirectory())) {
      LOG.info("The working directory does not exist; creating it.");
      if (!FileUtility.createDirectory(this.localConfig.getWorkingDirectory())) {
        LOG.severe("Failed to create directory: " + this.localConfig.getWorkingDirectory());
        return false;
      }
    }

    targetTopologyPackage = String.format("%s/%s",
        localConfig.getWorkingDirectory(), FileUtility.getBaseName(topologyPackageLocation));


    // 1. Copy the topology package to target working directory
    if (!FileUtility.copyFile(topologyPackageLocation, targetTopologyPackage)) {
      return false;
    }

    // We would not untar the topology here; we would untar in the LocalLauncher

    return true;
  }

  @Override
  public void undo() {
    // Clean the tmp working directory
    LOG.info("Clean uploaded jar");

    FileUtility.deleteFile(targetTopologyPackage);
  }
}
