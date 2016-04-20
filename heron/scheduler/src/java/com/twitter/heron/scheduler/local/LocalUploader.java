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

import java.util.logging.Logger;

import com.twitter.heron.common.basics.FileUtils;
import com.twitter.heron.spi.scheduler.context.LaunchContext;
import com.twitter.heron.spi.uploader.IUploader;

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
    if (!FileUtils.isDirectoryExists(this.localConfig.getWorkingDirectory())) {
      LOG.info("The working directory does not exist; creating it.");
      if (!FileUtils.createDirectory(this.localConfig.getWorkingDirectory())) {
        LOG.severe("Failed to create directory: " + this.localConfig.getWorkingDirectory());
        return false;
      }
    }

    targetTopologyPackage = String.format("%s/%s",
        localConfig.getWorkingDirectory(), FileUtils.getBaseName(topologyPackageLocation));


    // 1. Copy the topology package to target working directory
    if (!FileUtils.copyFile(topologyPackageLocation, targetTopologyPackage)) {
      return false;
    }

    // We would not untar the topology here; we would untar in the LocalLauncher

    return true;
  }

  @Override
  public void undo() {
    // Clean the tmp working directory
    LOG.info("Clean uploaded jar");

    FileUtils.deleteFile(targetTopologyPackage);
  }
}
