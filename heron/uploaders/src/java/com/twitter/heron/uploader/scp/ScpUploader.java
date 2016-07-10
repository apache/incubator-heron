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

package com.twitter.heron.uploader.scp;

import java.io.File;
import java.net.URI;
import java.nio.file.Paths;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.twitter.heron.common.basics.TypeUtils;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Context;
import com.twitter.heron.spi.uploader.IUploader;
import com.twitter.heron.spi.utils.UploaderUtils;

/**
 * Uploader for uploading topology packages to a common location using the scp command.
 */
public class ScpUploader implements IUploader {
  private static final Logger LOG = Logger.getLogger(ScpUploader.class.getName());
  // get the directory containing the file
  private String destTopologyDirectory;
  private Config config;
  private String topologyPackageLocation;
  private String destTopologyFile;
  private URI packageURI;

  private ScpController controller;

  // Utils method
  protected ScpController getScpController() {
    return new ScpController(
        ScpContext.scpCommand(config), ScpContext.sshCommand(config), Context.verbose(config));
  }

  @Override
  public void initialize(Config ipconfig) {
    this.config = ipconfig;

    // Instantiate the scp controller
    this.controller = getScpController();

    // get the destination directory
    this.destTopologyDirectory = ScpContext.uploadDirPath(config);
    // get the original topology package location
    this.topologyPackageLocation = Context.topologyPackageFile(config);

    // name of the destination file is the same as the base name of the topology package file
    String fileName =
        UploaderUtils.generateFilename(
            Context.topologyName(config), Context.role(config));
    this.destTopologyFile = Paths.get(destTopologyDirectory, fileName).toString();
    packageURI = TypeUtils.getURI(String.format("%s/%s", destTopologyDirectory, fileName));
  }

  @Override
  public URI uploadPackage() {
    // first, check if the topology package exists
    boolean fileExists = new File(topologyPackageLocation).isFile();
    if (!fileExists) {
      LOG.log(Level.SEVERE, "Topology file {0} does not exist.", topologyPackageLocation);
      return null;
    }

    // create the upload directory, if not exists
    if (!this.controller.mkdirsIfNotExists(destTopologyDirectory)) {
      LOG.log(Level.SEVERE, "Failed to create directories requried for uploading the topology {0}.",
          destTopologyDirectory);
      return null;
    }

    // now copy the file
    if (!this.controller.copyFromLocalFile(topologyPackageLocation, destTopologyFile)) {
      LOG.log(Level.SEVERE, "Failed to upload the file from local file system to remote machine " +
          "{0} -> {1}.", new String[]{topologyPackageLocation, destTopologyDirectory});
      return null;
    }

    LOG.log(Level.INFO, "Package URL to download: {}", packageURI.toString());
    return packageURI;
  }

  @Override
  public boolean undo() {
    return this.controller.delete(destTopologyFile);
  }

  @Override
  public void close() {
  }
}
