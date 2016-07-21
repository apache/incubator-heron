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

package com.twitter.heron.uploader.hdfs;

import java.io.File;
import java.net.URI;
import java.util.logging.Logger;

import com.twitter.heron.common.basics.TypeUtils;
import com.twitter.heron.spi.common.Context;
import com.twitter.heron.spi.common.SpiCommonConfig;
import com.twitter.heron.spi.uploader.IUploader;
import com.twitter.heron.spi.utils.UploaderUtils;

public class HdfsUploader implements IUploader {
  private static final Logger LOG = Logger.getLogger(HdfsUploader.class.getName());
  // get the directory containing the file
  private String destTopologyDirectoryURI;
  private SpiCommonConfig config;
  private String topologyPackageLocation;
  private URI packageURI;

  // The controller on hdfs
  private HdfsController controller;

  @Override
  public void initialize(SpiCommonConfig ipconfig) {
    this.config = ipconfig;

    // Instantiate the hdfs controller
    this.controller = getHdfsController();

    this.destTopologyDirectoryURI = HdfsContext.hdfsTopologiesDirectoryURI(config);
    // get the original topology package location
    this.topologyPackageLocation = Context.topologyPackageFile(config);

    // name of the destination file is the same as the base name of the topology package file
    String fileName =
        UploaderUtils.generateFilename(
            Context.topologyName(config), Context.role(config));
    packageURI = TypeUtils.getURI(String.format("%s/%s", destTopologyDirectoryURI, fileName));
  }

  // Utils method
  protected HdfsController getHdfsController() {
    return new HdfsController(
        HdfsContext.hadoopConfigDirectory(config), Context.verbose(config));
  }

  // Utils method
  protected boolean isLocalFileExists(String file) {
    return new File(file).isFile();
  }

  @Override
  public URI uploadPackage() {
    // first, check if the topology package exists
    if (!isLocalFileExists(topologyPackageLocation)) {
      LOG.info("Topology file " + topologyPackageLocation + " does not exist.");
      return null;
    }

    // if the dest directory does not exist, create it.
    if (!controller.exists(destTopologyDirectoryURI)) {
      LOG.info("The destination directory does not exist; creating it.");
      if (!controller.mkdirs(destTopologyDirectoryURI)) {
        LOG.severe("Failed to create directory: " + destTopologyDirectoryURI);
        return null;
      }
    } else {
      // if the destination file exists, write a log message
      LOG.info("Target topology file " + packageURI.toString() + " exists, overwriting...");

    }

    // copy the topology package to target working directory
    LOG.info("Uploading topology " + topologyPackageLocation
        + " package to target hdfs " + packageURI.toString());

    if (!controller.copyFromLocalFile(topologyPackageLocation, packageURI.toString())) {
      LOG.severe("Failed to upload the package to:" + packageURI.toString());
      return null;
    }

    return packageURI;
  }

  @Override
  public boolean undo() {
    return controller.delete(packageURI.toString());
  }

  @Override
  public void close() {
    // Nothing to do here
  }
}
