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

package org.apache.heron.uploader.hdfs;

import java.io.File;
import java.net.URI;
import java.util.logging.Logger;

import org.apache.heron.common.basics.TypeUtils;
import org.apache.heron.spi.common.Config;
import org.apache.heron.spi.common.Context;
import org.apache.heron.spi.uploader.IUploader;
import org.apache.heron.spi.uploader.UploaderException;
import org.apache.heron.spi.utils.UploaderUtils;

public class HdfsUploader implements IUploader {
  private static final Logger LOG = Logger.getLogger(HdfsUploader.class.getName());
  // get the directory containing the file
  private String destTopologyDirectoryURI;
  private Config config;
  private String topologyPackageLocation;
  private URI packageURI;

  // The controller on HDFS
  private HdfsController controller;

  @Override
  public void initialize(Config ipconfig) {
    this.config = ipconfig;

    // Instantiate the HDFS controller
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
  public URI uploadPackage() throws UploaderException {
    // first, check if the topology package exists
    if (!isLocalFileExists(topologyPackageLocation)) {
      throw new UploaderException(
        String.format("Expected topology package file to be uploaded does not exist at '%s'",
            topologyPackageLocation));
    }

    // if the dest directory does not exist, create it.
    if (!controller.exists(destTopologyDirectoryURI)) {
      LOG.info(String.format(
          "The destination directory does not exist. Creating it now at URI '%s'",
          destTopologyDirectoryURI));
      if (!controller.mkdirs(destTopologyDirectoryURI)) {
        throw new UploaderException(
            String.format("Failed to create directory for topology package at URI '%s'",
                destTopologyDirectoryURI));
      }
    } else {
      // if the destination file exists, write a log message
      LOG.info(String.format("Target topology file already exists at '%s'. Overwriting it now",
          packageURI.toString()));
    }

    // copy the topology package to target working directory
    LOG.info(String.format("Uploading topology package at '%s' to target HDFS at '%s'",
        topologyPackageLocation, packageURI.toString()));

    if (!controller.copyFromLocalFile(topologyPackageLocation, packageURI.toString())) {
      throw new UploaderException(
          String.format("Failed to upload the topology package at '%s' to: '%s'",
              topologyPackageLocation, packageURI.toString()));
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
