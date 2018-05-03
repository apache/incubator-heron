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

package org.apache.heron.uploader.localfs;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.CopyOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.logging.Logger;

import org.apache.heron.common.basics.TypeUtils;
import org.apache.heron.spi.common.Config;
import org.apache.heron.spi.common.Context;
import org.apache.heron.spi.uploader.IUploader;
import org.apache.heron.spi.uploader.UploaderException;
import org.apache.heron.spi.utils.UploaderUtils;

public class LocalFileSystemUploader implements IUploader {
  private static final Logger LOG = Logger.getLogger(LocalFileSystemUploader.class.getName());

  private Config config;
  private String destTopologyFile;
  private String destTopologyDirectory;
  private String topologyPackageLocation;

  @Override
  public void initialize(Config ipconfig) {
    this.config = ipconfig;

    this.destTopologyDirectory = LocalFileSystemContext.getFileSystemDirectory(config);

    // name of the destination file is the same as the base name of the topology package file
    String fileName =
        UploaderUtils.generateFilename(
            Context.topologyName(config), Context.role(config));
    this.destTopologyFile = Paths.get(destTopologyDirectory, fileName).toString();

    // get the original topology package location
    this.topologyPackageLocation = LocalFileSystemContext.topologyPackageFile(config);
  }

  protected URI getUri(String filename) {
    StringBuilder sb = new StringBuilder()
        .append("file://")
        .append(filename);

    return TypeUtils.getURI(sb.toString());
  }

  /**
   * Upload the topology package to the destined location in local file system
   *
   * @return destination URI of where the topology package has
   * been uploaded if successful, or {@code null} if failed.
   */
  @Override
  public URI uploadPackage() throws UploaderException {
    // first, check if the topology package exists
    boolean fileExists = new File(topologyPackageLocation).isFile();
    if (!fileExists) {
      throw new UploaderException(
          String.format("Topology package does not exist at '%s'", topologyPackageLocation));
    }

    // get the directory containing the file
    Path filePath = Paths.get(destTopologyFile);
    File parentDirectory = filePath.getParent().toFile();
    assert parentDirectory != null;

    // if the dest directory does not exist, create it.
    if (!parentDirectory.exists()) {
      LOG.fine(String.format(
          "Working directory does not exist. Creating it now at %s", parentDirectory.getPath()));
      if (!parentDirectory.mkdirs()) {
        throw new UploaderException(
            String.format("Failed to create directory for topology package at %s",
                parentDirectory.getPath()));
      }
    }

    // if the dest file exists, write a log message
    fileExists = new File(filePath.toString()).isFile();
    if (fileExists) {
      LOG.fine(String.format("Target topology package already exists at '%s'. Overwriting it now",
          filePath.toString()));
    }

    // copy the topology package to target working directory
    LOG.fine(String.format("Copying topology package at '%s' to target working directory '%s'",
        topologyPackageLocation, filePath.toString()));

    Path source = Paths.get(topologyPackageLocation);
    try {
      CopyOption[] options = new CopyOption[]{StandardCopyOption.REPLACE_EXISTING};
      Files.copy(source, filePath, options);
    } catch (IOException e) {
      throw new UploaderException(
            String.format("Unable to copy topology file from '%s' to '%s'",
                source, filePath), e);
    }

    return getUri(destTopologyFile);
  }

  /**
   * Remove the uploaded topology package for cleaning up
   *
   * @return true, if successful
   */
  @Override
  public boolean undo() {
    if (destTopologyFile != null) {
      LOG.info("Clean uploaded jar");
      File file = new File(destTopologyFile);
      return file.delete();
    }
    return true;
  }

  @Override
  public void close() {
  }

  /**
   * Used for unit testing. Get the topology directory where the package
   * is uploaded.
   *
   * @return topology directory
   */
  protected String getTopologyDirectory() {
    return this.destTopologyDirectory;
  }

  /**
   * Used for unit testing. Get the topology package file
   *
   * @return topology file
   */
  protected String getTopologyFile() {
    return this.destTopologyFile;
  }
}
