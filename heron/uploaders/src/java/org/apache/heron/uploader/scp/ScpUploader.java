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

package org.apache.heron.uploader.scp;

import java.io.File;
import java.net.URI;
import java.nio.file.Paths;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.heron.common.basics.TypeUtils;
import org.apache.heron.spi.common.Config;
import org.apache.heron.spi.common.Context;
import org.apache.heron.spi.uploader.IUploader;
import org.apache.heron.spi.uploader.UploaderException;
import org.apache.heron.spi.utils.UploaderUtils;

/**
 * Uploader for uploading topology packages to the file system of a machine in the cluster using
 * the scp command.
 * <p>
 * This uploader can be used to upload the topologies to a shared machine in the cluster. Then scp
 * command can be used to fetch the packages from this location. In case of a failure,
 * it will delete the topology copied to the share location.
 * </p>
 * The config values for this uploader are:
 * <ul>
 * <li>heron.class.uploader:  uploader class for transferring the topology jar/tar files to storage
 * <li>heron.uploader.scp.command.options:   This is the first part of the scp command used by the
 * uploader. This has to be customized to reflect the user name, hostname and ssh keys if required.
 * <li>heron.uploader.ssh.command.options:   The ssh command that will be used to connect to
 * the uploading host to execute command such as delete files, make directories
 * <li>heron.uploader.scp.dir.path:  The directory where the file will be uploaded, make sure
 * the user has the necessary permissions to upload the file here.
 * </ul>
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
    String scpOptions = ScpContext.scpOptions(config);
    String scpConnection = ScpContext.scpConnection(config);
    String sshOptions = ScpContext.sshOptions(config);
    String sshConnection = ScpContext.sshConnection(config);

    if (scpOptions == null) {
      throw new RuntimeException("Missing "
          + ScpContext.HERON_UPLOADER_SCP_OPTIONS + " config value");
    }
    if (scpConnection == null) {
      throw new RuntimeException("Missing "
          + ScpContext.HERON_UPLOADER_SCP_CONNECTION + " config value");
    }

    if (sshOptions == null) {
      throw new RuntimeException("Missing "
          + ScpContext.HERON_UPLOADER_SSH_OPTIONS + " config value");
    }
    if (sshConnection == null) {
      throw new RuntimeException("Missing "
          + ScpContext.HERON_UPLOADER_SSH_CONNECTION + " config value");
    }

    return new ScpController(
        scpOptions, scpConnection, sshOptions, sshConnection, Context.verbose(config));
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
  public URI uploadPackage() throws UploaderException {
    // first, check if the topology package exists
    boolean fileExists = isLocalFileExists(topologyPackageLocation);
    if (!fileExists) {
      throw new UploaderException(
          String.format("Topology file %s does not exist.", topologyPackageLocation));
    }

    // create the upload directory, if not exists
    if (!this.controller.mkdirsIfNotExists(destTopologyDirectory)) {
      throw new UploaderException(
          String.format(
              "Failed to create directories required for uploading the topology %s",
              destTopologyDirectory));
    }

    // now copy the file
    if (!this.controller.copyFromLocalFile(topologyPackageLocation, destTopologyFile)) {
      throw new UploaderException(
          String.format(
              "Failed to upload the file from local file system to remote machine: %s -> %s.",
              topologyPackageLocation, destTopologyDirectory));
    }

    LOG.log(Level.INFO, "Package URL to download: {}", packageURI.toString());
    return packageURI;
  }

  // Utils method
  protected boolean isLocalFileExists(String file) {
    return new File(file).isFile();
  }

  @Override
  public boolean undo() {
    return this.controller.delete(destTopologyFile);
  }

  @Override
  public void close() {
  }
}
