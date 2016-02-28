package com.twitter.heron.uploader.localfs;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.Files;
import java.nio.file.CopyOption;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.*;
import java.util.logging.Logger;

import com.twitter.heron.spi.common.Keys;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Context;
import com.twitter.heron.spi.common.ShellUtils;
import com.twitter.heron.spi.uploader.IUploader;

public class LocalFileSystemUploader implements IUploader {
  private static final Logger LOG = Logger.getLogger(LocalFileSystemUploader.class.getName());

  private Config config;
  private String destTopologyFile;
  private String destTopologyDirectory;
  private String topologyPackageLocation;

  @Override
  public void initialize(Config config) {
    this.config = config;
    
    this.destTopologyDirectory = LocalFileSystemContext.fileSystemDirectory(config);

    // name of the destination file is the same as the base name of the topology package file
    String fileName = new File(Context.topologyPackageFile(config)).getName();
    this.destTopologyFile = Paths.get(destTopologyDirectory, fileName).toString();

    // get the original topology package location
    this.topologyPackageLocation = Context.topologyPackageFile(config);
  }

  protected String getTopologyDirectory() {
    return this.destTopologyDirectory;
  }

  protected String getTopologyFile() {
    return this.destTopologyFile;
  }

  @Override
  public boolean uploadPackage() {

    // first, check if the topology package exists
    boolean fileExists = new File(topologyPackageLocation).isFile();
    if (!fileExists) {
      LOG.info("Topology file " + topologyPackageLocation + " does not exist.");
      return false;
    }

    // get the directory containing the file
    Path filePath = Paths.get(destTopologyFile);
    File parentDirectory = filePath.getParent().toFile();
    assert parentDirectory != null;

    // if the dest directory does not exist, create it.
    if (!parentDirectory.exists()) {
      LOG.info("The working directory does not exist; creating it.");
      if (!parentDirectory.mkdirs()) {
        LOG.severe("Failed to create directory: " + parentDirectory.getPath());
        return false;
      }
    }

    // if the dest file exists, write a log message
    fileExists = new File(filePath.toString()).isFile();
    if (fileExists) {
      LOG.info("Target topology file " + filePath.toString() + " exists, overwriting...");
    }

    // copy the topology package to target working directory
    LOG.info("Copying topology " + topologyPackageLocation + 
        " package to target working directory " + filePath.toString());

    Path source = Paths.get(topologyPackageLocation);
    try {
      CopyOption[] options = new CopyOption[] { StandardCopyOption.REPLACE_EXISTING };
      Files.copy(source, filePath, options);
    } catch (IOException ex) {
      LOG.info("Unable to copy: "  + source.toString() + " " + ex);
      return false; 
    }

    return true;
  }

  @Override
  public boolean undo() {
    LOG.info("Clean uploaded jar");
    File file = new File(destTopologyFile);
    return file.delete();
  }

  @Override
  public void close() {
  }
}
