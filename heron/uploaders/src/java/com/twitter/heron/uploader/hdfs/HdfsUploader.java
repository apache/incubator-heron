package com.twitter.heron.uploader.hdfs;

import java.io.File;
import java.net.URI;
import java.util.logging.Logger;

import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Context;
import com.twitter.heron.spi.common.Convert;
import com.twitter.heron.spi.uploader.IUploader;

public class HdfsUploader implements IUploader {
  private static final Logger LOG = Logger.getLogger(HdfsUploader.class.getName());

  private Config config;
  private String hadoopConfdir;
  // get the directory containing the file
  String destTopologyDirectoryURI;

  private String topologyPackageLocation;
  private URI packageURI;

  @Override
  public void initialize(Config config) {
    this.config = config;

    this.hadoopConfdir = HdfsContext.hadoopConfigDirectory(config);
    this.destTopologyDirectoryURI = HdfsContext.hdfsTopologiesDirectoryURI(config);
    // get the original topology package location
    this.topologyPackageLocation = Context.topologyPackageFile(config);

    // name of the destination file is the same as the base name of the topology package file
    String fileName = new File(topologyPackageLocation).getName();
    packageURI = Convert.getURI(String.format("%s/%s", destTopologyDirectoryURI, fileName));
  }

  @Override
  public URI uploadPackage() {
    // first, check if the topology package exists

    boolean fileExists = new File(topologyPackageLocation).isFile();
    if (!fileExists) {
      LOG.info("Topology file " + topologyPackageLocation + " does not exist.");
      return null;
    }

    // if the dest directory does not exist, create it.
    if (!HdfsUtils.isFileExists(hadoopConfdir, destTopologyDirectoryURI, true)) {
      LOG.info("The destination directory does not exist; creating it.");
      if (!HdfsUtils.createDir(hadoopConfdir, destTopologyDirectoryURI, true)) {
        LOG.severe("Failed to create directory: " + destTopologyDirectoryURI);
        return null;
      }
    }

    // if the destination file exists, write a log message
    if (HdfsUtils.isFileExists(hadoopConfdir, packageURI.toString(), true)) {
      LOG.info("Target topology file " + packageURI.toString() + " exists, overwriting...");
    }

    // copy the topology package to target working directory
    LOG.info("Uploading topology " + topologyPackageLocation +
        " package to target hdfs " + packageURI.toString());

    if (!HdfsUtils.copyFromLocal(
        hadoopConfdir, topologyPackageLocation, packageURI.toString(), true)) {
      LOG.severe("Failed to upload the package to:" + packageURI.toString());
      return null;
    }

    return packageURI;
  }

  @Override
  public boolean undo() {
    return HdfsUtils.remove(hadoopConfdir, packageURI.toString(), true);
  }

  @Override
  public void close() {
    // Nothing to do here
  }
}
