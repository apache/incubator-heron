package com.twitter.heron.scheduler.api;

import com.twitter.heron.scheduler.api.context.LaunchContext;

/**
 * Uploads topology package to a shared location. This location must be accessible by
 * runtime environment of topology. The uploader will upload topology jar, topology jar
 * dependencies, topology definition and if required heron core packages and libraries.
 * Uploader outputs string containing all information that will be used by Launcher to
 * launch topology. Launcher will get this Uploader object also.
 * Location passed to uploader will be generated from heron-cli.
 * Implementation of IUploader are required to have a no-argument constructor which will be called
 * to create IUploader object,
 */
public interface IUploader {
  /**
   * Initialize Uploader with a config file. heron-cli will pass config file to initialize uploader.
   */
  void initialize(LaunchContext context);

  /**
   * Will be called by heron-cli with relevant parameters.
   *
   * @param topologyPackageLocation Location of topology jar and dependencies as 1 file.
   * @return true if successful.
   */
  boolean uploadPackage(final String topologyPackageLocation);

  /**
   * If subsequent stages failed, this will be called to free resources used by uploading package.
   * This will try to cleanup uploaded package.
   */
  void undo();
}
