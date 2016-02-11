package com.twitter.heron.spi.uploader;

import com.twitter.heron.spi.common.Context;

/**
 * Uploads topology package to a shared location. This location must be
 * accessible by runtime environment of topology. The uploader will upload
 *
 *   - topology jar,
 *   - topology jar dependencies,
 *   - topology definition, and
 *   - heron core packages and libraries, if required
 *
 * Uploader outputs another context containing the necessary information that
 * will be used by Launcher to launch topology.
 *
 * Launcher will get this Uploader object also. Location passed to uploader will
 * be provided by heron-cli.
 *
 * Implementation of IUploader is required to have a no argument constructor
 * that will be called to create an instance of IUploader.
 *
 */
public interface IUploader {
  /**
   * Initialize the uploader with the incoming context.
   */
  void initialize(Context context);

  /**
   * UploadPackage will upload the topology package to the given location.
   * This is used by heron cli with the required parameters.
   *
   * @param topologyPackageLocation Location of topology jar and dependencies
   * as 1 file.
   * @return true if successful.
   */
   boolean uploadPackage(String topologyPackageLocation);

  /**
   * Get the return context of the uploader
   *
   * @return Context
   */
  Context getContext();

  /**
   * If subsequent stages fail, undo will be called to free resources used by
   * uploading package. Ideally, this should try to remove the uploaded package.
   */
   boolean undo();

  /**
   * This is to for disposing or cleaning up any internal state accumulated by
   * the uploader
   */
  void cleanup();
}
