package com.twitter.heron.spi.uploader;

import com.twitter.heron.spi.common.Config;

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
 * will be used by next stages of topology submission.
 *
 * Implementation of IUploader is required to have a no argument constructor
 * that will be called to create an instance of IUploader.
 *
 */
public interface IUploader {
  /**
   * Initialize the uploader with the incoming context.
   */
  public void initialize(Config config);

  /**
   * UploadPackage will upload the topology package to the given location.
   *
   * @return true if successful.
   */
  public boolean uploadPackage();

  /**
   * getUri will get the destination URI of where the topology package has 
   * been uploaded
   *
   * @return uri
   */
  public String getUri();

  /**
   * If subsequent stages fail, undo will be called to free resources used by
   * uploading package. Ideally, this should try to remove the uploaded package.
   */
  public boolean undo();

  /**
   * This is to for disposing or cleaning up any internal state accumulated by
   * the uploader
   */
  public void close();
}
