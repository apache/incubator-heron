package com.twitter.heron.spi.uploader;

import java.net.URI;

import com.twitter.heron.spi.common.Config;

/**
 * Uploads topology package to a shared location. This location must be
 * accessible by runtime environment of topology. The uploader will upload
 * <p/>
 * - topology jar,
 * - topology jar dependencies,
 * - topology definition, and
 * - heron core packages and libraries, if required
 * <p/>
 * Uploader outputs another context containing the necessary information that
 * will be used by next stages of topology submission.
 * <p/>
 * Implementation of IUploader is required to have a no argument constructor
 * that will be called to create an instance of IUploader.
 */
public interface IUploader extends AutoCloseable {
  /**
   * Initialize the uploader with the incoming context.
   */
  void initialize(Config config);

  /**
   * UploadPackage will upload the topology package to the given location.
   *
   * @return destination URI of where the topology package has
   * been uploaded if successful, or {@code null} if failed.
   */
  URI uploadPackage();

  /**
   * If subsequent stages fail, undo will be called to free resources used by
   * uploading package. Ideally, this should try to remove the uploaded package.
   */
  boolean undo();

  /**
   * This is to for disposing or cleaning up any internal state accumulated by
   * the uploader
   * <p/>
   * Closes this stream and releases any system resources associated
   * with it. If the stream is already closed then invoking this
   * method has no effect.
   */
  void close();
}
