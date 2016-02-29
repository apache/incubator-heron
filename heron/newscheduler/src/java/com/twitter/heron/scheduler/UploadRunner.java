package com.twitter.heron.scheduler;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.lang.ClassNotFoundException;

import com.twitter.heron.spi.common.Keys;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Context;
import com.twitter.heron.spi.uploader.IUploader;

/**
 * Runs uploader.
 */
public class UploadRunner implements Callable<Boolean> {

  // instance of the uploader class 
  private IUploader uploader;

  public UploadRunner(Config config) throws 
      ClassNotFoundException, InstantiationException, IllegalAccessException, IOException {

    // create an instance of the uploader class
    String uploaderClass = Context.uploaderClass(config);
    this.uploader = (IUploader)Class.forName(uploaderClass).newInstance();

    // initialize the uploader with config provided
    this.uploader.initialize(config);
  }

  /**
   * Call the upload package method to upload the topology package
   *
   * @return true, if successful
   */
  public Boolean call() {
    return uploader.uploadPackage();
  }

  /**
   * Get the URI of the location where the topology package has been uploaded
   *
   * @return uri
   */
  public String getUri() {
    return uploader.getUri();
  }

  /**
   * Undo the uploaded topology package, essentially it deletes the topology package
   * where it was uploaded 
   *
   * @return true, if successful
   */
  public Boolean undo() {
    return uploader.undo();
  }
}
