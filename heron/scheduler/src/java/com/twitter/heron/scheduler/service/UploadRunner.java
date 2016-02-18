package com.twitter.heron.scheduler.service;

import java.util.concurrent.Callable;

import com.twitter.heron.spi.uploader.IUploader;
import com.twitter.heron.spi.scheduler.context.LaunchContext;

/**
 * Runs uploader.
 */
public class UploadRunner implements Callable<Boolean> {
  private IUploader uploader;
  private LaunchContext context;
  private String topologyPackage;

  public UploadRunner(IUploader uploader,
                      LaunchContext context,
                      String topologyPackage) {
    this.uploader = uploader;
    this.context = context;
    this.topologyPackage = topologyPackage;
  }

  public Boolean call() {
    uploader.initialize(context);
    return uploader.uploadPackage(topologyPackage);
  }
}
