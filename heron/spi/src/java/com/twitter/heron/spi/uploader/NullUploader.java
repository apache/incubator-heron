package com.twitter.heron.spi.uploader;

import com.twitter.heron.spi.scheduler.context.LaunchContext;

public class NullUploader implements IUploader {
  @Override
  public void initialize(LaunchContext context) {
  }

  @Override
  public boolean uploadPackage(String topologyPackage) {
    return true;
  }

  @Override
  public void undo() {
  }
}

