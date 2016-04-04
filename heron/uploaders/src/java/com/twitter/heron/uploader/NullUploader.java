package com.twitter.heron.uploader;

import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.uploader.IUploader;

public class NullUploader implements IUploader {

  @Override
  public void initialize(Config config) {
  }

  @Override
  public Object uploadPackage() {
    return "NOT_NULL";
  }

  @Override
  public boolean undo() {
    return true;
  }

  @Override
  public void close() {
  }
}
