package com.twitter.heron.uploaders;

import com.twitter.heron.spi.common.Context;
import com.twitter.heron.spi.uploader.IUploader;

public class NullUploader implements IUploader {

  @Override
  public void initialize(Context context) {
  }

  @Override
  public boolean uploadPackage(String topologyPackage) {
    return true;
  }

  @Override
  public Context getContext() {
    return Context.newBuilder().build();
  }

  @Override
  public boolean undo() {
    return true;
  }

  @Override
  public void cleanup() {
  }
}
