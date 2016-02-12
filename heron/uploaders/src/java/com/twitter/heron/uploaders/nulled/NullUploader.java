package com.twitter.heron.uploaders.nulled;

import com.twitter.heron.spi.common.Context;
import com.twitter.heron.spi.newuploader.IUploader;

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
    return new Context(new Context.Builder());
  }

  @Override
  public boolean undo() {
    return true;
  }

  @Override
  public void cleanup() {
  }
}

