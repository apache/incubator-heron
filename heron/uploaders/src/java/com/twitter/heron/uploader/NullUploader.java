package com.twitter.heron.uploader;

import java.net.URI;

import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Convert;
import com.twitter.heron.spi.uploader.IUploader;

public class NullUploader implements IUploader {

  @Override
  public void initialize(Config config) {
  }

  @Override
  public URI uploadPackage() {
    // Construct a URI from valid syntax String
    return Convert.getURI("null://uploader:9519/w#lanfang");
  }

  @Override
  public boolean undo() {
    return true;
  }

  @Override
  public void close() {
  }
}
