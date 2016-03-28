package com.twitter.heron.uploader.localfs;

import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Context;

public class LocalFileSystemContext extends Context {
  public static String fileSystemDirectory(Config config) {
    return config.getStringValue(LocalFileSystemKeys.fileSystemDirectory());
  }
}
