package com.twitter.heron.uploader.localfs;

import com.twitter.heron.spi.common.Defaults;

public class LocalFileSystemDefaults extends Defaults {
  public static String fileSystemDirectory() {
    return LocalFileSystemConfigDefaults.get("FILE_SYSTEM_DIRECTORY");
  }
}
