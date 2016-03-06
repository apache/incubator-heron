package com.twitter.heron.uploader.localfs;

import com.twitter.heron.spi.common.Keys;

public class LocalFileSystemKeys extends Keys {
  public static String fileSystemDirectory() {
    return LocalFileSystemConfigKeys.get("FILE_SYSTEM_DIRECTORY");
  }
}
