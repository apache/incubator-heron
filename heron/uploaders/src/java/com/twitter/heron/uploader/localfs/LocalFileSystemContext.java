package com.twitter.heron.uploader.localfs;

import java.nio.file.Paths;

import com.twitter.heron.spi.common.Config;

public class LocalFileSystemContext {
  public static String fileSystemDirectory(Config cfg) {
    return cfg.getStringValue(LocalFileSystemKeys.FILE_SYSTEM_DIRECTORY,
        LocalFileSystemDefaults.FILE_SYSTEM_DIRECTORY);
  }
}
