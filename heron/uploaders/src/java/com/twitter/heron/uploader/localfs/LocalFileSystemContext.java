package com.twitter.heron.uploader.localfs;

import java.nio.file.Paths;

import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Misc;

public class LocalFileSystemContext {
  public static String fileSystemDirectory(Config cfg) {
    String directory = cfg.getStringValue(LocalFileSystemKeys.FILE_SYSTEM_DIRECTORY,
        LocalFileSystemDefaults.FILE_SYSTEM_DIRECTORY);
    return Misc.substitute(cfg, directory);
  }
}
