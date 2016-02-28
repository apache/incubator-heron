package com.twitter.heron.statemgr.localfs;

import com.twitter.heron.spi.common.Config;

public class LocalFileSystemContext {
  public static boolean initLocalFileTree(Config cfg) {
    return cfg.get(LocalFileSystemKeys.IS_INITIALIZE_FILE_TREE) == null ?
        true : (Boolean) cfg.get(LocalFileSystemKeys.IS_INITIALIZE_FILE_TREE);
  }
}
