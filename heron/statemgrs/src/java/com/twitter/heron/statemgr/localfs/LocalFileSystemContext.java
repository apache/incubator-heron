package com.twitter.heron.statemgr.localfs;

import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Context;

public class LocalFileSystemContext extends Context {

  /**
   * Get the config specifying whether to initialize file directory hierarchy
   *
   * @param Config, the config map
   *
   * @return true, if config does not exist, else the specified value
   */
  public static boolean initLocalFileTree(Config config) {
    return config.get(LocalFileSystemKeys.get("IS_INITIALIZE_FILE_TREE")) == null ?
        true : (Boolean) config.get(LocalFileSystemKeys.get("IS_INITIALIZE_FILE_TREE"));
  }
}
