package com.twitter.heron.statemgr.localfs;

import com.twitter.heron.spi.common.Keys;

public class LocalFileSystemKeys extends Keys {
  public static String initializeFileTree() {
    return LocalFileSystemConfigKeys.get("IS_INITIALIZE_FILE_TREE");
  }
}
