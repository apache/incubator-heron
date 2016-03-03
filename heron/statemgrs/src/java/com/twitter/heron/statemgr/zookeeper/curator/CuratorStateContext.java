package com.twitter.heron.statemgr.zookeeper.curator;

import com.twitter.heron.spi.common.Config;

public class CuratorStateContext {
  public static String zkConnectionString(Config cfg) {
    return cfg.getStringValue(CuratorStateKeys.ZK_CONNECTION_STRING);
  }
}
