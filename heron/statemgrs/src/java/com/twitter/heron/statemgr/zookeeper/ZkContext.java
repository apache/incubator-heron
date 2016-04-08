package com.twitter.heron.statemgr.zookeeper;

import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Context;

public class ZkContext extends Context {
  public static final String IS_INITIALIZE_TREE = "heron.statemgr.zookeeper.is.initialize.tree";
  public static final String SESSION_TIMEOUT_MS = "heron.statemgr.zookeeper.session.timeout.ms";
  public static final String CONNECTION_TIMEOUT_MS = "heron.statemgr.zookeeper.connection.timeout.ms";
  public static final String RETRY_COUNT = "heron.statemgr.zookeeper.retry.count";
  public static final String RETRY_INTERVAL_MS = "heron.statemgr.zookeeper.retry.interval.ms";

  public static boolean isInitializeTree(Config config) {
    return config.getBooleanValue(IS_INITIALIZE_TREE, true);
  }

  public static int sessionTimeoutMs(Config config) {
    return config.getIntegerValue(SESSION_TIMEOUT_MS);
  }

  public static int connectionTimeoutMs(Config config) {
    return config.getIntegerValue(CONNECTION_TIMEOUT_MS);
  }

  public static int retryCount(Config config) {
    return config.getIntegerValue(RETRY_COUNT);
  }

  public static int retryIntervalMs(Config config) {
    return config.getIntegerValue(RETRY_INTERVAL_MS);
  }
}
