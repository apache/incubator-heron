package com.twitter.heron.spi.scheduler.context;

import com.twitter.heron.spi.scheduler.IConfigLoader;

public class RuntimeManagerContext extends Context {
  public static final String RESTART_CONTAINER_INDEX = "shard";

  public RuntimeManagerContext(IConfigLoader configLoader,
                               String topologyName)
      throws IllegalAccessException, ClassNotFoundException, InstantiationException {
    super(configLoader, topologyName);
  }

  public int getRestartShard() {
    String shardStr = (String) getConfig().get(RESTART_CONTAINER_INDEX);
    if (shardStr == null) {
      throw new RuntimeException("Failed to get shard to restart in config");
    }

    return Integer.parseInt(shardStr);
  }
}
