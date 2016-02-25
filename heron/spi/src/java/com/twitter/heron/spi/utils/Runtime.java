package com.twitter.heron.spi.utils;

import com.twitter.heron.spi.common.Keys;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.statemgr.IStateManager;
import com.twitter.heron.api.generated.TopologyAPI;

public class Runtime {

  public static String topologyId(Config runtime) {
    return runtime.getStringValue(Keys.TOPOLOGY_ID);
  }
  
  public static String topologyName(Config runtime) {
    return runtime.getStringValue(Keys.TOPOLOGY_NAME);
  }

  public static TopologyAPI.Topology topology(Config runtime) {
    return (TopologyAPI.Topology)runtime.get(Keys.TOPOLOGY_DEFINITION);
  }

  public static IStateManager stateManager(Config runtime) {
    return (IStateManager)runtime.get(Keys.STATE_MANAGER);
  }
}
