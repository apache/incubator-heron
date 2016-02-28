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

  public static String topologyClassPath(Config runtime) {
    return runtime.getStringValue(Keys.TOPOLOGY_CLASS_PATH);
  }

  public static TopologyAPI.Topology topology(Config runtime) {
    return (TopologyAPI.Topology)runtime.get(Keys.TOPOLOGY_DEFINITION);
  }

  public static IStateManager stateManager(Config runtime) {
    return (IStateManager)runtime.get(Keys.STATE_MANAGER);
  }

  public static String componentRamMap(Config runtime) {
    return runtime.getStringValue(Keys.COMPONENT_RAMMAP);
  }

  public static String componentJvmOpts(Config runtime) {
    return runtime.getStringValue(Keys.COMPONENT_JVM_OPTS_IN_BASE64);
  }

  public static String instanceDistribution(Config runtime) {
    return runtime.getStringValue(Keys.INSTANCE_DISTRIBUTION);
  }

  public static String instanceJvmOpts(Config runtime) {
    return runtime.getStringValue(Keys.INSTANCE_JVM_OPTS_IN_BASE64);
  }

  public static String numContainers(Config runtime) {
    return runtime.getStringValue(Keys.NUM_CONTAINERS);
  }
}
