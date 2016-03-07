package com.twitter.heron.spi.utils;

import com.twitter.heron.spi.common.Keys;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.statemgr.SchedulerStateManager;
import com.twitter.heron.api.generated.TopologyAPI;

public class Runtime {

  public static String topologyId(Config runtime) {
    return runtime.getStringValue(Keys.topologyId());
  }
  
  public static String topologyName(Config runtime) {
    return runtime.getStringValue(Keys.topologyName());
  }

  public static String topologyClassPath(Config runtime) {
    return runtime.getStringValue(Keys.topologyClassPath());
  }

  public static TopologyAPI.Topology topology(Config runtime) {
    return (TopologyAPI.Topology)runtime.get(Keys.topologyDefinition());
  }

  public static String topologyPackageUri(Config cfg) {
    return cfg.getStringValue(Keys.topologyPackageUri());
  }

  public static SchedulerStateManager schedulerStateManager(Config runtime) {
    return (SchedulerStateManager)runtime.get(Keys.schedulerStateManager());
  }

  public static Shutdown schedulerShutdown(Config runtime) {
    return (Shutdown)runtime.get(Keys.schedulerShutdown());
  }

  public static String componentRamMap(Config runtime) {
    return runtime.getStringValue(Keys.componentRamMap());
  }

  public static String componentJvmOpts(Config runtime) {
    return runtime.getStringValue(Keys.componentJvmOpts());
  }

  public static String instanceDistribution(Config runtime) {
    return runtime.getStringValue(Keys.instanceDistribution());
  }

  public static String instanceJvmOpts(Config runtime) {
    return runtime.getStringValue(Keys.instanceJvmOpts());
  }

  public static Long numContainers(Config runtime) {
    return runtime.getLongValue(Keys.numContainers());
  }
}
