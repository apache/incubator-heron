package com.twitter.heron.scheduler.api.context;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.scheduler.api.IConfigLoader;

public class LaunchContext extends Context {
  private final TopologyAPI.Topology topology;

  public LaunchContext(IConfigLoader configLoader,
                       TopologyAPI.Topology topology)
      throws IllegalAccessException, ClassNotFoundException, InstantiationException {
    super(configLoader, topology.getName());

    this.topology = topology;
  }

  public TopologyAPI.Topology getTopology() {
    return topology;
  }
}
