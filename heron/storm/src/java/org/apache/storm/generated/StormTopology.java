package org.apache.storm.generated;

import com.twitter.heron.api.HeronTopology;

public class StormTopology {
  private HeronTopology topology;

  public StormTopology(HeronTopology topology) {
    this.topology = topology;
  }

  public HeronTopology getStormTopology() {
    return topology;
  }
}
