package org.apache.storm.topology;

// TODO:- Add this
// import org.apache.storm.grouping.CustomStreamGrouping;
import com.twitter.heron.api.HeronTopology;

import org.apache.storm.generated.StormTopology;

public class TopologyBuilder {
  private com.twitter.heron.api.topology.TopologyBuilder delegate =
              new com.twitter.heron.api.topology.TopologyBuilder();

  public StormTopology createTopology() {
    HeronTopology topology = delegate.createTopology();
    return new StormTopology(topology);
  }

  public BoltDeclarer setBolt(String id, IRichBolt bolt) {
    return setBolt(id, bolt, null);
  }

  public BoltDeclarer setBolt(String id, IRichBolt bolt, Number parallelism_hint) {
    IRichBoltDelegate boltImpl = new IRichBoltDelegate(bolt);
    com.twitter.heron.api.topology.BoltDeclarer declarer = delegate.setBolt(id, boltImpl, parallelism_hint);
    return new BoltDeclarerImpl(declarer);
  }

  public BoltDeclarer setBolt(String id, IBasicBolt bolt) {
    return setBolt(id, bolt, null);
  }

  public BoltDeclarer setBolt(String id, IBasicBolt bolt, Number parallelism_hint) {
    return setBolt(id, new BasicBoltExecutor(bolt), parallelism_hint);
  }

  public SpoutDeclarer setSpout(String id, IRichSpout spout) {
    return setSpout(id, spout, null);
  }

  public SpoutDeclarer setSpout(String id, IRichSpout spout, Number parallelism_hint) {
    IRichSpoutDelegate spoutImpl = new IRichSpoutDelegate(spout);
    com.twitter.heron.api.topology.SpoutDeclarer declarer = delegate.setSpout(id, spoutImpl, parallelism_hint);
    return new SpoutDeclarerImpl(declarer);
  }
}
