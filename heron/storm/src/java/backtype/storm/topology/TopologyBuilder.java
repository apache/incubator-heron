// Copyright 2016 Twitter. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package backtype.storm.topology;

// TODO:- Add this
// import backtype.storm.grouping.CustomStreamGrouping;
import com.twitter.heron.api.HeronTopology;

import backtype.storm.generated.StormTopology;

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
