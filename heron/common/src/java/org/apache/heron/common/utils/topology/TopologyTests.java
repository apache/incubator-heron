/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */


package org.apache.heron.common.utils.topology;

import java.util.HashMap;
import java.util.Map;

import org.apache.heron.api.Config;
import org.apache.heron.api.HeronTopology;
import org.apache.heron.api.bolt.BaseBasicBolt;
import org.apache.heron.api.bolt.BasicOutputCollector;
import org.apache.heron.api.generated.TopologyAPI;
import org.apache.heron.api.spout.BaseRichSpout;
import org.apache.heron.api.spout.SpoutOutputCollector;
import org.apache.heron.api.topology.BoltDeclarer;
import org.apache.heron.api.topology.OutputFieldsDeclarer;
import org.apache.heron.api.topology.TopologyBuilder;
import org.apache.heron.api.topology.TopologyContext;
import org.apache.heron.api.tuple.Fields;
import org.apache.heron.api.tuple.Tuple;

public final class TopologyTests {

  private TopologyTests() {
  }

  /**
   * Create Topology proto object using HeronSubmitter API.
   *
   * @param heronConfig desired config params.
   * @param spouts spoutName -&gt; parallelism
   * @param bolts boltName -&gt; parallelism
   * @param connections connect default stream from value to key.
   * @return topology proto.
   */
  public static TopologyAPI.Topology createTopologyWithConnection(
      String topologyName,
      Config heronConfig,
      Map<String, Integer> spouts,
      Map<String, Integer> bolts,
      Map<String, String> connections) {
    TopologyBuilder builder = new TopologyBuilder();
    BaseRichSpout baseSpout = new BaseRichSpout() {
      private static final long serialVersionUID = -719523487475322625L;

      public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("field1"));
      }

      public void open(
          Map<String, Object> conf,
          TopologyContext context,
          SpoutOutputCollector collector) {
      }

      public void nextTuple() {
      }
    };
    BaseBasicBolt basicBolt = new BaseBasicBolt() {
      private static final long serialVersionUID = 2544765902130713628L;

      public void execute(Tuple input, BasicOutputCollector collector) {
      }

      public void declareOutputFields(OutputFieldsDeclarer declarer) {
      }
    };

    for (String spout : spouts.keySet()) {
      builder.setSpout(spout, baseSpout, spouts.get(spout));
    }

    for (String bolt : bolts.keySet()) {
      BoltDeclarer boltDeclarer = builder.setBolt(bolt, basicBolt, bolts.get(bolt));
      if (connections.containsKey(bolt)) {
        boltDeclarer.shuffleGrouping(connections.get(bolt));
      }
    }

    HeronTopology heronTopology = builder.createTopology();

    return heronTopology.
        setName(topologyName).
        setConfig(heronConfig).
        setState(TopologyAPI.TopologyState.RUNNING).
        getTopology();
  }

  public static TopologyAPI.Topology createTopology(String topologyName,
                                                    Config heronConfig,
                                                    Map<String, Integer> spouts,
                                                    Map<String, Integer> bolts) {
    return createTopologyWithConnection(
        topologyName, heronConfig, spouts, bolts, new HashMap<String, String>());
  }

  public static TopologyAPI.Topology createTopology(String topologyName, Config topologyConfig,
                                                    String spoutName, String boltName,
                                                    int spoutParallelism, int boltParallelism) {
    // Setup the spout parallelism
    Map<String, Integer> spouts = new HashMap<>();
    spouts.put(spoutName, spoutParallelism);

    // Setup the bolt parallelism
    Map<String, Integer> bolts = new HashMap<>();
    bolts.put(boltName, boltParallelism);

    return TopologyTests.createTopology(topologyName, topologyConfig, spouts, bolts);
  }
}
