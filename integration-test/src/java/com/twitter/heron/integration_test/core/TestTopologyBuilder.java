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
package com.twitter.heron.integration_test.core;

import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import com.twitter.heron.api.Config;
import com.twitter.heron.api.HeronTopology;
import com.twitter.heron.api.bolt.IRichBolt;
import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.api.spout.IRichSpout;
import com.twitter.heron.api.topology.BoltDeclarer;
import com.twitter.heron.api.topology.SpoutDeclarer;
import com.twitter.heron.api.topology.TopologyBuilder;

public class TestTopologyBuilder extends TopologyBuilder {
  private static final Logger LOG = Logger.getLogger(TestTopologyBuilder.class.getName());
  // This variable will be used as input variable for constructor of our aggregator bolt
  // This will determine the location of where our output is directed
  // Could be URL, file location, etc.
  private final String outputLocation;
  // The structure of the topologyBlr - a graph directed from children to parents
  private final Map<String, TopologyAPI.Bolt.Builder> bolts = new HashMap<>();
  private final Map<String, TopologyAPI.Spout.Builder> spouts = new HashMap<>();
  private final Map<String, HashSet<String>> prev = new HashMap<>();
  // By default, terminalBoltClass will be AggregatorBolt, which writes to specified HTTP server
  private String terminalBoltClass = "com.twitter.heron.integration_test.core.AggregatorBolt";

  public TestTopologyBuilder(String outputLocation) {
    this.outputLocation = outputLocation;
  }

  public TestTopologyBuilder(String topologyName, String httpServerUrl) {
    this.outputLocation = httpServerUrl + "/" + topologyName;
  }

  @Override
  public BoltDeclarer setBolt(String id, IRichBolt bolt, Number parallelismHint) {
    // Wrap bolt
    IntegrationTestBolt itBolt = new IntegrationTestBolt(bolt);
    // Call super
    return super.setBolt(id, itBolt, parallelismHint);
  }

  @Override
  public SpoutDeclarer setSpout(String id, IRichSpout spout, Number parallelismHint) {
    // Wrap Spout
    IntegrationTestSpout itSpout = new IntegrationTestSpout(spout);
    return setSpout(id, itSpout, parallelismHint);
  }

  // A method allows user to define the maxExecutionCount of the spout
  // To be compatible with earlier Integration Test Framework
  public SpoutDeclarer setSpout(String id, IRichSpout spout,
                                Number parallelismHint, int maxExecutionCount) {
    // Wrap Spout
    IntegrationTestSpout itSpout = new IntegrationTestSpout(spout, maxExecutionCount);
    return setSpout(id, itSpout, parallelismHint);
  }

  private SpoutDeclarer setSpout(String id, IntegrationTestSpout itSpout, Number parallelismHint) {
    // Call super
    return super.setSpout(id, itSpout, parallelismHint);
  }

  public void setTerminalBoltClass(String terminalBoltClass) {
    this.terminalBoltClass = terminalBoltClass;
  }

  // By default, will use AggregatorBolt, which writes to HTTP Server and takes URL as input
  @Override
  public HeronTopology createTopology() {
    // We will add the aggregation_bolt to be serialized
    final String AGGREGATOR_BOLT = "__integration_test_aggregator_bolt";
    BaseBatchBolt aggregatorBolt;
    try {
      // Terminal Bolt will be initialized using reflection, based on the value of terminal bolt
      // class.
      // class should be built on top of BaseBatchBolt abstract class, and can be changed using
      // setTerminalBolt function
      aggregatorBolt =
          (BaseBatchBolt) Class.forName(terminalBoltClass).getConstructor(String.class)
              .newInstance(this.outputLocation);
    } catch (NoSuchMethodException e) {
      throw new RuntimeException(e + " Terminal Bolt class must have a single String constructor.");
    } catch (InstantiationException e) {
      throw new RuntimeException(e + " Terminal bolt class could not be instantiated.");
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e + " Terminal Bolt class constructor is not accessible.");
    } catch (InvocationTargetException e) {
      throw new RuntimeException(e + " Terminal Bolt class constructor could not be invoked.");
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e + " Terminal Bolt class must be a class path.");
    }
    setBolt(AGGREGATOR_BOLT, aggregatorBolt, 1);

    // We get the user-defined TopologyAPI.Topology.Builder
    TopologyAPI.Topology.Builder topologyBlr =
        super.createTopology().
            setConfig(new Config()).
            setName("").
            setState(TopologyAPI.TopologyState.RUNNING).
            getTopology().toBuilder();

    // Clear unnecessary fields to make the state of TopologyAPI.Topology.Builder clean
    topologyBlr.clearTopologyConfig().clearName().clearState();

    for (TopologyAPI.Spout.Builder spout : topologyBlr.getSpoutsBuilderList()) {
      String name = spout.getComp().getName();
      spouts.put(name, spout);
    }

    // We will build the structure of the topologyBlr - a graph directed from children to parents,
    // by looking only on bolts, since spout will not have parents
    for (TopologyAPI.Bolt.Builder bolt : topologyBlr.getBoltsBuilderList()) {
      String name = bolt.getComp().getName();
      bolts.put(name, bolt);

      if (name.equals(AGGREGATOR_BOLT)) {
        // We should not consider aggregator bolt in the topology's graph or terminal components
        // since it is not user-defined
        continue;
      }

      // To get the parent's component to construct a graph of topology structure
      for (TopologyAPI.InputStream inputStream : bolt.getInputsList()) {
        String parent = inputStream.getStream().getComponentName();
        if (prev.containsKey(name)) {
          prev.get(name).add(parent);
        } else {
          HashSet<String> parents = new HashSet<String>();
          parents.add(parent);
          prev.put(name, parents);
        }
      }
    }

    // To find the terminal bolts defined by users and link them with "AggregatorBolt"
    // First, "it" of course needs upstream component, we don't want the isolated bolt
    HashSet<String> terminals = new HashSet<>();
    // Second, "it" should not exists in the prev.valueSet, which means, it has no downstream
    HashSet<String> nonTerminals = new HashSet<>();
    for (HashSet<String> set : prev.values()) {
      nonTerminals.addAll(set);
    }
    // Here we iterate bolt in prev.keySet() rather than bolts.keySet() due to we don't want
    // a isolated bolt, including AggregatorBolt
    for (String bolt : prev.keySet()) {
      if (!nonTerminals.contains(bolt)) {
        terminals.add(bolt);
      }
    }
    // We will also consider the cases with spouts without children
    for (String spout : spouts.keySet()) {
      if (!nonTerminals.contains(spout)) {
        terminals.add(spout);
      }
    }

    // Now first, we will add all grouping to components
    for (String child : prev.keySet()) {
      for (String parent : prev.get(child)) {
        addAllGrouping(child, parent, Constants.INTEGRATION_TEST_CONTROL_STREAM_ID);
      }
    }

    // Then we need to connect aggregator bolt with user's terminal components
    // We could use any grouping but for convenience we would use allGrouping here
    for (String t : terminals) {
      List<TopologyAPI.OutputStream> osList;
      if (bolts.get(t) != null) {
        osList = bolts.get(t).getOutputsList();
      } else {
        osList = spouts.get(t).getOutputsList();
      }
      for (TopologyAPI.OutputStream os : osList) {
        addAllGrouping(AGGREGATOR_BOLT, t, os.getStream().getId());
      }
    }

    // We wrap it to the new topologyBuilder
    return new HeronTopology(topologyBlr);
  }

  /**
   * Given a child component and its upstream component, add the allGrouping between them with
   * given streamId
   *
   * @param child the child component id
   * @param parent the upstream component id
   * @param streamId the stream id
   */
  protected void addAllGrouping(String child, String parent, String streamId) {
    TopologyAPI.InputStream.Builder builder = TopologyAPI.InputStream.newBuilder();
    builder.setStream(TopologyAPI.StreamId.newBuilder().setId(streamId).setComponentName(parent));
    builder.setGtype(TopologyAPI.Grouping.ALL);

    bolts.get(child).addInputs(builder);
  }
}
