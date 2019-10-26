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
package org.apache.heron.integration_topology_test.core;

import java.util.HashMap;
import java.util.Map;

import org.apache.heron.api.Config;
import org.apache.heron.api.HeronTopology;
import org.apache.heron.api.generated.TopologyAPI;
import org.apache.heron.api.topology.BoltDeclarer;
import org.apache.heron.api.topology.SpoutDeclarer;
import org.apache.heron.api.topology.TopologyBuilder;

public class TopologyTestTopologyBuilder extends TopologyBuilder {

  private static final int DEFAULT_EXECUTION_COUNT = 10;
  private final Map<String, Integer> components = new HashMap<>();
  private final String outputLocation;

  public TopologyTestTopologyBuilder(String outputLocation) {
    this.outputLocation = outputLocation;
  }

  public BoltDeclarer setBolt(String id, StatefulBolt bolt, Number parallelismHint) {
    return setBolt(id, new StatefulIntegrationTopologyTestBolt(bolt, this.outputLocation),
        parallelismHint);
  }

  public BoltDeclarer setBolt(String id, StatefulIntegrationTopologyTestBolt bolt,
                              Number parallelismHint) {
    components.put(id, (Integer) parallelismHint);
    return super.setBolt(id, bolt, parallelismHint);
  }

  public SpoutDeclarer setSpout(String id, StatefulSpout spout, Number parallelismHint) {
    return setSpout(id, spout, parallelismHint, DEFAULT_EXECUTION_COUNT);
  }

  // A method allows user to define the maxExecutionCount of the spout
  // To be compatible with earlier Integration Test Framework
  public SpoutDeclarer setSpout(String id, StatefulSpout spout,
                                Number parallelismHint, int maxExecutionCount) {

    StatefulIntegrationTopologyTestSpout wrappedSpout =
        new StatefulIntegrationTopologyTestSpout(spout, maxExecutionCount, this.outputLocation);

    return setSpout(id, wrappedSpout, parallelismHint);
  }

  private SpoutDeclarer setSpout(String id, StatefulIntegrationTopologyTestSpout itSpout,
                                 Number parallelismHint) {
    components.put(id, (Integer) parallelismHint);
    return super.setSpout(id, itSpout, parallelismHint);
  }

  @Override
  public HeronTopology createTopology() {

    TopologyAPI.Topology.Builder topologyBlr =
        super.createTopology().
            setConfig(new Config()).
            setName("").
            setState(TopologyAPI.TopologyState.RUNNING).
            getTopology().toBuilder();

    // Clear unnecessary fields to make the state of TopologyAPI.Topology.Builder clean
    topologyBlr.clearTopologyConfig().clearName().clearState();

    // We wrap it to the new topologyBuilder
    return new HeronTopology(topologyBlr);
  }

  public Map<String, Integer> getComponentParallelism() {
    return components;
  }
}
