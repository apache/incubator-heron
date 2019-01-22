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

package org.apache.heron.api;

import java.util.Map;
import java.util.UUID;

import org.apache.heron.api.generated.TopologyAPI;
import org.apache.heron.api.utils.Utils;

public class HeronTopology {

  private TopologyAPI.Topology.Builder topologyBuilder;

  private String name;

  private TopologyAPI.TopologyState state;

  private Config heronConfig;

  public HeronTopology(TopologyAPI.Topology.Builder topologyBuilder) {
    this.topologyBuilder = topologyBuilder;
  }

  private static void addDefaultTopologyConfig(Map<String, Object> userConfig) {
    if (!userConfig.containsKey(Config.TOPOLOGY_DEBUG)) {
      userConfig.put(Config.TOPOLOGY_DEBUG, "false");
    }
    if (!userConfig.containsKey(Config.TOPOLOGY_STMGRS)) {
      userConfig.put(Config.TOPOLOGY_STMGRS, "1");
    }
    if (!userConfig.containsKey(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS)) {
      userConfig.put(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS, "30");
    }
    if (!userConfig.containsKey(Config.TOPOLOGY_COMPONENT_PARALLELISM)) {
      userConfig.put(Config.TOPOLOGY_COMPONENT_PARALLELISM, "1");
    }
    if (!userConfig.containsKey(Config.TOPOLOGY_MAX_SPOUT_PENDING)) {
      userConfig.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, "100");
    }
    if (!userConfig.containsKey(Config.TOPOLOGY_RELIABILITY_MODE)) {
      userConfig.put(Config.TOPOLOGY_RELIABILITY_MODE,
                     String.valueOf(Config.TopologyReliabilityMode.ATMOST_ONCE));
    }
    if (!userConfig.containsKey(Config.TOPOLOGY_ENABLE_MESSAGE_TIMEOUTS)) {
      userConfig.put(Config.TOPOLOGY_ENABLE_MESSAGE_TIMEOUTS, "true");
    }
    if (!userConfig.containsKey(Config.TOPOLOGY_STATEFUL_SPILL_STATE)) {
      userConfig.put(Config.TOPOLOGY_STATEFUL_SPILL_STATE, "false");
      if (!userConfig.containsKey(Config.TOPOLOGY_STATEFUL_SPILL_STATE_LOCATION)) {
        userConfig.put(Config.TOPOLOGY_STATEFUL_SPILL_STATE_LOCATION, "./spilled-state/");
      }
    }
  }

  public TopologyAPI.Topology getTopology() {
    if (name == null || state == null || heronConfig == null) {
      throw new IllegalArgumentException("Failed to build topology; missing necessary info.");
    }

    String topologyId = name + UUID.randomUUID().toString();

    topologyBuilder.setId(topologyId);
    topologyBuilder.setName(name);
    topologyBuilder.setState(state);

    // Add extra config
    addDefaultTopologyConfig(heronConfig);
    heronConfig.put(Config.TOPOLOGY_NAME, name);

    topologyBuilder.setTopologyConfig(Utils.getConfigBuilder(heronConfig));

    return topologyBuilder.build();
  }

  public HeronTopology setName(String topologyName) {
    this.name = topologyName;
    return this;
  }

  public HeronTopology setState(TopologyAPI.TopologyState topologyState) {
    this.state = topologyState;
    return this;
  }

  public HeronTopology setConfig(Config hConfig) {
    this.heronConfig = hConfig;
    return this;
  }
}
