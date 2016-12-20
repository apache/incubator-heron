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

package com.twitter.heron.api;

import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.google.protobuf.ByteString;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.api.utils.Utils;

public class HeronTopology {

  private TopologyAPI.Topology.Builder topologyBuilder;

  private String name;

  private TopologyAPI.TopologyState state;

  private Config heronConfig;

  public HeronTopology(TopologyAPI.Topology.Builder topologyBuilder) {
    this.topologyBuilder = topologyBuilder;
  }

  private static TopologyAPI.Config.Builder getConfigBuilder(Config config) {
    TopologyAPI.Config.Builder cBldr = TopologyAPI.Config.newBuilder();
    Set<String> apiVars = config.getApiVars();
    for (String key : config.keySet()) {
      Object value = config.get(key);
      TopologyAPI.Config.KeyValue.Builder b = TopologyAPI.Config.KeyValue.newBuilder();
      b.setKey(key);
      if (apiVars.contains(key)) {
        b.setType(TopologyAPI.ConfigValueType.STRING_VALUE);
        b.setValue(value.toString());
      } else {
        b.setType(TopologyAPI.ConfigValueType.JAVA_SERIALIZED_VALUE);
        b.setSerializedValue(ByteString.copyFrom(Utils.serialize(value)));
      }
      cBldr.addKvs(b);
    }

    return cBldr;
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
    if (!userConfig.containsKey(Config.TOPOLOGY_ENABLE_ACKING)) {
      userConfig.put(Config.TOPOLOGY_ENABLE_ACKING, "false");
    }
    if (!userConfig.containsKey(Config.TOPOLOGY_ENABLE_MESSAGE_TIMEOUTS)) {
      userConfig.put(Config.TOPOLOGY_ENABLE_MESSAGE_TIMEOUTS, "true");
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

    topologyBuilder.setTopologyConfig(getConfigBuilder(heronConfig));

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
