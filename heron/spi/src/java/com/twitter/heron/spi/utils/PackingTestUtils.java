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
package com.twitter.heron.spi.utils;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.proto.system.PackingPlans;
import com.twitter.heron.spi.common.ClusterDefaults;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Keys;
import com.twitter.heron.spi.packing.IPacking;
import com.twitter.heron.spi.packing.PackingPlan;
import com.twitter.heron.spi.packing.PackingPlanProtoSerializer;
import com.twitter.heron.spi.packing.Resource;

/**
 * Packing utilities for testing
 */
public final class PackingTestUtils {

  private PackingTestUtils() {
  }

  public static PackingPlan testPackingPlan(String topologyName, IPacking packing) {
    Map<String, Integer> spouts = new HashMap<>();
    spouts.put("testSpout", 2);

    Map<String, Integer> bolts = new HashMap<>();
    bolts.put("testBolt", 3);

    com.twitter.heron.api.Config topologyConfig = new com.twitter.heron.api.Config();
    topologyConfig.put(com.twitter.heron.api.Config.TOPOLOGY_STMGRS, 1);

    TopologyAPI.Topology topology =
        TopologyTests.createTopology(topologyName, topologyConfig, spouts, bolts);

    Config config = Config.newBuilder()
        .put(Keys.topologyId(), topology.getId())
        .put(Keys.topologyName(), topology.getName())
        .putAll(ClusterDefaults.getDefaults())
        .build();

    packing.initialize(config, topology);
    return packing.pack();
  }

  public static PackingPlans.PackingPlan testProtoPackingPlan(
      String topologyName, IPacking packing) {
    PackingPlan plan = testPackingPlan(topologyName, packing);
    PackingPlanProtoSerializer serializer = new PackingPlanProtoSerializer();
    return serializer.toProto(plan);
  }

  public static PackingPlan.ContainerPlan testContainerPlan(String containerId) {
    Resource resource = new Resource(7.5, 6, 9);
    Set<PackingPlan.InstancePlan> instancePlans = new HashSet<>();
    for (int index : new Integer[]{0, 1}) {
      String instanceId = "instance-" + index;
      String componentName = "componentName-" + index;
      instancePlans.add(testInstancePlan(instanceId, componentName));
    }
    return new PackingPlan.ContainerPlan(containerId, instancePlans, resource);
  }

  private static PackingPlan.InstancePlan testInstancePlan(String id, String componentName) {
    Resource resource = new Resource(1.5, 2, 3);
    return new PackingPlan.InstancePlan(id, componentName, resource);
  }
}
