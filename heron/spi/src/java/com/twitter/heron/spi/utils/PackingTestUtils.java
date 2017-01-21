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

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.common.basics.ByteAmount;
import com.twitter.heron.common.basics.Pair;
import com.twitter.heron.proto.system.PackingPlans;
import com.twitter.heron.spi.common.ClusterDefaults;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Keys;
import com.twitter.heron.spi.packing.IPacking;
import com.twitter.heron.spi.packing.InstanceId;
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

  public static PackingPlan.ContainerPlan testContainerPlan(int containerId) {
    return testContainerPlan(containerId, 0, 1);
  }

  @SuppressWarnings("unchecked")
  public static PackingPlan.ContainerPlan testContainerPlan(int containerId,
                                                            Integer... instanceIndices) {
    List<Pair<String, Integer>> instanceInfo = new ArrayList<>();
    for (int instanceIndex : instanceIndices) {
      String componentName = "componentName-" + instanceIndex;
      instanceInfo.add(new Pair<>(componentName, instanceIndex));
    }
    return testContainerPlan(containerId, instanceInfo.toArray(
        (Pair<String, Integer>[]) Array.newInstance(Pair.class, instanceInfo.size())));
  }

  @SafeVarargs
  public static PackingPlan.ContainerPlan testContainerPlan(int containerId,
                                                            Pair<String, Integer>... instanceInfo) {
    double cpu = 1.5;
    ByteAmount ram = ByteAmount.fromGigabytes(1);

    Set<PackingPlan.InstancePlan> instancePlans = new HashSet<>();
    for (Pair<String, Integer> info: instanceInfo) {
      PackingPlan.InstancePlan instance = testInstancePlan(info.first, info.second);
      instancePlans.add(instance);
      cpu += instance.getResource().getCpu();
      ram = ram.plus(instance.getResource().getRam());
    }
    Resource resource = new Resource(cpu, ram, ram);
    return new PackingPlan.ContainerPlan(containerId, instancePlans, resource);
  }

  public static PackingPlan.InstancePlan testInstancePlan(
      String componentName, int instanceIndex) {
    Resource resource = new Resource(1.5, ByteAmount.fromGigabytes(2), ByteAmount.fromBytes(3));
    return new PackingPlan.InstancePlan(new InstanceId(componentName, instanceIndex, 1), resource);
  }
}
