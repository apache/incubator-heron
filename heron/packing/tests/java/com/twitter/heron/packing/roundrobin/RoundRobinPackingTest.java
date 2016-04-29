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

package com.twitter.heron.packing.roundrobin;

import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.spi.common.ClusterDefaults;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Keys;
import com.twitter.heron.spi.common.PackingPlan;
import com.twitter.heron.spi.utils.TopologyTests;
import com.twitter.heron.spi.utils.TopologyUtils;

public class RoundRobinPackingTest {
  private int countCompoment(String component, Map<String, PackingPlan.InstancePlan> instances,
                             RoundRobinPacking packing) {
    int count = 0;
    for (PackingPlan.InstancePlan pair : instances.values()) {
      if (component.equals(packing.getComponentName(pair.id))) {
        count++;
      }
    }
    return count;
  }

  /**
   * test even packing of instances
   */
  @Test
  public void testEvenPacking() throws Exception {
    int numContainers = 2;
    int componentParallelism = 4;

    // Set up the topology and its config
    com.twitter.heron.api.Config topologyConfig = new com.twitter.heron.api.Config();
    topologyConfig.put(com.twitter.heron.api.Config.TOPOLOGY_STMGRS, numContainers);

    // Setup the spout parallelism
    Map<String, Integer> spouts = new HashMap<>();
    spouts.put("spout", componentParallelism);

    // Setup the bolt parallelism
    Map<String, Integer> bolts = new HashMap<>();
    bolts.put("bolt", componentParallelism);

    TopologyAPI.Topology topology =
        TopologyTests.createTopology("testTopology", topologyConfig, spouts, bolts);

    int numInstance = TopologyUtils.getTotalInstance(topology);
    Assert.assertEquals((spouts.size() + bolts.size()) * componentParallelism, numInstance);

    Config config = Config.newBuilder()
        .put(Keys.topologyId(), topology.getId())
        .put(Keys.topologyName(), topology.getName())
        .putAll(ClusterDefaults.getDefaults())
        .build();

    Config runtime = Config.newBuilder()
        .put(Keys.topologyDefinition(), topology)
        .build();

    // DefaultConfigLoader configLoader = DefaultConfigLoader.class.newInstance();
    // String stateMgrClass = "com.twitter.heron.statemgr.NullStateManager";
    // String overrides = String.format("%s=%s", Constants.STATE_MANAGER_CLASS, stateMgrClass);
    // configLoader.load("", overrides);

    // Context context = new Context(configLoader, topology);

    RoundRobinPacking packing = RoundRobinPacking.class.newInstance();
    packing.initialize(config, runtime);
    PackingPlan output = packing.pack();
    Assert.assertEquals(numContainers, output.containers.size());

    for (PackingPlan.ContainerPlan container : output.containers.values()) {
      Assert.assertEquals(numInstance / numContainers, container.instances.size());

      // Verify each container got 2 spout and 2 bolt and container 1 got
      Assert.assertEquals(
          2, countCompoment("spout", container.instances, packing));
      Assert.assertEquals(
          2, countCompoment("bolt", container.instances, packing));
    }
  }
}
