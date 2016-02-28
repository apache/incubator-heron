package com.twitter.heron.packing.roundrobin;

import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import com.twitter.heron.api.generated.TopologyAPI;

import com.twitter.heron.spi.common.Constants;
import com.twitter.heron.spi.common.Keys;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.PackingPlan;
import com.twitter.heron.spi.common.ClusterDefaults;

import com.twitter.heron.spi.utils.TopologyUtils;
import com.twitter.heron.spi.utils.TopologyTests;

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
        .put(Keys.TOPOLOGY_ID, topology.getId())
        .put(Keys.TOPOLOGY_NAME, topology.getName())
        .putAll(ClusterDefaults.getDefaults())
        .build();

    Config runtime = Config.newBuilder()
        .put(Keys.TOPOLOGY_DEFINITION, topology)
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
