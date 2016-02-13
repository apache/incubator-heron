package com.twitter.heron.packing.roundrobin;

import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import com.twitter.heron.api.Config;
import com.twitter.heron.api.generated.TopologyAPI;

import com.twitter.heron.spi.common.Constants;
import com.twitter.heron.spi.common.Keys;
import com.twitter.heron.spi.common.Context;
import com.twitter.heron.spi.common.PackingPlan;

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
    int numContainer = 2;
    int componentParallelism = 4;

    // Set up the topology and its config
    Config topologyConfig = new Config();
    topologyConfig.put(Config.TOPOLOGY_STMGRS, numContainer);

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

    Context context = Context.newBuilder()
        .put(Keys.Runtime.TOPOLOGY_PHYSICAL_PLAN, topology)
        .build();

    // DefaultConfigLoader configLoader = DefaultConfigLoader.class.newInstance();
    // String stateMgrClass = "com.twitter.heron.spi.statemgr.NullStateManager";
    // String overrides = String.format("%s=%s", Constants.STATE_MANAGER_CLASS, stateMgrClass);
    // configLoader.load("", overrides);

    // Context context = new Context(configLoader, topology);

    RoundRobinPacking packing = RoundRobinPacking.class.newInstance();
    packing.initialize(context);
    PackingPlan output = packing.pack();
    Assert.assertEquals(numContainer, output.containers.size());

    for (PackingPlan.ContainerPlan container : output.containers.values()) {
      Assert.assertEquals(numInstance / numContainer, container.instances.size());

      // Verify each container got 2 spout and 2 bolt and container 1 got
      Assert.assertEquals(
          2, countCompoment("spout", container.instances, packing));
      Assert.assertEquals(
          2, countCompoment("bolt", container.instances, packing));
    }
  }
}
