package com.twitter.heron.scheduler.util;

import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import com.twitter.heron.api.Config;
import com.twitter.heron.api.generated.TopologyAPI;

import com.twitter.heron.spi.common.Constants;
import com.twitter.heron.spi.common.PackingPlan;
import com.twitter.heron.spi.scheduler.context.LaunchContext;

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
    RoundRobinPacking packing = RoundRobinPacking.class.newInstance();
    Config topologyConfig = new Config();
    topologyConfig.put(Config.TOPOLOGY_STMGRS, numContainer);
    Map<String, Integer> spouts = new HashMap<>();
    spouts.put("spout", componentParallelism);
    Map<String, Integer> bolts = new HashMap<>();
    bolts.put("bolt", componentParallelism);
    TopologyAPI.Topology topology =
        TopologyUtilityTest.createTopology("testTopology", topologyConfig, spouts, bolts);
    int numInstance = TopologyUtility.getTotalInstance(topology);
    Assert.assertEquals((spouts.size() + bolts.size()) * componentParallelism, numInstance);
    DefaultConfigLoader configLoader = DefaultConfigLoader.class.newInstance();
    String stateMgrClass = "com.twitter.heron.spi.statemgr.NullStateManager";
    String overrides = String.format("%s=%s", Constants.STATE_MANAGER_CLASS, stateMgrClass);
    configLoader.load("", overrides);

    LaunchContext context = new LaunchContext(configLoader, topology);

    PackingPlan output =
        packing.pack(context);
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
