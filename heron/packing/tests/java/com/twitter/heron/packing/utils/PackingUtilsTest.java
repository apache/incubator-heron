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
package com.twitter.heron.packing.utils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.common.basics.ByteAmount;
import com.twitter.heron.common.utils.topology.TopologyTests;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Context;
import com.twitter.heron.spi.packing.Resource;
import com.twitter.heron.spi.utils.PackingTestUtils;

public class PackingUtilsTest {

  private static TopologyAPI.Topology getTopology(
      int spoutParallelism, int boltParallelism,
      com.twitter.heron.api.Config topologyConfig) {
    return TopologyTests.createTopology("testTopology", topologyConfig, "spout", "bolt",
        spoutParallelism, boltParallelism);
  }

  /**
   * Tests get default instance resources
   */
  @Test
  public void testDefaultInstanceResource() {
    int noSpouts = 6;
    int noBolts = 3;
    int boltScalingDown = 2;
    // Without component default resource config
    com.twitter.heron.api.Config topologyConfig = new com.twitter.heron.api.Config();
    TopologyAPI.Topology topology1 = getTopology(noSpouts, noBolts, topologyConfig);
    Config config1 = PackingTestUtils.newTestConfig(topology1);

    List<TopologyAPI.Config.KeyValue> confList1 = topology1.getTopologyConfig().getKvsList();
    Resource defaultInstanceResources1 =
        PackingUtils.getDefaultInstanceResources(confList1, config1);

    double instanceCpu = Context.instanceCpu(config1);
    Assert.assertEquals(defaultInstanceResources1.getCpu(), instanceCpu, 0.0001);
    Assert.assertEquals(defaultInstanceResources1.getRam(), Context.instanceRam(config1));
    Assert.assertEquals(defaultInstanceResources1.getDisk(), Context.instanceDisk(config1));

    // With component default resource config
    topologyConfig.setComponentDefaultCpu(111);
    topologyConfig.setComponentDefaultRam(ByteAmount.fromMegabytes(222));
    topologyConfig.setComponentDefaultDisk(ByteAmount.fromMegabytes(333));
    TopologyAPI.Topology topology2 = getTopology(noSpouts, noBolts, topologyConfig);
    Config config2 = PackingTestUtils.newTestConfig(topology2);

    List<TopologyAPI.Config.KeyValue> confList2 = topology2.getTopologyConfig().getKvsList();
    Resource defaultInstanceResources2 =
        PackingUtils.getDefaultInstanceResources(confList2, config2);

    Assert.assertEquals(defaultInstanceResources2.getCpu(), 111, 0.0001);
    Assert.assertEquals(defaultInstanceResources2.getRam(), ByteAmount.fromMegabytes(222));
    Assert.assertEquals(defaultInstanceResources2.getDisk(), ByteAmount.fromMegabytes(333));
  }

  /**
   * Tests the increaseBy method for long values
   */
  @Test
  public void testIncreaseByLong() {
    long value = 1024;
    int padding = 1;
    long expectedResult = 1034;
    Assert.assertEquals(expectedResult, PackingUtils.increaseBy(value, padding));
  }

  /**
   * Tests the increaseBy method for double values
   */
  @Test
  public void testIncreaseByDouble() {
    double value = 10.0;
    int padding = 1;
    double expectedResult = 10.1;
    Assert.assertEquals(0, Double.compare(PackingUtils.increaseBy(value, padding), expectedResult));
  }

  /**
   * Tests the component scale up and down methods.
   */
  @Test
  public void testComponentScaling() {

    Map<String, Integer> componentChanges = new HashMap<>();
    componentChanges.put("spout", -2);
    componentChanges.put("bolt1", 2);
    componentChanges.put("bolt2", -1);

    Map<String, Integer> componentToScaleUp = PackingUtils.getComponentsToScale(componentChanges,
        PackingUtils.ScalingDirection.UP);
    Assert.assertEquals(1, componentToScaleUp.size());
    Assert.assertEquals(2, (int) componentToScaleUp.get("bolt1"));

    Map<String, Integer> componentToScaleDown =
        PackingUtils.getComponentsToScale(componentChanges, PackingUtils.ScalingDirection.DOWN);
    Assert.assertEquals(2, componentToScaleDown.size());
    Assert.assertEquals(-2, (int) componentToScaleDown.get("spout"));
    Assert.assertEquals(-1, (int) componentToScaleDown.get("bolt2"));
  }

  @Test
  public void testResourceScaleDown() {
    int noSpouts = 6;
    int noBolts = 3;
    int boltScalingDown = 2;
    com.twitter.heron.api.Config topologyConfig = new com.twitter.heron.api.Config();

    TopologyAPI.Topology topology = getTopology(noSpouts, noBolts, topologyConfig);
    Config config = PackingTestUtils.newTestConfig(topology);

    Resource defaultInstanceResources = new Resource(
        Context.instanceCpu(config),
        Context.instanceRam(config),
        Context.instanceDisk(config));
    Map<String, Integer> componentChanges = new HashMap<>();
    componentChanges.put("bolt", -boltScalingDown); // 1 bolt
    Resource scaledownResource = PackingUtils.computeTotalResourceChange(topology,
        componentChanges, defaultInstanceResources, PackingUtils.ScalingDirection.DOWN);
    Assert.assertEquals((long) (boltScalingDown * defaultInstanceResources.getCpu()),
        (long) scaledownResource.getCpu());
    Assert.assertEquals(defaultInstanceResources.getRam().multiply(boltScalingDown),
        scaledownResource.getRam());
    Assert.assertEquals(defaultInstanceResources.getDisk().multiply(boltScalingDown),
        scaledownResource.getDisk());
  }

  @Test
  public void testResourceScaleUp() {
    int noSpouts = 6;
    int noBolts = 3;
    int boltScalingUp = 2;
    com.twitter.heron.api.Config topologyConfig = new com.twitter.heron.api.Config();

    TopologyAPI.Topology topology = getTopology(noSpouts, noBolts, topologyConfig);
    Config config = PackingTestUtils.newTestConfig(topology);

    Resource defaultInstanceResources = new Resource(
        Context.instanceCpu(config),
        Context.instanceRam(config),
        Context.instanceDisk(config));
    Map<String, Integer> componentChanges = new HashMap<>();
    componentChanges.put("bolt", boltScalingUp); // 5 bolts
    Resource scaleupResource = PackingUtils.computeTotalResourceChange(topology,
        componentChanges, defaultInstanceResources, PackingUtils.ScalingDirection.UP);
    Assert.assertEquals((long) (boltScalingUp * defaultInstanceResources.getCpu()),
        (long) scaleupResource.getCpu());
    Assert.assertEquals(defaultInstanceResources.getRam().multiply(boltScalingUp),
        scaleupResource.getRam());
    Assert.assertEquals(defaultInstanceResources.getDisk().multiply(boltScalingUp),
        scaleupResource.getDisk());
  }
}
