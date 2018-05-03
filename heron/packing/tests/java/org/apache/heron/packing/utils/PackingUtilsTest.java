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

package org.apache.heron.packing.utils;

import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import org.apache.heron.api.generated.TopologyAPI;
import org.apache.heron.common.utils.topology.TopologyTests;
import org.apache.heron.spi.common.Config;
import org.apache.heron.spi.common.Context;
import org.apache.heron.spi.packing.Resource;
import org.apache.heron.spi.utils.PackingTestUtils;

public class PackingUtilsTest {

  private static TopologyAPI.Topology getTopology(
      int spoutParallelism, int boltParallelism,
      org.apache.heron.api.Config topologyConfig) {
    return TopologyTests.createTopology("testTopology", topologyConfig, "spout", "bolt",
        spoutParallelism, boltParallelism);
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
    org.apache.heron.api.Config topologyConfig = new org.apache.heron.api.Config();

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
    org.apache.heron.api.Config topologyConfig = new org.apache.heron.api.Config();

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
