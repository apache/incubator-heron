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
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.spi.common.ClusterDefaults;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Constants;
import com.twitter.heron.spi.common.Keys;
import com.twitter.heron.spi.packing.PackingPlan;
import com.twitter.heron.spi.packing.Resource;
import com.twitter.heron.spi.utils.TopologyTests;
import com.twitter.heron.spi.utils.TopologyUtils;

public class RoundRobinPackingTest {
  private static final String BOLT_NAME = "bolt";
  private static final String SPOUT_NAME = "spout";
  private static final double DELTA = 0.1;

  private int countCompoment(String component, Set<PackingPlan.InstancePlan> instances) {
    int count = 0;
    for (PackingPlan.InstancePlan pair : instances) {
      if (component.equals(RoundRobinPacking.getComponentName(pair.getId()))) {
        count++;
      }
    }
    return count;
  }

  protected TopologyAPI.Topology getTopology(
      int spoutParallelism, int boltParallelism,
      com.twitter.heron.api.Config topologyConfig) {
    // Setup the spout parallelism
    Map<String, Integer> spouts = new HashMap<>();
    spouts.put(SPOUT_NAME, spoutParallelism);

    // Setup the bolt parallelism
    Map<String, Integer> bolts = new HashMap<>();
    bolts.put(BOLT_NAME, boltParallelism);

    TopologyAPI.Topology topology =
        TopologyTests.createTopology("testTopology", topologyConfig, spouts, bolts);

    return topology;
  }

  protected PackingPlan getRoundRobinPackingPlan(TopologyAPI.Topology topology) {
    Config config = Config.newBuilder()
        .put(Keys.topologyId(), topology.getId())
        .put(Keys.topologyName(), topology.getName())
        .putAll(ClusterDefaults.getDefaults())
        .build();

    RoundRobinPacking packing = new RoundRobinPacking();
    packing.initialize(config, topology);
    PackingPlan output = packing.pack();

    return output;
  }

  @Test
  public void testCheckFailure() throws Exception {
    int numContainers = 2;
    int spoutParallelism = 4;
    int boltParallelism = 3;

    // Set up the topology and its config
    com.twitter.heron.api.Config topologyConfig = new com.twitter.heron.api.Config();
    topologyConfig.put(com.twitter.heron.api.Config.TOPOLOGY_STMGRS, numContainers);

    // Explicit set insufficient ram for container
    long containerRam = -1L * Constants.GB;

    topologyConfig.setContainerRamRequested(containerRam);

    TopologyAPI.Topology topology =
        getTopology(spoutParallelism, boltParallelism, topologyConfig);

    PackingPlan packingPlan =
        getRoundRobinPackingPlan(topology);

    Assert.assertNull(packingPlan);
  }

  @Test
  public void testDefaultResources() throws Exception {
    int numContainers = 2;
    int spoutParallelism = 4;
    int boltParallelism = 3;

    // Set up the topology and its config
    com.twitter.heron.api.Config topologyConfig = new com.twitter.heron.api.Config();
    topologyConfig.put(com.twitter.heron.api.Config.TOPOLOGY_STMGRS, numContainers);

    // No explicit resources required
    TopologyAPI.Topology topologyNoExplicitResourcesConfig =
        getTopology(spoutParallelism, boltParallelism, topologyConfig);
    PackingPlan packingPlanNoExplicitResourcesConfig =
        getRoundRobinPackingPlan(topologyNoExplicitResourcesConfig);

    Assert.assertEquals(
        (Math.max(spoutParallelism, boltParallelism)
            + RoundRobinPacking.DEFAULT_CPU_PADDING_PER_CONTAINER) * (numContainers + 1),
        packingPlanNoExplicitResourcesConfig.getResource().cpu, DELTA);

    Assert.assertEquals(
        (spoutParallelism + boltParallelism) * Constants.GB
            + RoundRobinPacking.DEFAULT_RAM_PADDING_PER_CONTAINER * numContainers,
        packingPlanNoExplicitResourcesConfig.getResource().ram);

    Assert.assertEquals(
        (Math.max(spoutParallelism, boltParallelism) * Constants.GB
            + RoundRobinPacking.DEFAULT_DISK_PADDING_PER_CONTAINER) * (numContainers + 1),
        packingPlanNoExplicitResourcesConfig.getResource().disk);

    Assert.assertEquals(numContainers,
        packingPlanNoExplicitResourcesConfig.getContainers().size());
  }

  /**
   * Test the scenario container level resource config are set
   */
  @Test
  public void testContainerRequestedResources() throws Exception {
    int numContainers = 2;
    int spoutParallelism = 4;
    int boltParallelism = 3;

    // Set up the topology and its config
    com.twitter.heron.api.Config topologyConfig = new com.twitter.heron.api.Config();
    topologyConfig.put(com.twitter.heron.api.Config.TOPOLOGY_STMGRS, numContainers);
    // Explicit set resources for container
    long containerRam = 10L * Constants.GB;
    long containerDisk = 20L * Constants.GB;
    float containerCpu = 30;

    topologyConfig.setContainerRamRequested(containerRam);
    topologyConfig.setContainerDiskRequested(containerDisk);
    topologyConfig.setContainerCpuRequested(containerCpu);
    TopologyAPI.Topology topologyExplicitResourcesConfig =
        getTopology(spoutParallelism, boltParallelism, topologyConfig);
    PackingPlan packingPlanExplicitResourcesConfig =
        getRoundRobinPackingPlan(topologyExplicitResourcesConfig);

    Assert.assertEquals(containerCpu * (numContainers + 1),
        packingPlanExplicitResourcesConfig.getResource().cpu, DELTA);

    // The total recommended ram should be in the range of configured ram, account for rounding
    // errors
    Assert.assertEquals((double) containerRam * numContainers,
        (double) packingPlanExplicitResourcesConfig.getResource().ram,
        spoutParallelism + boltParallelism);

    Assert.assertEquals(containerDisk * (numContainers + 1),
        packingPlanExplicitResourcesConfig.getResource().disk);

    Assert.assertEquals(numContainers,
        packingPlanExplicitResourcesConfig.getContainers().size());

    for (PackingPlan.ContainerPlan containerPlan
        : packingPlanExplicitResourcesConfig.getContainers()) {
      Assert.assertEquals(containerCpu, containerPlan.getResource().cpu, DELTA);

      Assert.assertEquals((double) containerRam,
          (double) containerPlan.getResource().ram,
          containerPlan.getInstances().size());

      Assert.assertEquals(containerDisk, containerPlan.getResource().disk);

      // All instances' resource requirement should be equal
      // So the size of set should be 1
      Set<Resource> resources = new HashSet<>();
      for (PackingPlan.InstancePlan instancePlan : containerPlan.getInstances()) {
        resources.add(instancePlan.getResource());
      }

      Assert.assertEquals(1, resources.size());
      int instancesCount = containerPlan.getInstances().size();
      Assert.assertEquals(
          (containerRam - RoundRobinPacking.DEFAULT_RAM_PADDING_PER_CONTAINER) / instancesCount,
          resources.iterator().next().ram);
    }
  }

  /**
   * Test the scenario ram map config is partially set
   */
  @Test
  public void testCompleteRamMapRequested() throws Exception {
    int numContainers = 2;
    int spoutParallelism = 4;
    int boltParallelism = 3;

    // Set up the topology and its config
    com.twitter.heron.api.Config topologyConfig = new com.twitter.heron.api.Config();
    topologyConfig.put(com.twitter.heron.api.Config.TOPOLOGY_STMGRS, numContainers);

    // Explicit set resources for container
    // the value should be ignored, since we set the complete component ram map
    long containerRam = Long.MAX_VALUE;

    // Explicit set component ram map
    long boltRam = 1L * Constants.GB;
    long spoutRam = 2L * Constants.GB;

    topologyConfig.setContainerRamRequested(containerRam);
    topologyConfig.setComponentRam(BOLT_NAME, boltRam);
    topologyConfig.setComponentRam(SPOUT_NAME, spoutRam);

    TopologyAPI.Topology topologyExplicitRamMap =
        getTopology(spoutParallelism, boltParallelism, topologyConfig);
    PackingPlan packingPlanExplicitRamMap =
        getRoundRobinPackingPlan(topologyExplicitRamMap);

    // Ram for bolt should be the value in component ram map
    for (PackingPlan.ContainerPlan containerPlan
        : packingPlanExplicitRamMap.getContainers()) {
      // The containerRam should be ignored, since we set the complete component ram map
      Assert.assertNotEquals(containerRam, containerPlan.getResource().ram);
      for (PackingPlan.InstancePlan instancePlan : containerPlan.getInstances()) {
        if (instancePlan.getComponentName().equals(BOLT_NAME)) {
          Assert.assertEquals(boltRam, instancePlan.getResource().ram);
        }
        if (instancePlan.getComponentName().equals(SPOUT_NAME)) {
          Assert.assertEquals(spoutRam, instancePlan.getResource().ram);
        }
      }
    }
  }

  /**
   * Test the scenario ram map config is partially set
   */
  @Test
  public void testPartialRamMap() throws Exception {
    int numContainers = 2;
    int spoutParallelism = 4;
    int boltParallelism = 3;

    // Set up the topology and its config
    com.twitter.heron.api.Config topologyConfig = new com.twitter.heron.api.Config();
    topologyConfig.put(com.twitter.heron.api.Config.TOPOLOGY_STMGRS, numContainers);

    // Explicit set resources for container
    long containerRam = 10L * Constants.GB;

    // Explicit set component ram map
    long boltRam = 1L * Constants.GB;

    topologyConfig.setContainerRamRequested(containerRam);
    topologyConfig.setComponentRam(BOLT_NAME, boltRam);

    TopologyAPI.Topology topologyExplicitRamMap =
        getTopology(spoutParallelism, boltParallelism, topologyConfig);
    PackingPlan packingPlanExplicitRamMap =
        getRoundRobinPackingPlan(topologyExplicitRamMap);

    // Ram for bolt should be the value in component ram map
    for (PackingPlan.ContainerPlan containerPlan
        : packingPlanExplicitRamMap.getContainers()) {
      Assert.assertEquals(containerRam, containerPlan.getResource().ram);
      int boltCount = 0;
      int instancesCount = containerPlan.getInstances().size();
      for (PackingPlan.InstancePlan instancePlan : containerPlan.getInstances()) {
        if (instancePlan.getComponentName().equals(BOLT_NAME)) {
          Assert.assertEquals(boltRam, instancePlan.getResource().ram);
          boltCount++;
        }
      }

      // Ram for spout should be:
      // (containerRam - all ram for bolt - ram for padding) / (# of spouts)
      int spoutCount = instancesCount - boltCount;
      for (PackingPlan.InstancePlan instancePlan : containerPlan.getInstances()) {
        if (instancePlan.getComponentName().equals(SPOUT_NAME)) {
          Assert.assertEquals(
              (containerRam
                  - boltCount * boltRam
                  - RoundRobinPacking.DEFAULT_RAM_PADDING_PER_CONTAINER) / spoutCount,
              instancePlan.getResource().ram);
        }
      }
    }
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

    TopologyAPI.Topology topology =
        getTopology(componentParallelism, componentParallelism, topologyConfig);

    int numInstance = TopologyUtils.getTotalInstance(topology);
    // Two components
    Assert.assertEquals(2 * componentParallelism, numInstance);
    PackingPlan output = getRoundRobinPackingPlan(topology);
    Assert.assertEquals(numContainers, output.getContainers().size());

    for (PackingPlan.ContainerPlan container : output.getContainers()) {
      Assert.assertEquals(numInstance / numContainers, container.getInstances().size());

      // Verify each container got 2 spout and 2 bolt and container 1 got
      Assert.assertEquals(
          2, countCompoment("spout", container.getInstances()));
      Assert.assertEquals(
          2, countCompoment("bolt", container.getInstances()));
    }
  }
}
