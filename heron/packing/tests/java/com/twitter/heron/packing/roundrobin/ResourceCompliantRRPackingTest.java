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

import java.util.HashSet;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.packing.AssertPacking;
import com.twitter.heron.spi.common.ClusterDefaults;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Constants;
import com.twitter.heron.spi.common.Context;
import com.twitter.heron.spi.common.Keys;
import com.twitter.heron.spi.packing.PackingPlan;
import com.twitter.heron.spi.packing.Resource;
import com.twitter.heron.spi.utils.TopologyTests;
import com.twitter.heron.spi.utils.TopologyUtils;

public class ResourceCompliantRRPackingTest {

  private static final String BOLT_NAME = "bolt";
  private static final String SPOUT_NAME = "spout";
  private static final int DEFAULT_CONTAINER_PADDING = 10;

  private long instanceRamDefault;
  private double instanceCpuDefault;
  private long instanceDiskDefault;

  private int countComponent(String component, Set<PackingPlan.InstancePlan> instances) {
    int count = 0;
    for (PackingPlan.InstancePlan instancePlan : instances) {
      if (component.equals(instancePlan.getComponentName())) {
        count++;
      }
    }
    return count;
  }

  private TopologyAPI.Topology getTopology(
      int spoutParallelism, int boltParallelism,
      com.twitter.heron.api.Config topologyConfig) {
    return TopologyTests.createTopology("testTopology", topologyConfig, SPOUT_NAME, BOLT_NAME,
        spoutParallelism, boltParallelism);
  }

  private PackingPlan getResourceCompliantRRPackingPlan(TopologyAPI.Topology topology) {
    Config config = Config.newBuilder()
        .put(Keys.topologyId(), topology.getId())
        .put(Keys.topologyName(), topology.getName())
        .putAll(ClusterDefaults.getDefaults())
        .build();

    this.instanceRamDefault = Context.instanceRam(config);
    this.instanceCpuDefault = Context.instanceCpu(config);
    this.instanceDiskDefault = Context.instanceDisk(config);

    ResourceCompliantRRPacking packing = new ResourceCompliantRRPacking();
    packing.initialize(config, topology);
    return packing.pack();
  }

  @Test(expected = RuntimeException.class)
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

    TopologyAPI.Topology topology = getTopology(spoutParallelism, boltParallelism, topologyConfig);

    getResourceCompliantRRPackingPlan(topology);
  }

  /**
   * Test the scenario where the max container size is the default
   */
  @Test
  public void testDefaultResources() throws Exception {
    int numContainers = 2;
    int spoutParallelism = 4;
    int boltParallelism = 3;
    Integer totalInstances = spoutParallelism + boltParallelism;

    // Set up the topology and its config
    com.twitter.heron.api.Config topologyConfig = new com.twitter.heron.api.Config();
    topologyConfig.put(com.twitter.heron.api.Config.TOPOLOGY_STMGRS, numContainers);

    // No explicit resources required
    TopologyAPI.Topology topologyNoExplicitResourcesConfig =
        getTopology(spoutParallelism, boltParallelism, topologyConfig);

    PackingPlan packingPlanNoExplicitResourcesConfig =
        getResourceCompliantRRPackingPlan(topologyNoExplicitResourcesConfig);

    Assert.assertEquals(packingPlanNoExplicitResourcesConfig.getContainers().size(), numContainers);
    Assert.assertEquals(totalInstances, packingPlanNoExplicitResourcesConfig.getInstanceCount());
  }

  /**
   * Test the scenario where the max container size is the default and padding is configured
   */
  @Test
  public void testDefaultContainerSizeWithPadding() throws Exception {
    int numContainers = 2;
    int spoutParallelism = 4;
    int boltParallelism = 3;
    int padding = 50;
    Integer totalInstances = spoutParallelism + boltParallelism;

    // Set up the topology and its config
    com.twitter.heron.api.Config topologyConfig = new com.twitter.heron.api.Config();
    topologyConfig.put(com.twitter.heron.api.Config.TOPOLOGY_STMGRS, numContainers);
    topologyConfig.setContainerPaddingPercentage(padding);
    TopologyAPI.Topology topology = getTopology(spoutParallelism, boltParallelism, topologyConfig);

    PackingPlan packingPlan = getResourceCompliantRRPackingPlan(topology);

    Assert.assertEquals(packingPlan.getContainers().size(), numContainers);
    Assert.assertEquals(totalInstances, packingPlan.getInstanceCount());
  }

  /**
   * Test the scenario where container level resource config are set
   */
  @Test
  public void testContainerRequestedResourcesSingleContainer() throws Exception {
    int numContainers = 1;
    int spoutParallelism = 4;
    int boltParallelism = 3;
    Integer totalInstances = spoutParallelism + boltParallelism;

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
        getResourceCompliantRRPackingPlan(topologyExplicitResourcesConfig);

    Assert.assertEquals(packingPlanExplicitResourcesConfig.getContainers().size(), numContainers);
    Assert.assertEquals(totalInstances, packingPlanExplicitResourcesConfig.getInstanceCount());

    for (PackingPlan.ContainerPlan containerPlan
        : packingPlanExplicitResourcesConfig.getContainers()) {
      Assert.assertEquals(Math.round(totalInstances * instanceCpuDefault
              + (DEFAULT_CONTAINER_PADDING / 100.0) * totalInstances * instanceCpuDefault),
          (long) containerPlan.getRequiredResource().getCpu());

      Assert.assertEquals(totalInstances * instanceRamDefault
              + (long) ((DEFAULT_CONTAINER_PADDING / 100.0) * totalInstances * instanceRamDefault),
          containerPlan.getRequiredResource().getRam());

      Assert.assertEquals(totalInstances * instanceDiskDefault
              + (long) ((DEFAULT_CONTAINER_PADDING / 100.0) * totalInstances * instanceDiskDefault),
          containerPlan.getRequiredResource().getDisk());

      // All instances' resource requirement should be equal
      // So the size of set should be 1
      Set<Resource> resources = new HashSet<>();
      for (PackingPlan.InstancePlan instancePlan : containerPlan.getInstances()) {
        resources.add(instancePlan.getResource());
      }

      Assert.assertEquals(1, resources.size());
      Assert.assertEquals(instanceRamDefault, resources.iterator().next().getRam());
    }
  }

  /**
   * Test the scenario where container level resource config are set
   */
  @Test
  public void testContainerRequestedResourcesTwoContainers() throws Exception {
    int numContainers = 2;
    int spoutParallelism = 4;
    int boltParallelism = 3;
    Integer totalInstances = spoutParallelism + boltParallelism;

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
        getResourceCompliantRRPackingPlan(topologyExplicitResourcesConfig);

    Assert.assertEquals(packingPlanExplicitResourcesConfig.getContainers().size(), numContainers);
    Assert.assertEquals(totalInstances, packingPlanExplicitResourcesConfig.getInstanceCount());

    // Ram for bolt/spout should be the value in component ram map
    for (PackingPlan.ContainerPlan containerPlan
        : packingPlanExplicitResourcesConfig.getContainers()) {
      for (PackingPlan.InstancePlan instancePlan : containerPlan.getInstances()) {
        Assert.assertEquals(instanceRamDefault, instancePlan.getResource().getRam());
      }
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
    Integer totalInstances = spoutParallelism + boltParallelism;

    // Set up the topology and its config
    com.twitter.heron.api.Config topologyConfig = new com.twitter.heron.api.Config();
    topologyConfig.put(com.twitter.heron.api.Config.TOPOLOGY_STMGRS, numContainers);

    // Explicit set resources for container
    // the value should be ignored, since we set the complete component ram map
    long containerRam = Long.MAX_VALUE;

    // Explicit set component ram map
    long boltRam = 1L * Constants.GB;

    topologyConfig.setContainerRamRequested(containerRam);
    topologyConfig.setComponentRam(BOLT_NAME, boltRam);

    TopologyAPI.Topology topologyExplicitRamMap =
        getTopology(spoutParallelism, boltParallelism, topologyConfig);
    PackingPlan packingPlanExplicitRamMap =
        getResourceCompliantRRPackingPlan(topologyExplicitRamMap);
    Assert.assertEquals(totalInstances, packingPlanExplicitRamMap.getInstanceCount());

    AssertPacking.assertContainers(packingPlanExplicitRamMap.getContainers(),
        BOLT_NAME, SPOUT_NAME, boltRam, instanceRamDefault, containerRam);
  }

  /**
   * Test the scenario ram map config is fully set
   */
  @Test
  public void testPartialRamMap() throws Exception {
    int numContainers = 2;
    int spoutParallelism = 4;
    int boltParallelism = 3;
    Integer totalInstances = spoutParallelism + boltParallelism;

    // Set up the topology and its config
    com.twitter.heron.api.Config topologyConfig = new com.twitter.heron.api.Config();
    topologyConfig.put(com.twitter.heron.api.Config.TOPOLOGY_STMGRS, numContainers);

    // Explicit set resources for container
    long containerRam = 10L * Constants.GB;

    // Explicit set component ram map
    long boltRam = 1L * Constants.GB;
    long spoutRam = 2L * Constants.GB;

    topologyConfig.setContainerRamRequested(containerRam);
    topologyConfig.setComponentRam(BOLT_NAME, boltRam);
    topologyConfig.setComponentRam(SPOUT_NAME, spoutRam);

    TopologyAPI.Topology topologyExplicitRamMap =
        getTopology(spoutParallelism, boltParallelism, topologyConfig);
    PackingPlan packingPlanExplicitRamMap =
        getResourceCompliantRRPackingPlan(topologyExplicitRamMap);
    Assert.assertEquals(totalInstances, packingPlanExplicitRamMap.getInstanceCount());

    AssertPacking.assertContainers(packingPlanExplicitRamMap.getContainers(),
        BOLT_NAME, SPOUT_NAME, boltRam, spoutRam, containerRam);
  }

  /**
   * Test the scenario where the user defined number of containers is not sufficient.
   */
  @Test
  public void testInsufficientContainersWithOneAdjustment() throws Exception {
    int numContainers = 1;
    int spoutParallelism = 4;
    int boltParallelism = 3;
    Integer totalInstances = spoutParallelism + boltParallelism;

    // Set up the topology and its config
    com.twitter.heron.api.Config topologyConfig = new com.twitter.heron.api.Config();
    topologyConfig.put(com.twitter.heron.api.Config.TOPOLOGY_STMGRS, numContainers);

    // Explicit set resources for container
    long containerRam = 2L * Constants.GB;

    topologyConfig.setContainerRamRequested(containerRam);

    TopologyAPI.Topology topology =
        getTopology(spoutParallelism, boltParallelism, topologyConfig);
    PackingPlan packingPlan =
        getResourceCompliantRRPackingPlan(topology);
    Assert.assertEquals(packingPlan.getContainers().size(), 4);
    Assert.assertEquals(totalInstances, packingPlan.getInstanceCount());
  }

  /**
   * Test the scenario where the user defined number of containers is not sufficient.
   */
  @Test
  public void testInsufficientContainersWithMultipleAdjustments() throws Exception {
    int numContainers = 1;
    int spoutParallelism = 4;
    int boltParallelism = 3;
    Integer totalInstances = spoutParallelism + boltParallelism;

    // Set up the topology and its config
    com.twitter.heron.api.Config topologyConfig = new com.twitter.heron.api.Config();
    topologyConfig.put(com.twitter.heron.api.Config.TOPOLOGY_STMGRS, numContainers);

    // Explicit set resources for container
    long containerRam = 2L * Constants.GB;

    // Explicit set component ram map
    long boltRam = 1L * Constants.GB;
    long spoutRam = 2L * Constants.GB;

    topologyConfig.setContainerRamRequested(containerRam);
    topologyConfig.setComponentRam(BOLT_NAME, boltRam);
    topologyConfig.setComponentRam(SPOUT_NAME, spoutRam);

    TopologyAPI.Topology topologyExplicitRamMap =
        getTopology(spoutParallelism, boltParallelism, topologyConfig);
    PackingPlan packingPlan =
        getResourceCompliantRRPackingPlan(topologyExplicitRamMap);
    Assert.assertEquals(packingPlan.getContainers().size(), 7);
    Assert.assertEquals(totalInstances, packingPlan.getInstanceCount());
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
    PackingPlan output = getResourceCompliantRRPackingPlan(topology);
    Assert.assertEquals(numContainers, output.getContainers().size());
    Assert.assertEquals((Integer) numInstance, output.getInstanceCount());

    for (PackingPlan.ContainerPlan container : output.getContainers()) {
      Assert.assertEquals(numInstance / numContainers, container.getInstances().size());
      Assert.assertEquals(
          2, countComponent("spout", container.getInstances()));
      Assert.assertEquals(
          2, countComponent("bolt", container.getInstances()));
    }
  }
}
