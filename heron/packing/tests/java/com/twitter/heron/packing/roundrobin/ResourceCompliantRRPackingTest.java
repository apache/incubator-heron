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
  private static final int HERON_INTERNAL_CONTAINERS = 1;

  private long instanceRamDefault;
  private double instanceCpuDefault;
  private long instanceDiskDefault;

  private int countComponent(String component, Set<PackingPlan.InstancePlan> instances) {
    int count = 0;
    for (PackingPlan.InstancePlan pair : instances) {
      if (component.equals(RoundRobinPacking.getComponentName(pair.getId()))) {
        count++;
      }
    }
    return count;
  }

  private TopologyAPI.Topology getTopology(
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

  private PackingPlan getResourceCompliantRRPackingPlan(TopologyAPI.Topology topology) {
    Config config = Config.newBuilder()
        .put(Keys.topologyId(), topology.getId())
        .put(Keys.topologyName(), topology.getName())
        .putAll(ClusterDefaults.getDefaults())
        .build();

    this.instanceRamDefault = Context.instanceRam(config);
    this.instanceCpuDefault = Context.instanceCpu(config).doubleValue();
    this.instanceDiskDefault = Context.instanceDisk(config);

    Config runtime = Config.newBuilder()
        .put(Keys.topologyDefinition(), topology)
        .build();

    ResourceCompliantRRPacking packing = new ResourceCompliantRRPacking();
    packing.initialize(config, topology);
    PackingPlan output = packing.pack();

    return output;
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

    TopologyAPI.Topology topology =
        getTopology(spoutParallelism, boltParallelism, topologyConfig);

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
    int totalInstances = spoutParallelism + boltParallelism;

    // Set up the topology and its config
    com.twitter.heron.api.Config topologyConfig = new com.twitter.heron.api.Config();
    topologyConfig.put(com.twitter.heron.api.Config.TOPOLOGY_STMGRS, numContainers);

    // No explicit resources required
    TopologyAPI.Topology topologyNoExplicitResourcesConfig =
        getTopology(spoutParallelism, boltParallelism, topologyConfig);

    PackingPlan packingPlanNoExplicitResourcesConfig =
        getResourceCompliantRRPackingPlan(topologyNoExplicitResourcesConfig);

    Assert.assertEquals(packingPlanNoExplicitResourcesConfig.getContainers().size(), numContainers);

    //The first container consists 2 spouts and 2 bolts (4 instances) and the second container
    //consists of 2 spouts and 1 bolt (3 instances)
    Assert.assertEquals((long) (Math.round(4 * instanceCpuDefault
            + DEFAULT_CONTAINER_PADDING / 100.0 * (4 * instanceCpuDefault))
            + Math.round(3 * instanceCpuDefault
            + DEFAULT_CONTAINER_PADDING / 100.0 * (3 * instanceCpuDefault))
            + instanceCpuDefault),
        (long) packingPlanNoExplicitResourcesConfig.getResource().getCpu());

    Assert.assertEquals((4 * instanceRamDefault)
            + (long) ((DEFAULT_CONTAINER_PADDING / 100.0) * (4 * instanceRamDefault))
            + 3 * instanceRamDefault
            + (long) ((DEFAULT_CONTAINER_PADDING / 100.0) * (3 * instanceRamDefault))
            + instanceRamDefault,
        packingPlanNoExplicitResourcesConfig.getResource().getRam());

    Assert.assertEquals(4 * instanceDiskDefault
            + (long) ((DEFAULT_CONTAINER_PADDING / 100.0) * (4 * instanceDiskDefault))
            + 3 * instanceDiskDefault
            + (long) ((DEFAULT_CONTAINER_PADDING / 100.0) * (3 * instanceDiskDefault))
            + instanceDiskDefault,
        packingPlanNoExplicitResourcesConfig.getResource().getDisk());
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
    int totalInstances = spoutParallelism + boltParallelism;
    // Set up the topology and its config
    com.twitter.heron.api.Config topologyConfig = new com.twitter.heron.api.Config();
    topologyConfig.put(com.twitter.heron.api.Config.TOPOLOGY_STMGRS, numContainers);
    topologyConfig.setContainerPaddingPercentage(padding);
    TopologyAPI.Topology topology =
        getTopology(spoutParallelism, boltParallelism, topologyConfig);

    PackingPlan packingPlan =
        getResourceCompliantRRPackingPlan(topology);

    Assert.assertEquals(packingPlan.getContainers().size(), numContainers);

    //The first container consists 2 spouts and 2 bolts (4 instances) and the second container
    //consists of 2 spouts and 1 bolt (3 instances)
    Assert.assertEquals((long) (Math.round(4 * instanceCpuDefault
            + padding / 100.0 * (4 * instanceCpuDefault))
            + Math.round(3 * instanceCpuDefault
            + padding / 100.0 * (3 * instanceCpuDefault))
            + instanceCpuDefault),
        (long) packingPlan.getResource().getCpu());

    Assert.assertEquals((4 * instanceRamDefault)
            + (long) ((padding / 100.0) * (4 * instanceRamDefault))
            + 3 * instanceRamDefault
            + (long) ((padding / 100.0) * (3 * instanceRamDefault))
            + instanceRamDefault,
        packingPlan.getResource().getRam());

    Assert.assertEquals(4 * instanceDiskDefault
            + (long) ((padding / 100.0) * (4 * instanceDiskDefault))
            + 3 * instanceDiskDefault
            + (long) ((padding / 100.0) * (3 * instanceDiskDefault))
            + instanceDiskDefault,
        packingPlan.getResource().getDisk());
  }

  /**
   * Test the scenario where container level resource config are set
   */
  @Test
  public void testContainerRequestedResourcesSingleContainer() throws Exception {
    int numContainers = 1;
    int spoutParallelism = 4;
    int boltParallelism = 3;
    int totalInstances = spoutParallelism + boltParallelism;

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

    Assert.assertEquals(Math.round(totalInstances * instanceCpuDefault
            + (DEFAULT_CONTAINER_PADDING / 100.0) * totalInstances * instanceCpuDefault
            + instanceCpuDefault),
        (long) packingPlanExplicitResourcesConfig.getResource().getCpu());

    Assert.assertEquals(totalInstances * instanceRamDefault
            + (long) ((DEFAULT_CONTAINER_PADDING / 100.0) * totalInstances * instanceRamDefault)
            + instanceRamDefault,
        packingPlanExplicitResourcesConfig.getResource().getRam());

    Assert.assertEquals(totalInstances * instanceDiskDefault
            + (long) ((DEFAULT_CONTAINER_PADDING / 100.0) * totalInstances * instanceDiskDefault)
            + instanceDiskDefault,
        packingPlanExplicitResourcesConfig.getResource().getDisk());

    for (PackingPlan.ContainerPlan containerPlan
        : packingPlanExplicitResourcesConfig.getContainers()) {
      Assert.assertEquals(Math.round(totalInstances * instanceCpuDefault
              + (DEFAULT_CONTAINER_PADDING / 100.0) * totalInstances * instanceCpuDefault),
          (long) containerPlan.getResource().getCpu());

      Assert.assertEquals(totalInstances * instanceRamDefault
              + (long) ((DEFAULT_CONTAINER_PADDING / 100.0) * totalInstances * instanceRamDefault),
          containerPlan.getResource().getRam());

      Assert.assertEquals(totalInstances * instanceDiskDefault
              + (long) ((DEFAULT_CONTAINER_PADDING / 100.0) * totalInstances * instanceDiskDefault),
          containerPlan.getResource().getDisk());

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

    //The first container consists 2 spouts and 2 bolts (4 instances) and the second container
    //consists of 2 spouts and 1 bolt (3 instances)
    Assert.assertEquals((long) (Math.round(4 * instanceCpuDefault
            + DEFAULT_CONTAINER_PADDING / 100.0 * (4 * instanceCpuDefault))
            + Math.round(3 * instanceCpuDefault
            + DEFAULT_CONTAINER_PADDING / 100.0 * (3 * instanceCpuDefault))
            + instanceCpuDefault),
        (long) packingPlanExplicitResourcesConfig.getResource().getCpu());

    Assert.assertEquals((4 * instanceRamDefault)
            + (long) ((DEFAULT_CONTAINER_PADDING / 100.0) * (4 * instanceRamDefault))
            + 3 * instanceRamDefault
            + (long) ((DEFAULT_CONTAINER_PADDING / 100.0) * (3 * instanceRamDefault))
            + instanceRamDefault,
        packingPlanExplicitResourcesConfig.getResource().getRam());

    Assert.assertEquals(4 * instanceDiskDefault
            + (long) ((DEFAULT_CONTAINER_PADDING / 100.0) * (4 * instanceDiskDefault))
            + 3 * instanceDiskDefault
            + (long) ((DEFAULT_CONTAINER_PADDING / 100.0) * (3 * instanceDiskDefault))
            + instanceDiskDefault,
        packingPlanExplicitResourcesConfig.getResource().getDisk());

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

    // Ram for bolt should be the value in component ram map
    for (PackingPlan.ContainerPlan containerPlan
        : packingPlanExplicitRamMap.getContainers()) {
      // The containerRam should be ignored, since we set the complete component ram map
      Assert.assertNotEquals(containerRam, containerPlan.getResource().getRam());
      for (PackingPlan.InstancePlan instancePlan : containerPlan.getInstances()) {
        if (instancePlan.getComponentName().equals(BOLT_NAME)) {
          Assert.assertEquals(boltRam, instancePlan.getResource().getRam());
        }
        if (instancePlan.getComponentName().equals(SPOUT_NAME)) {
          Assert.assertEquals(instanceRamDefault, instancePlan.getResource().getRam());
        }
      }
    }
  }

  /**
   * Test the scenario ram map config is fully set
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
    long spoutRam = 2L * Constants.GB;

    topologyConfig.setContainerRamRequested(containerRam);
    topologyConfig.setComponentRam(BOLT_NAME, boltRam);
    topologyConfig.setComponentRam(SPOUT_NAME, spoutRam);

    TopologyAPI.Topology topologyExplicitRamMap =
        getTopology(spoutParallelism, boltParallelism, topologyConfig);
    PackingPlan packingPlanExplicitRamMap =
        getResourceCompliantRRPackingPlan(topologyExplicitRamMap);

    // Ram for bolt should be the value in component ram map
    for (PackingPlan.ContainerPlan containerPlan
        : packingPlanExplicitRamMap.getContainers()) {
      Assert.assertNotEquals(containerRam, containerPlan.getResource().getRam());
      for (PackingPlan.InstancePlan instancePlan : containerPlan.getInstances()) {
        if (instancePlan.getComponentName().equals(BOLT_NAME)) {
          Assert.assertEquals(boltRam, instancePlan.getResource().getRam());
        }
        if (instancePlan.getComponentName().equals(SPOUT_NAME)) {
          Assert.assertEquals(spoutRam, instancePlan.getResource().getRam());
        }
      }
    }
  }

  /**
   * Test the scenario where the user defined number of containers is not sufficient.
   */
  @Test
  public void testInsufficientContainersWithOneAdjustment() throws Exception {
    int numContainers = 1;
    int spoutParallelism = 4;
    int boltParallelism = 3;

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

    //The first 3 containers consist of 1 spout and 1 bolt (2 instances) and the fourth container
    //consists of 1 spout (1 instance)
    Assert.assertEquals((long) (Math.round(2 * instanceCpuDefault
            + DEFAULT_CONTAINER_PADDING / 100.0 * (2 * instanceCpuDefault))
            + Math.round(2 * instanceCpuDefault
            + DEFAULT_CONTAINER_PADDING / 100.0 * (2 * instanceCpuDefault))
            + Math.round(2 * instanceCpuDefault
            + DEFAULT_CONTAINER_PADDING / 100.0 * (2 * instanceCpuDefault))
            + Math.round(HERON_INTERNAL_CONTAINERS * instanceCpuDefault
            + DEFAULT_CONTAINER_PADDING / 100.0 * (HERON_INTERNAL_CONTAINERS * instanceCpuDefault))
            + instanceCpuDefault),
        (long) packingPlan.getResource().getCpu());

    Assert.assertEquals(2 * instanceRamDefault
            + (long) ((DEFAULT_CONTAINER_PADDING / 100.0) * (2 * instanceRamDefault))
            + 2 * instanceRamDefault
            + (long) ((DEFAULT_CONTAINER_PADDING / 100.0) * (2 * instanceRamDefault))
            + 2 * instanceRamDefault
            + (long) ((DEFAULT_CONTAINER_PADDING / 100.0) * (2 * instanceRamDefault))
            + HERON_INTERNAL_CONTAINERS * instanceRamDefault
            + (long) ((DEFAULT_CONTAINER_PADDING / 100.0)
            * (HERON_INTERNAL_CONTAINERS * instanceRamDefault))
            + instanceRamDefault,
        packingPlan.getResource().getRam());

    Assert.assertEquals(2 * instanceDiskDefault
            + (long) ((DEFAULT_CONTAINER_PADDING / 100.0) * (2 * instanceDiskDefault))
            + 2 * instanceDiskDefault
            + (long) ((DEFAULT_CONTAINER_PADDING / 100.0) * (2 * instanceDiskDefault))
            + 2 * instanceDiskDefault
            + (long) ((DEFAULT_CONTAINER_PADDING / 100.0) * (2 * instanceDiskDefault))
            + HERON_INTERNAL_CONTAINERS * instanceDiskDefault
            + (long) ((DEFAULT_CONTAINER_PADDING / 100.0)
            * (HERON_INTERNAL_CONTAINERS * instanceDiskDefault))
            + instanceDiskDefault,
        packingPlan.getResource().getDisk());
  }

  /**
   * Test the scenario where the user defined number of containers is not sufficient.
   */
  @Test
  public void testInsufficientContainersWithMultipleAdjustments() throws Exception {
    int numContainers = 1;
    int spoutParallelism = 4;
    int boltParallelism = 3;

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

    for (PackingPlan.ContainerPlan container : output.getContainers()) {
      Assert.assertEquals(numInstance / numContainers, container.getInstances().size());
      Assert.assertEquals(
          2, countComponent("spout", container.getInstances()));
      Assert.assertEquals(
          2, countComponent("bolt", container.getInstances()));
    }
  }
}
