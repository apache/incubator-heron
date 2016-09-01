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

package com.twitter.heron.packing.binpacking;

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

public class FirstFitDecreasingPackingTest {
  private static final String BOLT_NAME = "bolt";
  private static final String SPOUT_NAME = "spout";
  private static final int DEFAULT_CONTAINER_PADDING = 10;
  private static final int HERON_INTERNAL_CONTAINERS = 1;

  private long instanceRamDefault;
  private double instanceCpuDefault;
  private long instanceDiskDefault;

  private int countComponent(String component, Map<String, PackingPlan.InstancePlan> instances) {
    int count = 0;
    for (PackingPlan.InstancePlan pair : instances.values()) {
      if (component.equals(FirstFitDecreasingPacking.getComponentName(pair.getId()))) {
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

  protected PackingPlan getFirstFitDecreasingPackingPlan(TopologyAPI.Topology topology) {
    Config config = Config.newBuilder()
        .put(Keys.topologyId(), topology.getId())
        .put(Keys.topologyName(), topology.getName())
        .putAll(ClusterDefaults.getDefaults())
        .build();

    this.instanceRamDefault = Context.instanceRam(config);
    this.instanceCpuDefault = Context.instanceCpu(config).doubleValue();
    this.instanceDiskDefault = Context.instanceDisk(config);

    FirstFitDecreasingPacking packing = new FirstFitDecreasingPacking();

    packing.initialize(config, topology);
    PackingPlan output = packing.pack();

    return output;
  }

  protected PackingPlan getFirstFitDecreasingPackingPlanRepack(
      TopologyAPI.Topology topology,
      PackingPlan currentPackingPlan,
      Map<String, Integer> componentChanges) {
    Config config = Config.newBuilder()
        .put(Keys.topologyId(), topology.getId())
        .put(Keys.topologyName(), topology.getName())
        .putAll(ClusterDefaults.getDefaults())
        .build();

    this.instanceRamDefault = Context.instanceRam(config);
    this.instanceCpuDefault = Context.instanceCpu(config).doubleValue();
    this.instanceDiskDefault = Context.instanceDisk(config);

    FirstFitDecreasingPacking packing = new FirstFitDecreasingPacking();
    packing.initialize(config, topology);
    PackingPlan output = packing.repack(currentPackingPlan, componentChanges);

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

    topologyConfig.setContainerMaxRamHint(containerRam);

    TopologyAPI.Topology topology =
        getTopology(spoutParallelism, boltParallelism, topologyConfig);

    getFirstFitDecreasingPackingPlan(topology);
  }

  /**
   * Test the scenario where the max container size is the default
   */
  @Test
  public void testDefaultContainerSize() throws Exception {
    int spoutParallelism = 4;
    int boltParallelism = 3;
    int totalInstances = spoutParallelism + boltParallelism;
    // Set up the topology and its config
    com.twitter.heron.api.Config topologyConfig = new com.twitter.heron.api.Config();

    TopologyAPI.Topology topology =
        getTopology(spoutParallelism, boltParallelism, topologyConfig);

    PackingPlan packingPlan =
        getFirstFitDecreasingPackingPlan(topology);

    Assert.assertEquals(packingPlan.getContainers().size(), 2);

    long totalRam = (totalInstances + HERON_INTERNAL_CONTAINERS)
        * instanceRamDefault
        + (long) ((DEFAULT_CONTAINER_PADDING / 100.0
        * totalInstances * instanceRamDefault));

    Assert.assertEquals(packingPlan.getResource().ram, totalRam);

    double totalCpu = Math.round(spoutParallelism * instanceCpuDefault
        + (DEFAULT_CONTAINER_PADDING / 100.0 * spoutParallelism * instanceCpuDefault))
        + Math.round(boltParallelism * instanceCpuDefault
        + (DEFAULT_CONTAINER_PADDING / 100.0 * boltParallelism * instanceCpuDefault))
        + instanceCpuDefault;

    Assert.assertEquals((long) packingPlan.getResource().cpu, (long) totalCpu);

    long totalDisk = (totalInstances + HERON_INTERNAL_CONTAINERS)
        * instanceDiskDefault
        + (long) ((DEFAULT_CONTAINER_PADDING / 100.0
        * totalInstances * instanceDiskDefault));

    Assert.assertEquals(packingPlan.getResource().disk, totalDisk);
  }

  /**
   * Test the scenario where the max container size is the default but padding is configured
   */
  @Test
  public void testDefaultContainerSizeWithPadding() throws Exception {
    int spoutParallelism = 4;
    int boltParallelism = 3;
    int padding = 50;
    int totalInstances = spoutParallelism + boltParallelism;
    // Set up the topology and its config
    com.twitter.heron.api.Config topologyConfig = new com.twitter.heron.api.Config();

    topologyConfig.setContainerPaddingPercentage(padding);
    TopologyAPI.Topology topology =
        getTopology(spoutParallelism, boltParallelism, topologyConfig);

    PackingPlan packingPlan =
        getFirstFitDecreasingPackingPlan(topology);

    Assert.assertEquals(packingPlan.getContainers().size(), 2);

    long totalRam = (totalInstances + HERON_INTERNAL_CONTAINERS)
        * instanceRamDefault
        + ((long) (padding / 100.0 * totalInstances * instanceRamDefault));

    Assert.assertEquals(packingPlan.getResource().ram, totalRam);

    double totalCpu = Math.round(spoutParallelism * instanceCpuDefault
        + (padding / 100.0 * spoutParallelism * instanceCpuDefault))
        + Math.round(boltParallelism * instanceCpuDefault
        + (padding / 100.0 * boltParallelism * instanceCpuDefault))
        + instanceCpuDefault;

    Assert.assertEquals((long) packingPlan.getResource().cpu, (long) totalCpu);

    long totalDisk = (totalInstances + HERON_INTERNAL_CONTAINERS)
        * instanceDiskDefault
        + (long) (padding / 100.0 * totalInstances * instanceDiskDefault);

    Assert.assertEquals(packingPlan.getResource().disk, totalDisk);
  }


  /**
   * Test the scenario where container level resource config are set
   */
  @Test
  public void testContainerRequestedResources() throws Exception {
    int spoutParallelism = 4;
    int boltParallelism = 3;
    int totalInstances = spoutParallelism + boltParallelism;

    // Set up the topology and its config
    com.twitter.heron.api.Config topologyConfig = new com.twitter.heron.api.Config();

    // Explicit set resources for container
    long containerRam = 10L * Constants.GB;
    long containerDisk = 20L * Constants.GB;
    float containerCpu = 30;

    topologyConfig.setContainerMaxRamHint(containerRam);
    topologyConfig.setContainerMaxDiskHint(containerDisk);
    topologyConfig.setContainerMaxCpuHint(containerCpu);

    TopologyAPI.Topology topologyExplicitResourcesConfig =
        getTopology(spoutParallelism, boltParallelism, topologyConfig);
    PackingPlan packingPlanExplicitResourcesConfig =
        getFirstFitDecreasingPackingPlan(topologyExplicitResourcesConfig);

    Assert.assertEquals(packingPlanExplicitResourcesConfig.getContainers().size(), 1);


    Assert.assertEquals(Math.round(totalInstances * instanceCpuDefault
            + (DEFAULT_CONTAINER_PADDING / 100.0 * totalInstances * instanceCpuDefault)
            + instanceCpuDefault),
        (long) packingPlanExplicitResourcesConfig.getResource().cpu);

    Assert.assertEquals(totalInstances * instanceRamDefault
            + (long) (DEFAULT_CONTAINER_PADDING / 100.0 * totalInstances * instanceRamDefault)
            + instanceRamDefault,
        packingPlanExplicitResourcesConfig.getResource().ram);

    Assert.assertEquals(totalInstances * instanceDiskDefault
            + (long) (DEFAULT_CONTAINER_PADDING / 100.0 * totalInstances * instanceDiskDefault)
            + instanceDiskDefault,
        packingPlanExplicitResourcesConfig.getResource().disk);

    for (PackingPlan.ContainerPlan containerPlan
        : packingPlanExplicitResourcesConfig.getContainers()) {
      Assert.assertEquals(Math.round(totalInstances * instanceCpuDefault
              + (DEFAULT_CONTAINER_PADDING / 100.0 * totalInstances * instanceCpuDefault)),
          (long) containerPlan.getResource().cpu);

      Assert.assertEquals(totalInstances * instanceRamDefault
              + (long) (DEFAULT_CONTAINER_PADDING / 100.0 * totalInstances * instanceRamDefault),
          containerPlan.getResource().ram);

      Assert.assertEquals(totalInstances * instanceDiskDefault
              + (long) (DEFAULT_CONTAINER_PADDING / 100.0 * totalInstances * instanceDiskDefault),
          containerPlan.getResource().disk);

      // All instances' resource requirement should be equal
      // So the size of set should be 1
      Set<Resource> resources = new HashSet<>();
      for (PackingPlan.InstancePlan instancePlan : containerPlan.getInstances()) {
        resources.add(instancePlan.getResource());
      }

      Assert.assertEquals(1, resources.size());
      Assert.assertEquals(instanceRamDefault, resources.iterator().next().ram);
    }
  }

  /**
   * Test the scenario ram map config is fully set
   */
  @Test
  public void testCompleteRamMapRequested() throws Exception {
    int spoutParallelism = 4;
    int boltParallelism = 3;
    int totalInstances = spoutParallelism + boltParallelism;

    // Set up the topology and its config
    com.twitter.heron.api.Config topologyConfig = new com.twitter.heron.api.Config();

    // Explicit set max resources for container
    // the value should be ignored, since we set the complete component ram map
    long maxContainerRam = 15L * Constants.GB;
    long maxContainerDisk = 20L * Constants.GB;
    float maxContainerCpu = 30;

    // Explicit set component ram map
    long boltRam = 1L * Constants.GB;
    long spoutRam = 2L * Constants.GB;

    topologyConfig.setContainerMaxRamHint(maxContainerRam);
    topologyConfig.setContainerMaxDiskHint(maxContainerDisk);
    topologyConfig.setContainerMaxCpuHint(maxContainerCpu);
    topologyConfig.setComponentRam(BOLT_NAME, boltRam);
    topologyConfig.setComponentRam(SPOUT_NAME, spoutRam);

    TopologyAPI.Topology topologyExplicitRamMap =
        getTopology(spoutParallelism, boltParallelism, topologyConfig);
    PackingPlan packingPlanExplicitRamMap =
        getFirstFitDecreasingPackingPlan(topologyExplicitRamMap);

    Assert.assertEquals(packingPlanExplicitRamMap.getContainers().size(), 1);

    Assert.assertEquals(Math.round(totalInstances * instanceCpuDefault
            + DEFAULT_CONTAINER_PADDING / 100.0 * (totalInstances * instanceCpuDefault)
            + instanceCpuDefault),
        (long) packingPlanExplicitRamMap.getResource().cpu);

    Assert.assertEquals(spoutParallelism * spoutRam + boltParallelism * boltRam
            + (long) (DEFAULT_CONTAINER_PADDING / 100.0 * (spoutParallelism * spoutRam
            + boltParallelism * boltRam))
            + instanceRamDefault,
        packingPlanExplicitRamMap.getResource().ram);

    Assert.assertEquals(totalInstances * instanceDiskDefault
            + (long) (DEFAULT_CONTAINER_PADDING / 100.0 * (totalInstances * instanceDiskDefault))
            + instanceDiskDefault,
        packingPlanExplicitRamMap.getResource().disk);

    // Ram for bolt should be the value in component ram map
    for (PackingPlan.ContainerPlan containerPlan
        : packingPlanExplicitRamMap.getContainers()) {
      Assert.assertNotEquals(maxContainerRam, containerPlan.getResource().ram);
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
   * Test the scenario ram map config is fully set
   */
  @Test
  public void testCompleteRamMapRequested2() throws Exception {
    int spoutParallelism = 4;
    int boltParallelism = 3;

    // Set up the topology and its config
    com.twitter.heron.api.Config topologyConfig = new com.twitter.heron.api.Config();

    long maxContainerRam = 10L * Constants.GB;

    // Explicit set component ram map
    long boltRam = 1L * Constants.GB;
    long spoutRam = 2L * Constants.GB;

    topologyConfig.setContainerMaxRamHint(maxContainerRam);
    topologyConfig.setComponentRam(BOLT_NAME, boltRam);
    topologyConfig.setComponentRam(SPOUT_NAME, spoutRam);

    TopologyAPI.Topology topologyExplicitRamMap =
        getTopology(spoutParallelism, boltParallelism, topologyConfig);
    PackingPlan packingPlanExplicitRamMap =
        getFirstFitDecreasingPackingPlan(topologyExplicitRamMap);

    Assert.assertEquals(packingPlanExplicitRamMap.getContainers().size(), 2);

    Assert.assertEquals((long) (Math.round(spoutParallelism * instanceCpuDefault
            + DEFAULT_CONTAINER_PADDING / 100.0 * (spoutParallelism * instanceCpuDefault))
            + Math.round(boltParallelism * instanceCpuDefault
            + DEFAULT_CONTAINER_PADDING / 100.0 * (boltParallelism * instanceCpuDefault))
            + instanceCpuDefault),
        (long) packingPlanExplicitRamMap.getResource().cpu);

    Assert.assertEquals((spoutParallelism * spoutRam)
            + (long) (DEFAULT_CONTAINER_PADDING / 100.0 * (spoutParallelism * spoutRam))
            + boltParallelism * boltRam
            + (long) (DEFAULT_CONTAINER_PADDING / 100.0 * (boltParallelism * boltRam))
            + instanceRamDefault,
        packingPlanExplicitRamMap.getResource().ram);

    Assert.assertEquals(spoutParallelism * instanceDiskDefault
            + (long) (DEFAULT_CONTAINER_PADDING / 100.0 * (spoutParallelism * instanceDiskDefault))
            + boltParallelism * instanceDiskDefault
            + (long) (DEFAULT_CONTAINER_PADDING / 100.0 * (boltParallelism * instanceDiskDefault))
            + instanceDiskDefault,
        packingPlanExplicitRamMap.getResource().disk);

    // Ram for bolt/spout should be the value in component ram map
    for (PackingPlan.ContainerPlan containerPlan
        : packingPlanExplicitRamMap.getContainers()) {
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
    int spoutParallelism = 4;
    int boltParallelism = 3;

    // Set up the topology and its config
    com.twitter.heron.api.Config topologyConfig = new com.twitter.heron.api.Config();

    // Explicit set resources for container
    long maxContainerRam = 10L * Constants.GB;

    // Explicit set component ram map
    long boltRam = 4L * Constants.GB;

    topologyConfig.setContainerMaxRamHint(maxContainerRam);
    topologyConfig.setComponentRam(BOLT_NAME, boltRam);

    TopologyAPI.Topology topologyExplicitRamMap =
        getTopology(spoutParallelism, boltParallelism, topologyConfig);
    PackingPlan packingPlanExplicitRamMap =
        getFirstFitDecreasingPackingPlan(topologyExplicitRamMap);

    Assert.assertEquals(packingPlanExplicitRamMap.getContainers().size(), 2);

    Assert.assertEquals((long) (Math.round(4 * instanceCpuDefault
            + DEFAULT_CONTAINER_PADDING / 100.0 * (4 * instanceCpuDefault))
            + Math.round(3 * instanceCpuDefault
            + DEFAULT_CONTAINER_PADDING / 100.0 * (3 * instanceCpuDefault))
            + instanceCpuDefault),
        (long) packingPlanExplicitRamMap.getResource().cpu);

    Assert.assertEquals(2 * boltRam + 2 * instanceRamDefault
            + (long) (DEFAULT_CONTAINER_PADDING / 100.0 * (2 * boltRam + 2 * instanceRamDefault))
            + boltRam + 2 * instanceRamDefault
            + (long) (DEFAULT_CONTAINER_PADDING / 100.0 * (boltRam + 2 * instanceRamDefault))
            + instanceRamDefault,
        packingPlanExplicitRamMap.getResource().ram);

    Assert.assertEquals(4 * instanceDiskDefault
            + (long) (DEFAULT_CONTAINER_PADDING / 100.0 * (4 * instanceDiskDefault))
            + 3 * instanceDiskDefault
            + (long) (DEFAULT_CONTAINER_PADDING / 100.0 * (3 * instanceDiskDefault))
            + instanceDiskDefault,
        packingPlanExplicitRamMap.getResource().disk);

    for (PackingPlan.ContainerPlan containerPlan
        : packingPlanExplicitRamMap.getContainers()) {
      for (PackingPlan.InstancePlan instancePlan : containerPlan.getInstances()) {
        // Ram for bolt should be the value in component ram map
        if (instancePlan.getComponentName().equals(BOLT_NAME)) {
          Assert.assertEquals(boltRam, instancePlan.getResource().ram);
        }
        if (instancePlan.getComponentName().equals(SPOUT_NAME)) {
          Assert.assertEquals(instanceRamDefault, instancePlan.getResource().ram);
        }
      }
    }
  }

  /**
   * Test the scenario ram map config is partially set and padding is configured
   */
  @Test
  public void testPartialRamMapWithPadding() throws Exception {
    int spoutParallelism = 4;
    int boltParallelism = 3;
    int padding = 0;
    // Set up the topology and its config
    com.twitter.heron.api.Config topologyConfig = new com.twitter.heron.api.Config();

    topologyConfig.setContainerPaddingPercentage(0);
    // Explicit set resources for container
    long maxContainerRam = 10L * Constants.GB;

    // Explicit set component ram map
    long boltRam = 4L * Constants.GB;

    topologyConfig.setContainerMaxRamHint(maxContainerRam);
    topologyConfig.setComponentRam(BOLT_NAME, boltRam);

    TopologyAPI.Topology topologyExplicitRamMap =
        getTopology(spoutParallelism, boltParallelism, topologyConfig);
    PackingPlan packingPlanExplicitRamMap =
        getFirstFitDecreasingPackingPlan(topologyExplicitRamMap);

    Assert.assertEquals(packingPlanExplicitRamMap.getContainers().size(), 2);

    Assert.assertEquals((long) (Math.round(4 * instanceCpuDefault
            + padding / 100.0 * (4 * instanceCpuDefault))
            + Math.round(3 * instanceCpuDefault
            + padding / 100.0 * (3 * instanceCpuDefault))
            + instanceCpuDefault),
        (long) packingPlanExplicitRamMap.getResource().cpu);

    Assert.assertEquals(2 * boltRam + 2 * instanceRamDefault
            + (long) (padding / 100.0 * (2 * boltRam + 2 * instanceRamDefault))
            + boltRam + 2 * instanceRamDefault
            + (long) (padding / 100.0 * (boltRam + 2 * instanceRamDefault))
            + instanceRamDefault,
        packingPlanExplicitRamMap.getResource().ram);

    Assert.assertEquals(4 * instanceDiskDefault
            + (long) (padding / 100.0 * (4 * instanceDiskDefault))
            + 3 * instanceDiskDefault
            + (long) (padding / 100.0 * (3 * instanceDiskDefault))
            + instanceDiskDefault,
        packingPlanExplicitRamMap.getResource().disk);

    for (PackingPlan.ContainerPlan containerPlan
        : packingPlanExplicitRamMap.getContainers()) {
      for (PackingPlan.InstancePlan instancePlan : containerPlan.getInstances()) {
        // Ram for bolt should be the value in component ram map
        if (instancePlan.getComponentName().equals(BOLT_NAME)) {
          Assert.assertEquals(boltRam, instancePlan.getResource().ram);
        }
        if (instancePlan.getComponentName().equals(SPOUT_NAME)) {
          Assert.assertEquals(instanceRamDefault, instancePlan.getResource().ram);
        }
      }
    }
  }

  /**
   * Test invalid ram for instance
   */
  @Test(expected = RuntimeException.class)
  public void testInvalidRamInstance() throws Exception {
    int spoutParallelism = 4;
    int boltParallelism = 3;

    // Set up the topology and its config
    com.twitter.heron.api.Config topologyConfig = new com.twitter.heron.api.Config();

    // Explicit set resources for container
    long maxContainerRam = 10L * Constants.GB;

    // Explicit set component ram map
    long boltRam = 0L * Constants.GB;

    topologyConfig.setContainerMaxRamHint(maxContainerRam);
    topologyConfig.setComponentRam(BOLT_NAME, boltRam);

    TopologyAPI.Topology topologyExplicitRamMap =
        getTopology(spoutParallelism, boltParallelism, topologyConfig);
    PackingPlan packingPlanExplicitRamMap =
        getFirstFitDecreasingPackingPlan(topologyExplicitRamMap);
  }


  /**
   * Test the scenario where the max container size is the default
   * and scaling is requested.
   */
  @Test
  public void testDefaultContainerSizeRepack() throws Exception {
    int spoutParallelism = 4;
    int boltParallelism = 3;
    int totalInstances = spoutParallelism + boltParallelism;
    // Set up the topology and its config
    com.twitter.heron.api.Config topologyConfig = new com.twitter.heron.api.Config();

    TopologyAPI.Topology topology =
        getTopology(spoutParallelism, boltParallelism, topologyConfig);

    topologyConfig.setComponentRam(BOLT_NAME, instanceRamDefault);
    topologyConfig.setComponentRam(SPOUT_NAME, instanceRamDefault);

    PackingPlan packingPlan =
        getFirstFitDecreasingPackingPlan(topology);

    Assert.assertEquals(packingPlan.containers.size(), 2);

    long startRam = (totalInstances + HERON_INTERNAL_CONTAINERS)
        * instanceRamDefault
        + (long) ((DEFAULT_CONTAINER_PADDING / 100.0
        * totalInstances * instanceRamDefault));

    Assert.assertEquals(packingPlan.resource.ram, startRam);

    double startCpu = Math.round(spoutParallelism * instanceCpuDefault
        + (DEFAULT_CONTAINER_PADDING / 100.0 * spoutParallelism * instanceCpuDefault))
        + Math.round(boltParallelism * instanceCpuDefault
        + (DEFAULT_CONTAINER_PADDING / 100.0 * boltParallelism * instanceCpuDefault))
        + instanceCpuDefault;

    Assert.assertEquals((long) packingPlan.resource.cpu, (long) startCpu);

    long startDisk = (totalInstances + HERON_INTERNAL_CONTAINERS)
        * instanceDiskDefault
        + (long) ((DEFAULT_CONTAINER_PADDING / 100.0
        * totalInstances * instanceDiskDefault));

    Assert.assertEquals(packingPlan.resource.disk, startDisk);

    int numScalingInstances = 3;
    Map<String, Integer> componentChanges = new HashMap<String, Integer>();
    componentChanges.put(BOLT_NAME, numScalingInstances);
    PackingPlan newPackingPlan = getFirstFitDecreasingPackingPlanRepack(topology, packingPlan,
        componentChanges);

    Assert.assertEquals(newPackingPlan.containers.size(), 3);

    long newRam = numScalingInstances * instanceRamDefault
        + (long) (DEFAULT_CONTAINER_PADDING / 100.0
        * numScalingInstances * instanceRamDefault);

    Assert.assertEquals(newPackingPlan.resource.ram, startRam + newRam);

    double newCpu = Math.round(numScalingInstances * instanceCpuDefault
        + (DEFAULT_CONTAINER_PADDING / 100.0 * numScalingInstances * instanceCpuDefault));

    Assert.assertEquals((long) newPackingPlan.resource.cpu, (long) (startCpu + newCpu));

    long newDisk = numScalingInstances * instanceDiskDefault
        + (long) ((DEFAULT_CONTAINER_PADDING / 100.0
        * numScalingInstances * instanceDiskDefault));

    Assert.assertEquals(newPackingPlan.resource.disk, startDisk + newDisk);
  }

  /**
   * Test the scenario ram map config is partially set and scaling is requested
   */
  @Test
  public void testPartialRamMapScaling() throws Exception {
    int spoutParallelism = 4;
    int boltParallelism = 3;

    // Set up the topology and its config
    com.twitter.heron.api.Config topologyConfig = new com.twitter.heron.api.Config();

    // Explicit set resources for container
    long maxContainerRam = 10L * Constants.GB;

    // Explicit set component ram map
    long boltRam = 4L * Constants.GB;

    topologyConfig.setContainerMaxRamHint(maxContainerRam);
    topologyConfig.setComponentRam(BOLT_NAME, boltRam);

    TopologyAPI.Topology topologyExplicitRamMap =
        getTopology(spoutParallelism, boltParallelism, topologyConfig);
    PackingPlan packingPlanExplicitRamMap =
        getFirstFitDecreasingPackingPlan(topologyExplicitRamMap);

    Assert.assertEquals(packingPlanExplicitRamMap.containers.size(), 2);

    long startRam = 2 * boltRam + 2 * instanceRamDefault
        + (long) (DEFAULT_CONTAINER_PADDING / 100.0 * (2 * boltRam + 2 * instanceRamDefault))
        + boltRam + 2 * instanceRamDefault
        + (long) (DEFAULT_CONTAINER_PADDING / 100.0 * (boltRam + 2 * instanceRamDefault))
        + instanceRamDefault;

    Assert.assertEquals(startRam, packingPlanExplicitRamMap.resource.ram);

    double startCpu = (long) (Math.round(4 * instanceCpuDefault
        + DEFAULT_CONTAINER_PADDING / 100.0 * (4 * instanceCpuDefault))
        + Math.round(3 * instanceCpuDefault
        + DEFAULT_CONTAINER_PADDING / 100.0 * (3 * instanceCpuDefault))
        + instanceCpuDefault);

    Assert.assertEquals((long) startCpu, (long) packingPlanExplicitRamMap.resource.cpu);

    long startDisk = 4 * instanceDiskDefault
        + (long) (DEFAULT_CONTAINER_PADDING / 100.0 * (4 * instanceDiskDefault))
        + 3 * instanceDiskDefault
        + (long) (DEFAULT_CONTAINER_PADDING / 100.0 * (3 * instanceDiskDefault))
        + instanceDiskDefault;

    Assert.assertEquals(startDisk, packingPlanExplicitRamMap.resource.disk);

    for (PackingPlan.ContainerPlan containerPlan
        : packingPlanExplicitRamMap.containers.values()) {
      for (PackingPlan.InstancePlan instancePlan : containerPlan.instances.values()) {
        // Ram for bolt should be the value in component ram map
        if (instancePlan.componentName.equals(BOLT_NAME)) {
          Assert.assertEquals(boltRam, instancePlan.resource.ram);
        }
        if (instancePlan.componentName.equals(SPOUT_NAME)) {
          Assert.assertEquals(instanceRamDefault, instancePlan.resource.ram);
        }
      }
    }
    int numScalingInstances = 3;
    Map<String, Integer> componentChanges = new HashMap<String, Integer>();
    componentChanges.put(BOLT_NAME, numScalingInstances);
    PackingPlan newPackingPlan = getFirstFitDecreasingPackingPlanRepack(topologyExplicitRamMap,
        packingPlanExplicitRamMap,
        componentChanges);

    Assert.assertEquals(newPackingPlan.containers.size(), 4);

    long newRam = 2 * boltRam
        + (long) (DEFAULT_CONTAINER_PADDING / 100.0 * 2 * boltRam)
        + 1 * boltRam
        + (long) (DEFAULT_CONTAINER_PADDING / 100.0 * boltRam);

    Assert.assertEquals(newPackingPlan.resource.ram, startRam + newRam);

    double newCpu = Math.round(2 * instanceCpuDefault
        + (DEFAULT_CONTAINER_PADDING / 100.0 * 2 * instanceCpuDefault))
        + Math.round(1 * instanceCpuDefault
        + (DEFAULT_CONTAINER_PADDING / 100.0 * 1 * instanceCpuDefault));

    Assert.assertEquals((long) newPackingPlan.resource.cpu, (long) (startCpu + newCpu));

    long newDisk = 2 * instanceDiskDefault
        + (long) ((DEFAULT_CONTAINER_PADDING / 100.0
        * 2 * instanceDiskDefault))
        + 1 * instanceDiskDefault
        + (long) (DEFAULT_CONTAINER_PADDING / 100.0
        * instanceDiskDefault);

    Assert.assertEquals(newPackingPlan.resource.disk, startDisk + newDisk);

    for (PackingPlan.ContainerPlan containerPlan
        : newPackingPlan.containers.values()) {
      for (PackingPlan.InstancePlan instancePlan : containerPlan.instances.values()) {
        // Ram for bolt should be the value in component ram map
        if (instancePlan.componentName.equals(BOLT_NAME)) {
          Assert.assertEquals(boltRam, instancePlan.resource.ram);
        }
        if (instancePlan.componentName.equals(SPOUT_NAME)) {
          Assert.assertEquals(instanceRamDefault, instancePlan.resource.ram);
        }
      }
    }
  }
}
