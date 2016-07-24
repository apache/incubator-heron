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
import com.twitter.heron.spi.utils.TopologyTests;

public class FirstFitDecreasingPackingTest {
  private static final String BOLT_NAME = "bolt";
  private static final String SPOUT_NAME = "spout";
  private static final double DELTA = 0.1;

  private long instanceRamDefault;
  private double instanceCpuDefault;
  private long instanceDiskDefault;

  private int countComponent(String component, Map<String, PackingPlan.InstancePlan> instances) {
    int count = 0;
    for (PackingPlan.InstancePlan pair : instances.values()) {
      if (component.equals(FirstFitDecreasingPacking.getComponentName(pair.id))) {
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

    Config runtime = Config.newBuilder()
        .put(Keys.topologyDefinition(), topology)
        .build();

    this.instanceRamDefault = Context.instanceRam(config);
    this.instanceCpuDefault = Context.instanceCpu(config).doubleValue();
    this.instanceDiskDefault = Context.instanceDisk(config);

    FirstFitDecreasingPacking packing = new FirstFitDecreasingPacking();
    packing.initialize(config, runtime);
    PackingPlan output = packing.pack();

    return output;
  }

  /**
   * Test the scenario where the max container size is the default
   */
  @Test
  public void testDefaultContainerSize() throws Exception {
    int spoutParallelism = 4;
    int boltParallelism = 3;

    // Set up the topology and its config
    com.twitter.heron.api.Config topologyConfig = new com.twitter.heron.api.Config();

    TopologyAPI.Topology topology =
        getTopology(spoutParallelism, boltParallelism, topologyConfig);

    PackingPlan packingPlan =
        getFirstFitDecreasingPackingPlan(topology);

    Assert.assertEquals(packingPlan.containers.size(), 2);

    long totalRam = (spoutParallelism + boltParallelism + 1) * instanceRamDefault
        + (10 * (spoutParallelism + boltParallelism) * instanceRamDefault) / 100;

    Assert.assertEquals(packingPlan.resource.ram, totalRam);

    double totalCpu = Math.round(spoutParallelism * instanceCpuDefault
        + (10 * spoutParallelism * instanceCpuDefault) / 100)
        + Math.round(boltParallelism * instanceCpuDefault
        + (10 * boltParallelism * instanceCpuDefault) / 100
        + instanceCpuDefault);

    Assert.assertEquals((long) packingPlan.resource.cpu, (long) totalCpu);

    long totalDisk = (spoutParallelism + boltParallelism + 1) * instanceDiskDefault
        + (10 * (spoutParallelism + boltParallelism) * instanceDiskDefault) / 100;

    Assert.assertEquals(packingPlan.resource.disk, totalDisk);
  }

  /**
   * Test the scenario where the max container size is the default but padding is configured
   */
  @Test
  public void testDefaultContainerSizeWithPadding() throws Exception {
    int spoutParallelism = 4;
    int boltParallelism = 3;

    // Set up the topology and its config
    com.twitter.heron.api.Config topologyConfig = new com.twitter.heron.api.Config();

    topologyConfig.setContainerPaddingPercentage(50);
    TopologyAPI.Topology topology =
        getTopology(spoutParallelism, boltParallelism, topologyConfig);

    PackingPlan packingPlan =
        getFirstFitDecreasingPackingPlan(topology);

    Assert.assertEquals(packingPlan.containers.size(), 2);

    long totalRam = (spoutParallelism + boltParallelism + 1) * instanceRamDefault
        + (50 * (spoutParallelism + boltParallelism) * instanceRamDefault) / 100;

    Assert.assertEquals(packingPlan.resource.ram, totalRam);

    double totalCpu = Math.round(spoutParallelism * instanceCpuDefault
        + (50 * spoutParallelism * instanceCpuDefault) / 100)
        + Math.round(boltParallelism * instanceCpuDefault
        + (50 * boltParallelism * instanceCpuDefault) / 100
        + instanceCpuDefault);

    Assert.assertEquals((long) packingPlan.resource.cpu, (long) totalCpu);

    long totalDisk = (spoutParallelism + boltParallelism + 1) * instanceDiskDefault
        + (50 * (spoutParallelism + boltParallelism) * instanceDiskDefault) / 100;

    Assert.assertEquals(packingPlan.resource.disk, totalDisk);
  }


  /**
   * Test the scenario where container level resource config are set
   */
  @Test
  public void testContainerRequestedResources() throws Exception {
    int spoutParallelism = 4;
    int boltParallelism = 3;

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

    Assert.assertEquals(packingPlanExplicitResourcesConfig.containers.size(), 1);


    Assert.assertEquals(Math.round(7 * instanceCpuDefault
            + (10 * 7 * instanceCpuDefault) / 100 + instanceCpuDefault),
        (long) packingPlanExplicitResourcesConfig.resource.cpu);

    Assert.assertEquals(7 * instanceRamDefault
            + (10 * 7 * instanceRamDefault) / 100
            + instanceRamDefault,
        packingPlanExplicitResourcesConfig.resource.ram);

    Assert.assertEquals(7 * instanceDiskDefault
            + (10 * 7 * instanceDiskDefault) / 100
            + instanceDiskDefault,
        packingPlanExplicitResourcesConfig.resource.disk);

    for (PackingPlan.ContainerPlan containerPlan
        : packingPlanExplicitResourcesConfig.containers.values()) {
      Assert.assertEquals(Math.round(7 * instanceCpuDefault
              + (10 * 7 * instanceCpuDefault) / 100),
          (long) containerPlan.resource.cpu);

      Assert.assertEquals(7 * instanceRamDefault
              + (10 * 7 * instanceRamDefault) / 100,
          containerPlan.resource.ram);

      Assert.assertEquals(7 * instanceDiskDefault
              + (10 * 7 * instanceDiskDefault) / 100,
          containerPlan.resource.disk);

      // All instances' resource requirement should be equal
      // So the size of set should be 1
      Set<PackingPlan.Resource> resources = new HashSet<>();
      for (PackingPlan.InstancePlan instancePlan : containerPlan.instances.values()) {
        resources.add(instancePlan.resource);
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

    Assert.assertEquals(packingPlanExplicitRamMap.containers.size(), 1);

    Assert.assertEquals(Math.round(7 * instanceCpuDefault
            + 10 * (7 * instanceCpuDefault) / 100
            + instanceCpuDefault),
        (long) packingPlanExplicitRamMap.resource.cpu);

    Assert.assertEquals(4 * spoutRam + 3 * boltRam
            + 10 * (4 * spoutRam + 3 * boltRam) / 100
            + instanceRamDefault,
        packingPlanExplicitRamMap.resource.ram);

    Assert.assertEquals(7 * instanceDiskDefault
            + 10 * (7 * instanceDiskDefault) / 100
            + instanceDiskDefault,
        packingPlanExplicitRamMap.resource.disk);

    // Ram for bolt should be the value in component ram map
    for (PackingPlan.ContainerPlan containerPlan
        : packingPlanExplicitRamMap.containers.values()) {
      Assert.assertNotEquals(maxContainerRam, containerPlan.resource.ram);
      for (PackingPlan.InstancePlan instancePlan : containerPlan.instances.values()) {
        if (instancePlan.componentName.equals(BOLT_NAME)) {
          Assert.assertEquals(boltRam, instancePlan.resource.ram);
        }
        if (instancePlan.componentName.equals(SPOUT_NAME)) {
          Assert.assertEquals(spoutRam, instancePlan.resource.ram);
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

    Assert.assertEquals(packingPlanExplicitRamMap.containers.size(), 2);

    Assert.assertEquals((long) (Math.round(4 * instanceCpuDefault
            + 10 * (4 * instanceCpuDefault) / 100)
            + Math.round(3 * instanceCpuDefault
            + 10 * (3 * instanceCpuDefault) / 100)
            + instanceCpuDefault),
        (long) packingPlanExplicitRamMap.resource.cpu);

    Assert.assertEquals((4 * spoutRam)
            + 10 * (4 * spoutRam) / 100
            + 3 * boltRam
            + 10 * (3 * boltRam) / 100
            + instanceRamDefault,
        packingPlanExplicitRamMap.resource.ram);

    Assert.assertEquals(4 * instanceDiskDefault
            + 10 * (4 * instanceDiskDefault) / 100
            + 3 * instanceDiskDefault
            + 10 * (3 * instanceDiskDefault) / 100
            + instanceDiskDefault,
        packingPlanExplicitRamMap.resource.disk);

    // Ram for bolt/spout should be the value in component ram map
    for (PackingPlan.ContainerPlan containerPlan
        : packingPlanExplicitRamMap.containers.values()) {
      for (PackingPlan.InstancePlan instancePlan : containerPlan.instances.values()) {
        if (instancePlan.componentName.equals(BOLT_NAME)) {
          Assert.assertEquals(boltRam, instancePlan.resource.ram);
        }
        if (instancePlan.componentName.equals(SPOUT_NAME)) {
          Assert.assertEquals(spoutRam, instancePlan.resource.ram);
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

    Assert.assertEquals(packingPlanExplicitRamMap.containers.size(), 2);

    Assert.assertEquals((long) (Math.round(4 * instanceCpuDefault
            + 10 * (4 * instanceCpuDefault) / 100)
            + Math.round(3 * instanceCpuDefault
            + 10 * (3 * instanceCpuDefault) / 100)
            + instanceCpuDefault),
        (long) packingPlanExplicitRamMap.resource.cpu);

    Assert.assertEquals(2 * boltRam + 2 * instanceRamDefault
            + 10 * (2 * boltRam + 2 * instanceRamDefault) / 100
            + boltRam + 2 * instanceRamDefault
            + 10 * (boltRam + 2 * instanceRamDefault) / 100
            + instanceRamDefault,
        packingPlanExplicitRamMap.resource.ram);

    Assert.assertEquals(4 * instanceDiskDefault
            + 10 * (4 * instanceDiskDefault) / 100
            + 3 * instanceDiskDefault
            + 10 * (3 * instanceDiskDefault) / 100
            + instanceDiskDefault,
        packingPlanExplicitRamMap.resource.disk);

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
  }

  /**
   * Test the scenario ram map config is partially set and padding is configured
   */
  @Test
  public void testPartialRamMapWithPadding() throws Exception {
    int spoutParallelism = 4;
    int boltParallelism = 3;

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

    Assert.assertEquals(packingPlanExplicitRamMap.containers.size(), 2);

    Assert.assertEquals((long) (Math.round(4 * instanceCpuDefault
            + 0 * (4 * instanceCpuDefault) / 100)
            + Math.round(3 * instanceCpuDefault
            + 0 * (3 * instanceCpuDefault) / 100)
            + instanceCpuDefault),
        (long) packingPlanExplicitRamMap.resource.cpu);

    Assert.assertEquals(2 * boltRam + 2 * instanceRamDefault
            + 0 * (2 * boltRam + 2 * instanceRamDefault) / 100
            + boltRam + 2 * instanceRamDefault
            + 0 * (boltRam + 2 * instanceRamDefault) / 100
            + instanceRamDefault,
        packingPlanExplicitRamMap.resource.ram);

    Assert.assertEquals(4 * instanceDiskDefault
            + 0 * (4 * instanceDiskDefault) / 100
            + 3 * instanceDiskDefault
            + 0 * (3 * instanceDiskDefault) / 100
            + instanceDiskDefault,
        packingPlanExplicitRamMap.resource.disk);

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
  }

  /**
   * Test invalid ram for instance
   */
  @Test
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

    Assert.assertEquals(packingPlanExplicitRamMap, null);

  }

}
