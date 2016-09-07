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

import java.util.HashSet;
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

  private long instanceRamDefault;
  private double instanceCpuDefault;
  private long instanceDiskDefault;

  private TopologyAPI.Topology getTopology(
      int spoutParallelism, int boltParallelism,
      com.twitter.heron.api.Config topologyConfig) {
    return TopologyTests.createTopology("testTopology", topologyConfig, SPOUT_NAME, BOLT_NAME,
        spoutParallelism, boltParallelism);
  }


  protected PackingPlan getFirstFitDecreasingPackingPlan(TopologyAPI.Topology topology) {
    Config config = Config.newBuilder()
        .put(Keys.topologyId(), topology.getId())
        .put(Keys.topologyName(), topology.getName())
        .putAll(ClusterDefaults.getDefaults())
        .build();

    this.instanceRamDefault = Context.instanceRam(config);
    this.instanceCpuDefault = Context.instanceCpu(config);
    this.instanceDiskDefault = Context.instanceDisk(config);

    FirstFitDecreasingPacking packing = new FirstFitDecreasingPacking();

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

    topologyConfig.setContainerMaxRamHint(containerRam);

    TopologyAPI.Topology topology = getTopology(spoutParallelism, boltParallelism, topologyConfig);

    getFirstFitDecreasingPackingPlan(topology);
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

    TopologyAPI.Topology topology = getTopology(spoutParallelism, boltParallelism, topologyConfig);
    PackingPlan packingPlan = getFirstFitDecreasingPackingPlan(topology);

    Assert.assertEquals(packingPlan.getContainers().size(), 2);
  }

  /**
   * Test the scenario where the max container size is the default but padding is configured
   */
  @Test
  public void testDefaultContainerSizeWithPadding() throws Exception {
    int spoutParallelism = 4;
    int boltParallelism = 3;
    int padding = 50;
    // Set up the topology and its config
    com.twitter.heron.api.Config topologyConfig = new com.twitter.heron.api.Config();

    topologyConfig.setContainerPaddingPercentage(padding);
    TopologyAPI.Topology topology = getTopology(spoutParallelism, boltParallelism, topologyConfig);
    PackingPlan packingPlan = getFirstFitDecreasingPackingPlan(topology);

    Assert.assertEquals(packingPlan.getContainers().size(), 2);
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

    for (PackingPlan.ContainerPlan containerPlan
        : packingPlanExplicitResourcesConfig.getContainers()) {
      Assert.assertEquals(Math.round(totalInstances * instanceCpuDefault
              + (DEFAULT_CONTAINER_PADDING / 100.0 * totalInstances * instanceCpuDefault)),
          (long) containerPlan.getResource().getCpu());

      Assert.assertEquals(totalInstances * instanceRamDefault
              + (long) (DEFAULT_CONTAINER_PADDING / 100.0 * totalInstances * instanceRamDefault),
          containerPlan.getResource().getRam());

      Assert.assertEquals(totalInstances * instanceDiskDefault
              + (long) (DEFAULT_CONTAINER_PADDING / 100.0 * totalInstances * instanceDiskDefault),
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

    Assert.assertEquals(packingPlanExplicitRamMap.getContainers().size(), 1);

    // Ram for bolt should be the value in component ram map
    for (PackingPlan.ContainerPlan containerPlan
        : packingPlanExplicitRamMap.getContainers()) {
      Assert.assertNotEquals(maxContainerRam, containerPlan.getResource().getRam());
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

    // Ram for bolt/spout should be the value in component ram map
    for (PackingPlan.ContainerPlan containerPlan
        : packingPlanExplicitRamMap.getContainers()) {
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

    for (PackingPlan.ContainerPlan containerPlan
        : packingPlanExplicitRamMap.getContainers()) {
      for (PackingPlan.InstancePlan instancePlan : containerPlan.getInstances()) {
        // Ram for bolt should be the value in component ram map
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

    Assert.assertEquals(packingPlanExplicitRamMap.getContainers().size(), 2);

    for (PackingPlan.ContainerPlan containerPlan
        : packingPlanExplicitRamMap.getContainers()) {
      for (PackingPlan.InstancePlan instancePlan : containerPlan.getInstances()) {
        // Ram for bolt should be the value in component ram map
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
}
