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
import org.junit.Before;
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

public class FirstFitDecreasingPackingTest {
  private static final String BOLT_NAME = "bolt";
  private static final String SPOUT_NAME = "spout";
  private static final int DEFAULT_CONTAINER_PADDING = 10;

  private int spoutParallelism;
  private int boltParallelism;
  private Integer totalInstances;
  private com.twitter.heron.api.Config topologyConfig;
  private TopologyAPI.Topology topology;
  private Resource instanceDefaultResources;

  private static TopologyAPI.Topology getTopology(
      int spoutParallelism, int boltParallelism,
      com.twitter.heron.api.Config topologyConfig) {
    return TopologyTests.createTopology("testTopology", topologyConfig, SPOUT_NAME, BOLT_NAME,
        spoutParallelism, boltParallelism);
  }

  private static PackingPlan getFirstFitDecreasingPackingPlan(TopologyAPI.Topology topology) {
    Config config = Config.newBuilder()
        .put(Keys.topologyId(), topology.getId())
        .put(Keys.topologyName(), topology.getName())
        .putAll(ClusterDefaults.getDefaults())
        .build();

    FirstFitDecreasingPacking packing = new FirstFitDecreasingPacking();

    packing.initialize(config, topology);
    return packing.pack();
  }

  private static PackingPlan getFirstFitDecreasingPackingPlanRepack(
      TopologyAPI.Topology topology,
      PackingPlan currentPackingPlan,
      Map<String, Integer> componentChanges) {
    Config config = Config.newBuilder()
        .put(Keys.topologyId(), topology.getId())
        .put(Keys.topologyName(), topology.getName())
        .putAll(ClusterDefaults.getDefaults())
        .build();

    FirstFitDecreasingPacking packing = new FirstFitDecreasingPacking();
    packing.initialize(config, topology);
    return packing.repack(currentPackingPlan, componentChanges);
  }

  @Before
  public void setUp() {
    this.spoutParallelism = 4;
    this.boltParallelism = 3;
    this.totalInstances = this.spoutParallelism + this.boltParallelism;

    // Set up the topology and its config
    this.topologyConfig = new com.twitter.heron.api.Config();
    this.topology = getTopology(spoutParallelism, boltParallelism, topologyConfig);

    Config config = Config.newBuilder()
        .put(Keys.topologyId(), topology.getId())
        .put(Keys.topologyName(), topology.getName())
        .putAll(ClusterDefaults.getDefaults())
        .build();
    this.instanceDefaultResources = new Resource(
        Context.instanceCpu(config), Context.instanceRam(config), Context.instanceDisk(config));
  }

  @Test(expected = RuntimeException.class)
  public void testCheckFailure() throws Exception {
    int numContainers = 2;

    // Explicit set insufficient ram for container
    long containerRam = -1L * Constants.GB;

    topologyConfig.put(com.twitter.heron.api.Config.TOPOLOGY_STMGRS, numContainers);
    topologyConfig.setContainerMaxRamHint(containerRam);

    getFirstFitDecreasingPackingPlan(
        getTopology(spoutParallelism, boltParallelism, topologyConfig));
  }

  /**
   * Test the scenario where the max container size is the default
   */
  @Test
  public void testDefaultContainerSize() throws Exception {
    PackingPlan packingPlan = getFirstFitDecreasingPackingPlan(topology);

    Assert.assertEquals(2, packingPlan.getContainers().size());
    Assert.assertEquals(totalInstances, packingPlan.getInstanceCount());
    AssertPacking.assertContainerRam(packingPlan.getContainers(),
        4 * instanceDefaultResources.getRam());
  }

  /**
   * Test the scenario where the max container size is the default but padding is configured
   */
  @Test
  public void testDefaultContainerSizeWithPadding() throws Exception {
    int padding = 50;
    int defaultNumInstancesperContainer = 4;

    topologyConfig.setContainerPaddingPercentage(padding);
    TopologyAPI.Topology newTopology =
        getTopology(spoutParallelism, boltParallelism, topologyConfig);
    PackingPlan packingPlan = getFirstFitDecreasingPackingPlan(newTopology);

    Assert.assertEquals(2, packingPlan.getContainers().size());
    Assert.assertEquals(totalInstances, packingPlan.getInstanceCount());
    AssertPacking.assertContainerRam(packingPlan.getContainers(),
        defaultNumInstancesperContainer * instanceDefaultResources.getRam());
  }

  /**
   * Test the scenario where container level resource config are set
   */
  @Test
  public void testContainerRequestedResources() throws Exception {
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

    Assert.assertEquals(1, packingPlanExplicitResourcesConfig.getContainers().size());
    Assert.assertEquals(totalInstances, packingPlanExplicitResourcesConfig.getInstanceCount());

    for (PackingPlan.ContainerPlan containerPlan
        : packingPlanExplicitResourcesConfig.getContainers()) {
      Assert.assertEquals(Math.round(totalInstances * instanceDefaultResources.getCpu()
              + (DEFAULT_CONTAINER_PADDING / 100.0 * totalInstances
              * instanceDefaultResources.getCpu())),
          (long) containerPlan.getResource().getCpu());

      Assert.assertEquals(totalInstances * instanceDefaultResources.getRam()
              + (long) (DEFAULT_CONTAINER_PADDING / 100.0 * totalInstances
              * instanceDefaultResources.getRam()),
          containerPlan.getResource().getRam());

      Assert.assertEquals(totalInstances * instanceDefaultResources.getDisk()
              + (long) (DEFAULT_CONTAINER_PADDING / 100.0 * totalInstances
              * instanceDefaultResources.getDisk()),
          containerPlan.getResource().getDisk());

      // All instances' resource requirement should be equal
      // So the size of set should be 1
      Set<Resource> resources = new HashSet<>();
      for (PackingPlan.InstancePlan instancePlan : containerPlan.getInstances()) {
        resources.add(instancePlan.getResource());
      }

      Assert.assertEquals(1, resources.size());
      Assert.assertEquals(instanceDefaultResources.getRam(), resources.iterator().next().getRam());
    }
  }

  /**
   * Test the scenario ram map config is fully set
   */
  @Test
  public void testCompleteRamMapRequested() throws Exception {
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

    Assert.assertEquals(1, packingPlanExplicitRamMap.getContainers().size());
    Assert.assertEquals(totalInstances, packingPlanExplicitRamMap.getInstanceCount());
    AssertPacking.assertContainers(packingPlanExplicitRamMap.getContainers(),
        BOLT_NAME, SPOUT_NAME, boltRam, spoutRam, maxContainerRam);
    AssertPacking.assertContainerRam(packingPlanExplicitRamMap.getContainers(),
        maxContainerRam);
  }

  /**
   * Test the scenario ram map config is fully set
   */
  @Test
  public void testCompleteRamMapRequested2() throws Exception {
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

    Assert.assertEquals(2, packingPlanExplicitRamMap.getContainers().size());
    Assert.assertEquals(totalInstances, packingPlanExplicitRamMap.getInstanceCount());

    AssertPacking.assertContainers(packingPlanExplicitRamMap.getContainers(),
        BOLT_NAME, SPOUT_NAME, boltRam, spoutRam, maxContainerRam);
    AssertPacking.assertContainerRam(packingPlanExplicitRamMap.getContainers(),
        maxContainerRam);
  }

  /**
   * Test the scenario ram map config is partially set
   */
  @Test
  public void testPartialRamMap() throws Exception {
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

    Assert.assertEquals(2, packingPlanExplicitRamMap.getContainers().size());
    Assert.assertEquals(totalInstances, packingPlanExplicitRamMap.getInstanceCount());

    AssertPacking.assertContainers(packingPlanExplicitRamMap.getContainers(),
        BOLT_NAME, SPOUT_NAME, boltRam, instanceDefaultResources.getRam(), maxContainerRam);
    AssertPacking.assertContainerRam(packingPlanExplicitRamMap.getContainers(),
        maxContainerRam);
  }

  /**
   * Test the scenario ram map config is partially set and padding is configured
   */
  @Test
  public void testPartialRamMapWithPadding() throws Exception {
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

    Assert.assertEquals(2, packingPlanExplicitRamMap.getContainers().size());
    Assert.assertEquals(totalInstances, packingPlanExplicitRamMap.getInstanceCount());

    AssertPacking.assertContainers(packingPlanExplicitRamMap.getContainers(),
        BOLT_NAME, SPOUT_NAME, boltRam, instanceDefaultResources.getRam(), null);
    AssertPacking.assertContainerRam(packingPlanExplicitRamMap.getContainers(),
        maxContainerRam);
  }

  /**
   * Test invalid ram for instance
   */
  @Test(expected = RuntimeException.class)
  public void testInvalidRamInstance() throws Exception {
    long maxContainerRam = 10L * Constants.GB;
    int defaultNumInstancesperContainer = 4;

    // Explicit set component ram map
    long boltRam = 0L * Constants.GB;

    topologyConfig.setContainerMaxRamHint(maxContainerRam);
    topologyConfig.setComponentRam(BOLT_NAME, boltRam);

    TopologyAPI.Topology topologyExplicitRamMap =
        getTopology(spoutParallelism, boltParallelism, topologyConfig);
    PackingPlan packingPlanExplicitRamMap =
        getFirstFitDecreasingPackingPlan(topologyExplicitRamMap);
    Assert.assertEquals(totalInstances, packingPlanExplicitRamMap.getInstanceCount());
    AssertPacking.assertContainerRam(packingPlanExplicitRamMap.getContainers(),
        defaultNumInstancesperContainer * instanceDefaultResources.getRam());
  }

  /**
   * Test the scenario where the max container size is the default
   * and scaling is requested.
   */
  @Test
  public void testDefaultContainerSizeRepack() throws Exception {
    int defaultNumInstancesperContainer = 4;

    topologyConfig.setComponentRam(BOLT_NAME, instanceDefaultResources.getRam());
    topologyConfig.setComponentRam(SPOUT_NAME, instanceDefaultResources.getRam());

    PackingPlan packingPlan = getFirstFitDecreasingPackingPlan(topology);

    Assert.assertEquals(packingPlan.getContainers().size(), 2);
    Assert.assertEquals(totalInstances, packingPlan.getInstanceCount());

    int numScalingInstances = 3;
    Map<String, Integer> componentChanges = new HashMap<>();
    componentChanges.put(BOLT_NAME, numScalingInstances);
    PackingPlan newPackingPlan = getFirstFitDecreasingPackingPlanRepack(topology, packingPlan,
        componentChanges);

    Assert.assertEquals(3, newPackingPlan.getContainers().size());
    Assert.assertEquals((Integer) (totalInstances + numScalingInstances),
        newPackingPlan.getInstanceCount());
    AssertPacking.assertContainers(newPackingPlan.getContainers(),
        BOLT_NAME, SPOUT_NAME, instanceDefaultResources.getRam(),
        instanceDefaultResources.getRam(), null);
    AssertPacking.assertContainerRam(newPackingPlan.getContainers(),
        defaultNumInstancesperContainer * instanceDefaultResources.getRam());

  }

  /**
   * Test the scenario ram map config is partially set and scaling is requested
   */
  @Test
  public void testPartialRamMapScaling() throws Exception {
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

    Assert.assertEquals(2, packingPlanExplicitRamMap.getContainers().size());
    Assert.assertEquals(totalInstances, packingPlanExplicitRamMap.getInstanceCount());

    AssertPacking.assertContainers(packingPlanExplicitRamMap.getContainers(),
        BOLT_NAME, SPOUT_NAME, boltRam, instanceDefaultResources.getRam(), null);

    int numScalingInstances = 3;
    Map<String, Integer> componentChanges = new HashMap<>();
    componentChanges.put(BOLT_NAME, numScalingInstances);
    PackingPlan newPackingPlan = getFirstFitDecreasingPackingPlanRepack(topologyExplicitRamMap,
        packingPlanExplicitRamMap,
        componentChanges);

    Assert.assertEquals(4, newPackingPlan.getContainers().size());
    Assert.assertEquals((Integer) (totalInstances + numScalingInstances),
        newPackingPlan.getInstanceCount());
    AssertPacking.assertContainers(newPackingPlan.getContainers(),
        BOLT_NAME, SPOUT_NAME, boltRam, instanceDefaultResources.getRam(), null);
    AssertPacking.assertContainerRam(newPackingPlan.getContainers(), maxContainerRam);
  }
}
