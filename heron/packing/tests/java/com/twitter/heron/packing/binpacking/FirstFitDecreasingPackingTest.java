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
import com.twitter.heron.packing.PackingUtils;
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

  /**
   * Performs a scaling test for a specific topology. It first
   * computes an initial packing plan as a basis for scaling.
   * Given specific component parallelism changes, a new packing plan is produced.
   *
   * @param topology Input topology
   * @param componentChanges parallelism changes for scale up/down
   * @param boltRam ram allocated to bolts
   * @param boltParallelism bolt parallelism
   * @param spoutRam ram allocated to spouts
   * @param spoutParallelism spout parallelism
   * @param numContainersBeforeRepack number of containers that the initial packing plan should use
   * @param totalInstancesExpected number of instances expected before scaling
   * @return the new packing plan
   */
  private static PackingPlan doScalingTest(TopologyAPI.Topology topology,
                                           Map<String, Integer> componentChanges,
                                           long boltRam,
                                           int boltParallelism, long spoutRam,
                                           int spoutParallelism,
                                           int numContainersBeforeRepack,
                                           int totalInstancesExpected) {
    PackingPlan packingPlan = getFirstFitDecreasingPackingPlan(topology);

    Assert.assertEquals(numContainersBeforeRepack, packingPlan.getContainers().size());
    Assert.assertEquals(totalInstancesExpected, (int) packingPlan.getInstanceCount());
    AssertPacking.assertContainers(packingPlan.getContainers(),
        BOLT_NAME, SPOUT_NAME, boltRam, spoutRam, null);
    AssertPacking.assertNumInstances(packingPlan.getContainers(), BOLT_NAME, boltParallelism);
    AssertPacking.assertNumInstances(packingPlan.getContainers(), SPOUT_NAME, spoutParallelism);

    PackingPlan newPackingPlan =
        getFirstFitDecreasingPackingPlanRepack(topology, packingPlan, componentChanges);
    AssertPacking.assertContainerRam(newPackingPlan.getContainers(),
        packingPlan.getMaxContainerResources().getRam());

    return newPackingPlan;
  }

  private PackingPlan doDefaultScalingTest(Map<String, Integer> componentChanges,
                                           int numContainersBeforeRepack) {
    return doScalingTest(topology, componentChanges,
        instanceDefaultResources.getRam(), boltParallelism,
        instanceDefaultResources.getRam(), spoutParallelism,
        numContainersBeforeRepack, totalInstances);
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
    int defaultNumInstancesperContainer = 4;
    PackingPlan packingPlan = getFirstFitDecreasingPackingPlan(topology);

    Assert.assertEquals(2, packingPlan.getContainers().size());
    Assert.assertEquals(totalInstances, packingPlan.getInstanceCount());
    long defaultRam = PackingUtils.increaseBy(
        defaultNumInstancesperContainer * instanceDefaultResources.getRam(),
        DEFAULT_CONTAINER_PADDING);

    AssertPacking.assertContainerRam(packingPlan.getContainers(), defaultRam);
    AssertPacking.assertNumInstances(packingPlan.getContainers(), BOLT_NAME, 3);
    AssertPacking.assertNumInstances(packingPlan.getContainers(), SPOUT_NAME, 4);
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
    long defaultRam = PackingUtils.increaseBy(
        defaultNumInstancesperContainer * instanceDefaultResources.getRam(), padding);
    AssertPacking.assertContainerRam(packingPlan.getContainers(),
        defaultRam);
    AssertPacking.assertNumInstances(packingPlan.getContainers(), BOLT_NAME, 3);
    AssertPacking.assertNumInstances(packingPlan.getContainers(), SPOUT_NAME, 4);
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
    AssertPacking.assertNumInstances(packingPlanExplicitResourcesConfig.getContainers(),
        BOLT_NAME, 3);
    AssertPacking.assertNumInstances(packingPlanExplicitResourcesConfig.getContainers(),
        SPOUT_NAME, 4);

    for (PackingPlan.ContainerPlan containerPlan
        : packingPlanExplicitResourcesConfig.getContainers()) {
      Assert.assertEquals(Math.round(PackingUtils.increaseBy(
          totalInstances * instanceDefaultResources.getCpu(), DEFAULT_CONTAINER_PADDING)),
          (long) containerPlan.getRequiredResource().getCpu());

      Assert.assertEquals(PackingUtils.increaseBy(
          totalInstances * instanceDefaultResources.getRam(),
          DEFAULT_CONTAINER_PADDING),
          containerPlan.getRequiredResource().getRam());

      Assert.assertEquals(PackingUtils.increaseBy(
          totalInstances * instanceDefaultResources.getDisk(), DEFAULT_CONTAINER_PADDING),
          containerPlan.getRequiredResource().getDisk());

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
    AssertPacking.assertNumInstances(packingPlanExplicitRamMap.getContainers(), BOLT_NAME, 3);
    AssertPacking.assertNumInstances(packingPlanExplicitRamMap.getContainers(), SPOUT_NAME, 4);
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
    AssertPacking.assertNumInstances(packingPlanExplicitRamMap.getContainers(), BOLT_NAME, 3);
    AssertPacking.assertNumInstances(packingPlanExplicitRamMap.getContainers(), SPOUT_NAME, 4);

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
    AssertPacking.assertNumInstances(packingPlanExplicitRamMap.getContainers(), BOLT_NAME, 3);
    AssertPacking.assertNumInstances(packingPlanExplicitRamMap.getContainers(), SPOUT_NAME, 4);

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
    AssertPacking.assertNumInstances(packingPlanExplicitRamMap.getContainers(), BOLT_NAME, 3);
    AssertPacking.assertNumInstances(packingPlanExplicitRamMap.getContainers(), SPOUT_NAME, 4);

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
    AssertPacking.assertNumInstances(packingPlanExplicitRamMap.getContainers(), BOLT_NAME, 3);
    AssertPacking.assertNumInstances(packingPlanExplicitRamMap.getContainers(), SPOUT_NAME, 4);
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
    int numScalingInstances = 5;
    Map<String, Integer> componentChanges = new HashMap<>();
    componentChanges.put(BOLT_NAME, numScalingInstances);
    int numContainersBeforeRepack = 2;
    PackingPlan newPackingPlan = doDefaultScalingTest(componentChanges, numContainersBeforeRepack);
    Assert.assertEquals(3, newPackingPlan.getContainers().size());
    Assert.assertEquals((Integer) (totalInstances + numScalingInstances),
        newPackingPlan.getInstanceCount());
    AssertPacking.assertContainers(newPackingPlan.getContainers(),
        BOLT_NAME, SPOUT_NAME, instanceDefaultResources.getRam(),
        instanceDefaultResources.getRam(), null);
    for (PackingPlan.ContainerPlan containerPlan
        : newPackingPlan.getContainers()) {
      Assert.assertEquals(Math.round(PackingUtils.increaseBy(
          defaultNumInstancesperContainer * instanceDefaultResources.getCpu(),
          DEFAULT_CONTAINER_PADDING)), (long) containerPlan.getRequiredResource().getCpu());

      Assert.assertEquals(PackingUtils.increaseBy(
          defaultNumInstancesperContainer * instanceDefaultResources.getRam(),
          DEFAULT_CONTAINER_PADDING), containerPlan.getRequiredResource().getRam());

      Assert.assertEquals(PackingUtils.increaseBy(
          defaultNumInstancesperContainer * instanceDefaultResources.getDisk(),
          DEFAULT_CONTAINER_PADDING), containerPlan.getRequiredResource().getDisk());
    }
  }

  /**
   * Test the scenario ram map config is partially set and scaling is requested
   */
  @Test
  public void testRepackPadding() throws Exception {
    int paddingPercentage = 50;
    topologyConfig.setContainerPaddingPercentage(paddingPercentage);
    // Explicit set component ram map
    long boltRam = 4L * Constants.GB;
    long maxContainerRam = 10L * Constants.GB;
    topologyConfig.setComponentRam(BOLT_NAME, boltRam);
    topologyConfig.setContainerMaxRamHint(maxContainerRam);

    TopologyAPI.Topology topologyExplicitRamMap =
        getTopology(spoutParallelism, boltParallelism, topologyConfig);

    int numScalingInstances = 3;
    Map<String, Integer> componentChanges = new HashMap<>();
    componentChanges.put(BOLT_NAME, numScalingInstances);

    int numContainersBeforeRepack = 3;
    PackingPlan newPackingPlan =
        doScalingTest(topologyExplicitRamMap, componentChanges, boltRam,
            boltParallelism, instanceDefaultResources.getRam(), spoutParallelism,
            numContainersBeforeRepack, totalInstances);

    Assert.assertEquals(6, newPackingPlan.getContainers().size());
    Assert.assertEquals((Integer) (totalInstances + numScalingInstances),
        newPackingPlan.getInstanceCount());
    AssertPacking.assertContainers(newPackingPlan.getContainers(),
        BOLT_NAME, SPOUT_NAME, boltRam, instanceDefaultResources.getRam(), null);

    for (PackingPlan.ContainerPlan containerPlan : newPackingPlan.getContainers()) {
      //Each container either contains a single bolt or 1 bolt and 2 spouts
      if (containerPlan.getInstances().size() == 1) {
        Assert.assertEquals(PackingUtils.increaseBy(boltRam, paddingPercentage),
            containerPlan.getRequiredResource().getRam());
      }
      if (containerPlan.getInstances().size() == 3) {
        long resourceRam = boltRam + 2 * instanceDefaultResources.getRam();
        Assert.assertEquals(PackingUtils.increaseBy(resourceRam, paddingPercentage),
            containerPlan.getRequiredResource().getRam());
      }
    }
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

    int numScalingInstances = 3;
    Map<String, Integer> componentChanges = new HashMap<>();
    componentChanges.put(BOLT_NAME, numScalingInstances);

    int numContainersBeforeRepack = 2;
    PackingPlan newPackingPlan = doScalingTest(topologyExplicitRamMap, componentChanges, boltRam,
        boltParallelism, instanceDefaultResources.getRam(), spoutParallelism,
        numContainersBeforeRepack, totalInstances);

    Assert.assertEquals(4, newPackingPlan.getContainers().size());
    Assert.assertEquals((Integer) (totalInstances + numScalingInstances),
        newPackingPlan.getInstanceCount());
    AssertPacking.assertContainers(newPackingPlan.getContainers(),
        BOLT_NAME, SPOUT_NAME, boltRam, instanceDefaultResources.getRam(), null);
  }

  @Test(expected = RuntimeException.class)
  public void testScaleDownInvalidComponent() throws Exception {
    Map<String, Integer> componentChanges = new HashMap<>();
    componentChanges.put("SPOUT_FAKE", -10); //try to remove a component that does not exist
    int numContainersBeforeRepack = 2;
    doDefaultScalingTest(componentChanges, numContainersBeforeRepack);
  }

  @Test(expected = RuntimeException.class)
  public void testScaleDownInvalidScaleFactor() throws Exception {

    //try to remove more spout instances than possible
    int spoutScalingDown = -5;
    Map<String, Integer> componentChanges = new HashMap<>();
    componentChanges.put(SPOUT_NAME, spoutScalingDown);

    int numContainersBeforeRepack = 2;
    doDefaultScalingTest(componentChanges, numContainersBeforeRepack);
  }


  /**
   * Test the scenario where the scaling down is requested
   */
  @Test
  public void testScaleDown() throws Exception {
    int spoutScalingDown = -3;
    int boltScalingDown = -2;

    Map<String, Integer> componentChanges = new HashMap<>();
    componentChanges.put(SPOUT_NAME, spoutScalingDown); //leave 1 spout
    componentChanges.put(BOLT_NAME, boltScalingDown); //leave 1 bolt
    int numContainersBeforeRepack = 2;
    PackingPlan newPackingPlan = doDefaultScalingTest(componentChanges, numContainersBeforeRepack);

    Assert.assertEquals(2, newPackingPlan.getContainers().size());
    Assert.assertEquals((Integer) (totalInstances + spoutScalingDown + boltScalingDown),
        newPackingPlan.getInstanceCount());
    AssertPacking.assertNumInstances(newPackingPlan.getContainers(),
        BOLT_NAME, 1);
    AssertPacking.assertNumInstances(newPackingPlan.getContainers(),
        SPOUT_NAME, 1);
  }

  /**
   * Test the scenario where scaling down is requested and the first container is removed.
   */
  @Test
  public void removeFirstContainer() throws Exception {
    /* The packing plan consists of two containers. The first one contains 4 spouts and
       the second one contains 3 bolts. During scaling we remove 4 spouts and thus the f
       first container is removed.
     */
    int spoutScalingDown = -4;
    Map<String, Integer> componentChanges = new HashMap<>();
    componentChanges.put(SPOUT_NAME, spoutScalingDown);
    int numContainersBeforeRepack = 2;
    PackingPlan newPackingPlan = doDefaultScalingTest(componentChanges, numContainersBeforeRepack);

    Assert.assertEquals(1, newPackingPlan.getContainers().size());
    Assert.assertEquals((Integer) (totalInstances + spoutScalingDown),
        newPackingPlan.getInstanceCount());
    AssertPacking.assertNumInstances(newPackingPlan.getContainers(), BOLT_NAME, 3);
    AssertPacking.assertNumInstances(newPackingPlan.getContainers(), SPOUT_NAME, 0);
  }

  /**
   * Test the scenario where scaling down and up is simultaneously requested
   */
  @Test
  public void scaleDownAndUp() throws Exception {
    int spoutScalingDown = -4;
    int boltScalingUp = 6;

    Map<String, Integer> componentChanges = new HashMap<>();
    componentChanges.put(SPOUT_NAME, spoutScalingDown); // 0 spouts
    componentChanges.put(BOLT_NAME, boltScalingUp); // 9 bolts
    int numContainersBeforeRepack = 2;
    PackingPlan newPackingPlan = doDefaultScalingTest(componentChanges, numContainersBeforeRepack);

    Assert.assertEquals(3, newPackingPlan.getContainers().size());
    Assert.assertEquals((Integer) (totalInstances + spoutScalingDown + boltScalingUp),
        newPackingPlan.getInstanceCount());
    AssertPacking.assertNumInstances(newPackingPlan.getContainers(),
        BOLT_NAME, 9);
    AssertPacking.assertNumInstances(newPackingPlan.getContainers(),
        SPOUT_NAME, 0);
  }

  /**
   * Test the scenario where scaling down and up is simultaneously requested and padding is
   * configured
   */
  @Test
  public void scaleDownAndUpWithExtraPadding() throws Exception {
    int paddingPercentage = 50;
    topologyConfig.setContainerPaddingPercentage(paddingPercentage);
    // Explicit set resources for container
    long maxContainerRam = 12L * Constants.GB;
    // Explicit set component ram map
    long spoutRam = 2L * Constants.GB;
    topologyConfig.setContainerMaxRamHint(maxContainerRam);
    topologyConfig.setComponentRam(SPOUT_NAME, spoutRam);

    int noBolts = 2;
    int noSpouts = 1;

    TopologyAPI.Topology topologyExplicitRamMap =
        getTopology(noSpouts, noBolts, topologyConfig);

    int spoutScalingUp = 1;
    int boltScalingDown = -2;

    Map<String, Integer> componentChanges = new HashMap<>();
    componentChanges.put(SPOUT_NAME, spoutScalingUp); // 2 spouts
    componentChanges.put(BOLT_NAME, boltScalingDown); // 0 bolts
    int numContainersBeforeRepack = 1;
    PackingPlan newPackingPlan = doScalingTest(topologyExplicitRamMap, componentChanges,
        instanceDefaultResources.getRam(), noBolts, spoutRam, noSpouts,
        numContainersBeforeRepack, noSpouts + noBolts);

    Assert.assertEquals(1, newPackingPlan.getContainers().size());
    Assert.assertEquals((Integer) (noSpouts + noBolts + spoutScalingUp + boltScalingDown),
        newPackingPlan.getInstanceCount());
    AssertPacking.assertNumInstances(newPackingPlan.getContainers(),
        BOLT_NAME, noBolts + boltScalingDown);
    AssertPacking.assertNumInstances(newPackingPlan.getContainers(),
        SPOUT_NAME, noSpouts + spoutScalingUp);
  }

  /**
   * Test the scenario where scaling down and up is simultaneously requested and padding is
   * configured
   */
  @Test
  public void scaleDownAndUpNoPadding() throws Exception {
    int paddingPercentage = 0;
    topologyConfig.setContainerPaddingPercentage(paddingPercentage);
    // Explicit set resources for container
    long maxContainerRam = 12L * Constants.GB;
    // Explicit set component ram map
    long spoutRam = 4L * Constants.GB;
    topologyConfig.setContainerMaxRamHint(maxContainerRam);
    topologyConfig.setComponentRam(SPOUT_NAME, spoutRam);

    int noBolts = 3;
    int noSpouts = 1;

    TopologyAPI.Topology topologyExplicitRamMap =
        getTopology(noSpouts, noBolts, topologyConfig);

    int spoutScalingUp = 1;
    int boltScalingDown = -1;

    Map<String, Integer> componentChanges = new HashMap<>();
    componentChanges.put(SPOUT_NAME, spoutScalingUp); // 2 spouts
    componentChanges.put(BOLT_NAME, boltScalingDown); // 2 bolts
    int numContainersBeforeRepack = 1;
    PackingPlan newPackingPlan = doScalingTest(topologyExplicitRamMap, componentChanges,
        instanceDefaultResources.getRam(), noBolts, spoutRam, noSpouts,
        numContainersBeforeRepack, noSpouts + noBolts);

    Assert.assertEquals(2, newPackingPlan.getContainers().size());
    Assert.assertEquals((Integer) (noSpouts + noBolts + spoutScalingUp + boltScalingDown),
        newPackingPlan.getInstanceCount());
    AssertPacking.assertNumInstances(newPackingPlan.getContainers(),
        BOLT_NAME, noBolts + boltScalingDown);
    AssertPacking.assertNumInstances(newPackingPlan.getContainers(),
        SPOUT_NAME, noSpouts + spoutScalingUp);
  }
}
