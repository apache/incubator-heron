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
import com.twitter.heron.common.basics.ByteAmount;
import com.twitter.heron.packing.AssertPacking;
import com.twitter.heron.packing.CommonPackingTests;
import com.twitter.heron.packing.utils.PackingUtils;
import com.twitter.heron.spi.packing.IPacking;
import com.twitter.heron.spi.packing.IRepacking;
import com.twitter.heron.spi.packing.PackingException;
import com.twitter.heron.spi.packing.PackingPlan;
import com.twitter.heron.spi.packing.Resource;

public class FirstFitDecreasingPackingTest extends CommonPackingTests {

  @Override
  protected IPacking getPackingImpl() {
    return new FirstFitDecreasingPacking();
  }

  @Override
  protected IRepacking getRepackingImpl() {
    return new FirstFitDecreasingPacking();
  }

  @Test (expected = PackingException.class)
  public void testFailureInsufficientContainerRamHint() throws Exception {
    topologyConfig.setContainerMaxRamHint(ByteAmount.ZERO);
    pack(getTopology(spoutParallelism, boltParallelism, topologyConfig));
  }

  /**
   * Test the scenario where the max container size is the default
   */
  @Test
  public void testDefaultContainerSize() throws Exception {
    int defaultNumInstancesperContainer = 4;
    PackingPlan packingPlan = pack(topology);

    Assert.assertEquals(2, packingPlan.getContainers().size());
    Assert.assertEquals(totalInstances, packingPlan.getInstanceCount());
    ByteAmount defaultRam = instanceDefaultResources.getRam()
        .multiply(defaultNumInstancesperContainer).increaseBy(DEFAULT_CONTAINER_PADDING);

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
    PackingPlan packingPlan = pack(newTopology);

    Assert.assertEquals(2, packingPlan.getContainers().size());
    Assert.assertEquals(totalInstances, packingPlan.getInstanceCount());
    ByteAmount defaultRam = instanceDefaultResources.getRam()
        .multiply(defaultNumInstancesperContainer).increaseBy(padding);
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
    ByteAmount containerRam = ByteAmount.fromGigabytes(10);
    ByteAmount containerDisk = ByteAmount.fromGigabytes(20);
    float containerCpu = 30;

    topologyConfig.setContainerMaxRamHint(containerRam);
    topologyConfig.setContainerMaxDiskHint(containerDisk);
    topologyConfig.setContainerMaxCpuHint(containerCpu);

    TopologyAPI.Topology topologyExplicitResourcesConfig =
        getTopology(spoutParallelism, boltParallelism, topologyConfig);
    PackingPlan packingPlanExplicitResourcesConfig = pack(topologyExplicitResourcesConfig);

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

      Assert.assertEquals(instanceDefaultResources.getRam()
              .multiply(totalInstances)
              .increaseBy(DEFAULT_CONTAINER_PADDING),
          containerPlan.getRequiredResource().getRam());

      Assert.assertEquals(instanceDefaultResources.getDisk()
              .multiply(totalInstances)
              .increaseBy(DEFAULT_CONTAINER_PADDING),
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
    ByteAmount maxContainerRam = ByteAmount.fromGigabytes(15);
    ByteAmount maxContainerDisk = ByteAmount.fromGigabytes(20);
    float maxContainerCpu = 30;

    // Explicit set component ram map
    ByteAmount boltRam = ByteAmount.fromGigabytes(1);
    ByteAmount spoutRam = ByteAmount.fromGigabytes(2);

    topologyConfig.setContainerMaxRamHint(maxContainerRam);
    topologyConfig.setContainerMaxDiskHint(maxContainerDisk);
    topologyConfig.setContainerMaxCpuHint(maxContainerCpu);
    topologyConfig.setComponentRam(BOLT_NAME, boltRam);
    topologyConfig.setComponentRam(SPOUT_NAME, spoutRam);

    TopologyAPI.Topology topologyExplicitRamMap =
        getTopology(spoutParallelism, boltParallelism, topologyConfig);
    PackingPlan packingPlanExplicitRamMap = pack(topologyExplicitRamMap);

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
    ByteAmount maxContainerRam = ByteAmount.fromGigabytes(10);

    // Explicit set component ram map
    ByteAmount boltRam = ByteAmount.fromGigabytes(1);
    ByteAmount spoutRam = ByteAmount.fromGigabytes(2);

    topologyConfig.setContainerMaxRamHint(maxContainerRam);
    topologyConfig.setComponentRam(BOLT_NAME, boltRam);
    topologyConfig.setComponentRam(SPOUT_NAME, spoutRam);

    TopologyAPI.Topology topologyExplicitRamMap =
        getTopology(spoutParallelism, boltParallelism, topologyConfig);
    PackingPlan packingPlanExplicitRamMap = pack(topologyExplicitRamMap);

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
    ByteAmount maxContainerRam = ByteAmount.fromGigabytes(10);

    // Explicit set component ram map
    ByteAmount boltRam = ByteAmount.fromGigabytes(4);

    topologyConfig.setContainerMaxRamHint(maxContainerRam);
    topologyConfig.setComponentRam(BOLT_NAME, boltRam);

    TopologyAPI.Topology topologyExplicitRamMap =
        getTopology(spoutParallelism, boltParallelism, topologyConfig);
    PackingPlan packingPlanExplicitRamMap = pack(topologyExplicitRamMap);

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
    ByteAmount maxContainerRam = ByteAmount.fromGigabytes(10);

    // Explicit set component ram map
    ByteAmount boltRam = ByteAmount.fromGigabytes(4);

    topologyConfig.setContainerMaxRamHint(maxContainerRam);
    topologyConfig.setComponentRam(BOLT_NAME, boltRam);

    TopologyAPI.Topology topologyExplicitRamMap =
        getTopology(spoutParallelism, boltParallelism, topologyConfig);
    PackingPlan packingPlanExplicitRamMap = pack(topologyExplicitRamMap);

    Assert.assertEquals(2, packingPlanExplicitRamMap.getContainers().size());
    Assert.assertEquals(totalInstances, packingPlanExplicitRamMap.getInstanceCount());
    AssertPacking.assertNumInstances(packingPlanExplicitRamMap.getContainers(), BOLT_NAME, 3);
    AssertPacking.assertNumInstances(packingPlanExplicitRamMap.getContainers(), SPOUT_NAME, 4);

    AssertPacking.assertContainers(packingPlanExplicitRamMap.getContainers(),
        BOLT_NAME, SPOUT_NAME, boltRam, instanceDefaultResources.getRam(), null);
    AssertPacking.assertContainerRam(packingPlanExplicitRamMap.getContainers(),
        maxContainerRam);
  }

  @Test
  public void testContainersRequestedExceedsInstanceCount() throws Exception {
    doTestContainerCountRequested(8, 2); // instances will fit into 2 containers
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

      Assert.assertEquals(instanceDefaultResources.getRam()
          .multiply(defaultNumInstancesperContainer)
          .increaseBy(DEFAULT_CONTAINER_PADDING),
          containerPlan.getRequiredResource().getRam());

      Assert.assertEquals(instanceDefaultResources.getDisk()
          .multiply(defaultNumInstancesperContainer)
          .increaseBy(DEFAULT_CONTAINER_PADDING),
          containerPlan.getRequiredResource().getDisk());
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
    ByteAmount boltRam = ByteAmount.fromGigabytes(4);
    ByteAmount maxContainerRam = ByteAmount.fromGigabytes(10);
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
        Assert.assertEquals(boltRam.increaseBy(paddingPercentage),
            containerPlan.getRequiredResource().getRam());
      }
      if (containerPlan.getInstances().size() == 3) {
        ByteAmount resourceRam = boltRam.plus(instanceDefaultResources.getRam().multiply(2));
        Assert.assertEquals(resourceRam.increaseBy(paddingPercentage),
            containerPlan.getRequiredResource().getRam());
      }
    }
  }

  /**
   * Test the scenario ram map config is partially set and scaling is requested
   */
  @Test
  public void testPartialRamMapScaling() throws Exception {

    ByteAmount boltRam = ByteAmount.fromGigabytes(4);
    ByteAmount maxContainerRam = ByteAmount.fromGigabytes(10);
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
   * Test the scenario where scaling down and up is simultaneously requested and padding is
   * configured
   */
  @Test
  public void scaleDownAndUpWithExtraPadding() throws Exception {
    int paddingPercentage = 50;
    topologyConfig.setContainerPaddingPercentage(paddingPercentage);
    ByteAmount spoutRam = ByteAmount.fromGigabytes(2);
    ByteAmount maxContainerRam = ByteAmount.fromGigabytes(12);
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
    ByteAmount spoutRam = ByteAmount.fromGigabytes(4);
    ByteAmount maxContainerRam = ByteAmount.fromGigabytes(12);
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
