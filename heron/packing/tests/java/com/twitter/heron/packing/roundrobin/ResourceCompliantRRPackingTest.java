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
import com.twitter.heron.common.basics.ByteAmount;
import com.twitter.heron.packing.AssertPacking;
import com.twitter.heron.packing.CommonPackingTests;
import com.twitter.heron.packing.utils.PackingUtils;
import com.twitter.heron.spi.packing.IPacking;
import com.twitter.heron.spi.packing.IRepacking;
import com.twitter.heron.spi.packing.PackingException;
import com.twitter.heron.spi.packing.PackingPlan;
import com.twitter.heron.spi.packing.Resource;
import com.twitter.heron.spi.utils.TopologyUtils;

public class ResourceCompliantRRPackingTest extends CommonPackingTests {

  @Override
  protected IPacking getPackingImpl() {
    return new ResourceCompliantRRPacking();
  }

  @Override
  protected IRepacking getRepackingImpl() {
    return new ResourceCompliantRRPacking();
  }

  private int countComponent(String component, Set<PackingPlan.InstancePlan> instances) {
    int count = 0;
    for (PackingPlan.InstancePlan instancePlan : instances) {
      if (component.equals(instancePlan.getComponentName())) {
        count++;
      }
    }
    return count;
  }

  @Test(expected = PackingException.class)
  public void testFailureInsufficientContainerRamRequested() throws Exception {
    topologyConfig.setContainerRamRequested(ByteAmount.ZERO);
    pack(getTopology(spoutParallelism, boltParallelism, topologyConfig));
  }

  /**
   * Test the scenario where the max container size is the default
   */
  @Test
  public void testDefaultResources() throws Exception {
    int numContainers = 2;
    PackingPlan packingPlanNoExplicitResourcesConfig = pack(topology);

    Assert.assertEquals(numContainers, packingPlanNoExplicitResourcesConfig.getContainers().size());
    Assert.assertEquals(totalInstances, packingPlanNoExplicitResourcesConfig.getInstanceCount());
  }

  /**
   * Test the scenario where the max container size is the default and padding is configured
   */
  @Test
  public void testDefaultContainerSizeWithPadding() throws Exception {
    int numContainers = 2;
    int padding = 50;
    topologyConfig.setContainerPaddingPercentage(padding);
    TopologyAPI.Topology newTopology = getTopology(spoutParallelism, boltParallelism,
        topologyConfig);

    PackingPlan packingPlan = pack(newTopology);

    Assert.assertEquals(numContainers, packingPlan.getContainers().size());
    Assert.assertEquals(totalInstances, packingPlan.getInstanceCount());
  }

  /**
   * Test the scenario where container level resource config are set
   */
  @Test
  public void testContainerRequestedResourcesSingleContainer() throws Exception {
    int numContainers = 1;

    // Set up the topology and its config
    topologyConfig.put(com.twitter.heron.api.Config.TOPOLOGY_STMGRS, numContainers);
    // Explicit set resources for container
    ByteAmount containerRam = ByteAmount.fromGigabytes(10);
    ByteAmount containerDisk = ByteAmount.fromGigabytes(20);
    float containerCpu = 30;

    topologyConfig.setContainerRamRequested(containerRam);
    topologyConfig.setContainerDiskRequested(containerDisk);
    topologyConfig.setContainerCpuRequested(containerCpu);
    TopologyAPI.Topology topologyExplicitResourcesConfig =
        getTopology(spoutParallelism, boltParallelism, topologyConfig);
    PackingPlan packingPlanExplicitResourcesConfig = pack(topologyExplicitResourcesConfig);

    Assert.assertEquals(numContainers, packingPlanExplicitResourcesConfig.getContainers().size());
    Assert.assertEquals(totalInstances, packingPlanExplicitResourcesConfig.getInstanceCount());

    for (PackingPlan.ContainerPlan containerPlan
        : packingPlanExplicitResourcesConfig.getContainers()) {
      Assert.assertEquals(Math.round(PackingUtils.increaseBy(totalInstances
              * instanceDefaultResources.getCpu(), DEFAULT_CONTAINER_PADDING)),
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

  @Test
  public void testContainersRequestedExceedsInstanceCount() throws Exception {
    doTestContainerCountRequested(8, 7); // each of the 7 instances will get their own container
  }

  /**
   * Test the scenario ram map config is partially set
   */
  @Test
  public void testCompleteRamMapRequested() throws Exception {
    int numContainers = 2;

    // Explicit set resources for container
    // the value should be ignored, since we set the complete component ram map
    ByteAmount containerRam = ByteAmount.fromGigabytes(Long.MAX_VALUE);

    // Explicit set component ram map
    ByteAmount boltRam = ByteAmount.fromGigabytes(1);

    topologyConfig.setContainerRamRequested(containerRam);
    topologyConfig.setComponentRam(BOLT_NAME, boltRam);

    TopologyAPI.Topology topologyExplicitRamMap =
        getTopology(spoutParallelism, boltParallelism, topologyConfig);
    PackingPlan packingPlanExplicitRamMap = pack(topologyExplicitRamMap);
    Assert.assertEquals(totalInstances, packingPlanExplicitRamMap.getInstanceCount());
    Assert.assertEquals(numContainers, packingPlanExplicitRamMap.getContainers().size());

    AssertPacking.assertContainers(packingPlanExplicitRamMap.getContainers(),
        BOLT_NAME, SPOUT_NAME, boltRam, instanceDefaultResources.getRam(), containerRam);
  }

  /**
   * Test the scenario ram map config is fully set
   */
  @Test
  public void testPartialRamMap() throws Exception {
    int numContainers = 2;

    // Explicit set resources for container
    ByteAmount containerRam = ByteAmount.fromGigabytes(10);

    // Explicit set component ram map
    ByteAmount boltRam = ByteAmount.fromGigabytes(1);
    ByteAmount spoutRam = ByteAmount.fromGigabytes(2);

    topologyConfig.setContainerRamRequested(containerRam);
    topologyConfig.setComponentRam(BOLT_NAME, boltRam);
    topologyConfig.setComponentRam(SPOUT_NAME, spoutRam);

    TopologyAPI.Topology topologyExplicitRamMap =
        getTopology(spoutParallelism, boltParallelism, topologyConfig);
    PackingPlan packingPlanExplicitRamMap = pack(topologyExplicitRamMap);
    Assert.assertEquals(totalInstances, packingPlanExplicitRamMap.getInstanceCount());
    Assert.assertEquals(numContainers, packingPlanExplicitRamMap.getContainers().size());

    AssertPacking.assertContainers(packingPlanExplicitRamMap.getContainers(),
        BOLT_NAME, SPOUT_NAME, boltRam, spoutRam, containerRam);
  }

  /**
   * Test the scenario where the user defined number of containers is not sufficient.
   */
  @Test
  public void testInsufficientContainersWithOneAdjustment() throws Exception {
    int numContainers = 1;

    // Set up the topology and its config
    topologyConfig.put(com.twitter.heron.api.Config.TOPOLOGY_STMGRS, numContainers);

    // Explicit set resources for container
    ByteAmount containerRam = ByteAmount.fromGigabytes(2);

    topologyConfig.setContainerRamRequested(containerRam);

    TopologyAPI.Topology newTopology =
        getTopology(spoutParallelism, boltParallelism, topologyConfig);
    PackingPlan packingPlan = pack(newTopology);
    Assert.assertEquals(7, packingPlan.getContainers().size());
    Assert.assertEquals(totalInstances, packingPlan.getInstanceCount());
  }

  /**
   * Test the scenario where the user defined number of containers is not sufficient.
   */
  @Test
  public void testInsufficientContainersWithMultipleAdjustments() throws Exception {
    int numContainers = 1;

    // Set up the topology and its config
    topologyConfig.put(com.twitter.heron.api.Config.TOPOLOGY_STMGRS, numContainers);

    // Explicit set resources for container
    ByteAmount containerRam = ByteAmount.fromGigabytes(3);

    // Explicit set component ram map
    ByteAmount boltRam = ByteAmount.fromGigabytes(1);
    ByteAmount spoutRam = ByteAmount.fromGigabytes(2);

    topologyConfig.setContainerRamRequested(containerRam);
    topologyConfig.setComponentRam(BOLT_NAME, boltRam);
    topologyConfig.setComponentRam(SPOUT_NAME, spoutRam);

    TopologyAPI.Topology topologyExplicitRamMap =
        getTopology(spoutParallelism, boltParallelism, topologyConfig);
    PackingPlan packingPlan = pack(topologyExplicitRamMap);
    Assert.assertEquals(7, packingPlan.getContainers().size());
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
    topologyConfig.put(com.twitter.heron.api.Config.TOPOLOGY_STMGRS, numContainers);

    TopologyAPI.Topology newTopology =
        getTopology(componentParallelism, componentParallelism, topologyConfig);

    int numInstance = TopologyUtils.getTotalInstance(newTopology);
    // Two components
    Assert.assertEquals(2 * componentParallelism, numInstance);
    PackingPlan output = pack(newTopology);
    Assert.assertEquals(numContainers, output.getContainers().size());
    Assert.assertEquals((Integer) numInstance, output.getInstanceCount());

    for (PackingPlan.ContainerPlan container : output.getContainers()) {
      Assert.assertEquals(numInstance / numContainers, container.getInstances().size());
      Assert.assertEquals(2, countComponent("spout", container.getInstances()));
      Assert.assertEquals(2, countComponent("bolt", container.getInstances()));
    }
  }

  /**
   * Test the scenario where the max container size is the default
   * and scaling is requested.
   */
  @Test
  public void testDefaultContainerSizeRepack() throws Exception {
    int numScalingInstances = 5;
    Map<String, Integer> componentChanges = new HashMap<>();
    componentChanges.put(BOLT_NAME, numScalingInstances);
    int numContainersBeforeRepack = 2;
    PackingPlan newPackingPlan = doDefaultScalingTest(componentChanges, numContainersBeforeRepack);
    Assert.assertEquals(4, newPackingPlan.getContainers().size());
    Assert.assertEquals((Integer) (totalInstances + numScalingInstances),
        newPackingPlan.getInstanceCount());
    AssertPacking.assertContainers(newPackingPlan.getContainers(),
        BOLT_NAME, SPOUT_NAME, instanceDefaultResources.getRam(),
        instanceDefaultResources.getRam(), null);
    for (PackingPlan.ContainerPlan containerPlan
        : newPackingPlan.getContainers()) {
      Assert.assertEquals(Math.round(PackingUtils.increaseBy(
          containerPlan.getInstances().size() * instanceDefaultResources.getCpu(),
          DEFAULT_CONTAINER_PADDING)), (long) containerPlan.getRequiredResource().getCpu());

      Assert.assertEquals(instanceDefaultResources.getRam()
              .multiply(containerPlan.getInstances().size())
              .increaseBy(DEFAULT_CONTAINER_PADDING),
          containerPlan.getRequiredResource().getRam());

      Assert.assertEquals(instanceDefaultResources.getDisk()
              .multiply(containerPlan.getInstances().size())
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
    topologyConfig.setContainerRamRequested(maxContainerRam);

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
      //Each container either contains a single bolt or 1 bolt and 2 spouts or 1 bolt and 1 spout
      if (containerPlan.getInstances().size() == 1) {
        Assert.assertEquals(boltRam.increaseBy(paddingPercentage),
            containerPlan.getRequiredResource().getRam());
      }
      if (containerPlan.getInstances().size() == 2) {
        Assert.assertEquals(boltRam.plus(instanceDefaultResources.getRam())
                .increaseBy(paddingPercentage),
            containerPlan.getRequiredResource().getRam());
      }
      if (containerPlan.getInstances().size() == 3) {
        Assert.assertEquals(boltRam.plus(instanceDefaultResources.getRam().multiply(2))
                .increaseBy(paddingPercentage),
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
    ByteAmount maxContainerRam = ByteAmount.fromGigabytes(10);
    // Explicit set component ram map
    ByteAmount boltRam = ByteAmount.fromGigabytes(4);
    topologyConfig.setContainerRamRequested(maxContainerRam);
    topologyConfig.setComponentRam(BOLT_NAME, boltRam);

    TopologyAPI.Topology topologyExplicitRamMap =
        getTopology(spoutParallelism, boltParallelism, topologyConfig);

    int numScalingInstances = 3;
    Map<String, Integer> componentChanges = new HashMap<>();
    componentChanges.put(BOLT_NAME, numScalingInstances);

    int numContainersBeforeRepack = 3;
    PackingPlan newPackingPlan = doScalingTest(topologyExplicitRamMap, componentChanges, boltRam,
        boltParallelism, instanceDefaultResources.getRam(), spoutParallelism,
        numContainersBeforeRepack, totalInstances);

    Assert.assertEquals(6, newPackingPlan.getContainers().size());
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
    int spoutScalingDown = -2;
    int boltScalingDown = -1;

    Map<String, Integer> componentChanges = new HashMap<>();
    componentChanges.put(SPOUT_NAME, spoutScalingDown); //leave 2 spouts
    componentChanges.put(BOLT_NAME, boltScalingDown); //leave 2 bolts
    int numContainersBeforeRepack = 2;
    PackingPlan newPackingPlan = doDefaultScalingTest(componentChanges, numContainersBeforeRepack);
    Assert.assertEquals(1, newPackingPlan.getContainers().size());
    Assert.assertEquals((Integer) (totalInstances + spoutScalingDown + boltScalingDown),
        newPackingPlan.getInstanceCount());
    AssertPacking.assertNumInstances(newPackingPlan.getContainers(),
        BOLT_NAME, 2);
    AssertPacking.assertNumInstances(newPackingPlan.getContainers(),
        SPOUT_NAME, 2);
  }

  /**
   * Test the scenario where the scaling down is requested and the first container is removed
   */
  @Test
  public void removeFirstContainer() throws Exception {
    int spoutScalingDown = -3;
    int boltScalingDown = -3;

     /* The packing plan consists of two containers. The first one contains 2 spouts and 2 bolts
       the second one contains 2 spouts and 1 bolt. During scaling we remove 3 spouts and 3 bolts
       and thus the first container is removed.
     */
    Map<String, Integer> componentChanges = new HashMap<>();
    componentChanges.put(SPOUT_NAME, spoutScalingDown); //leave 1 spout
    componentChanges.put(BOLT_NAME, boltScalingDown); //leave 1 bolt
    int numContainersBeforeRepack = 2;
    PackingPlan newPackingPlan = doDefaultScalingTest(componentChanges, numContainersBeforeRepack);
    Assert.assertEquals(1, newPackingPlan.getContainers().size());
    Assert.assertEquals((Integer) (totalInstances + spoutScalingDown + boltScalingDown),
        newPackingPlan.getInstanceCount());
    AssertPacking.assertNumInstances(newPackingPlan.getContainers(),
        BOLT_NAME, 0);
    AssertPacking.assertNumInstances(newPackingPlan.getContainers(),
        SPOUT_NAME, 1);
  }

  /**
   * Test the scenario where scaling down and up is simultaneously requested and padding is
   * configured
   */
  @Test
  public void scaleDownAndUpWithExtraPadding() throws Exception {
    int paddingPercentage = 50;
    int numContainers = 1;
    topologyConfig.setContainerPaddingPercentage(paddingPercentage);
    // Explicit set resources for container
    ByteAmount maxContainerRam = ByteAmount.fromGigabytes(12);
    // Explicit set component ram map
    ByteAmount spoutRam = ByteAmount.fromGigabytes(2);
    topologyConfig.setContainerRamRequested(maxContainerRam);
    topologyConfig.setComponentRam(SPOUT_NAME, spoutRam);
    topologyConfig.setNumStmgrs(numContainers);

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
    int numContainers = 1;

    topologyConfig.setContainerPaddingPercentage(paddingPercentage);
    // Explicit set resources for container
    ByteAmount maxContainerRam = ByteAmount.fromGigabytes(12);
    // Explicit set component ram map
    ByteAmount spoutRam = ByteAmount.fromGigabytes(4);
    topologyConfig.setContainerRamRequested(maxContainerRam);
    topologyConfig.setComponentRam(SPOUT_NAME, spoutRam);
    topologyConfig.setNumStmgrs(numContainers);

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

  @Test
  public void scaleUpMultiple() throws Exception {
    int spoutScalingUp = 4;
    int boltScalingUp = 4;

    Map<String, Integer> componentChanges = new HashMap<>();
    componentChanges.put(SPOUT_NAME, spoutScalingUp); // 8 spouts
    componentChanges.put(BOLT_NAME, boltScalingUp); // 8 bolts
    int numContainersBeforeRepack = 2;
    PackingPlan newPackingPlan = doDefaultScalingTest(componentChanges, numContainersBeforeRepack);
    Assert.assertEquals(4, newPackingPlan.getContainers().size());
    Assert.assertEquals((Integer) (totalInstances + spoutScalingUp + boltScalingUp),
        newPackingPlan.getInstanceCount());
    AssertPacking.assertNumInstances(newPackingPlan.getContainers(),
        BOLT_NAME, boltParallelism + boltScalingUp);
    AssertPacking.assertNumInstances(newPackingPlan.getContainers(),
        SPOUT_NAME, spoutParallelism + spoutScalingUp);
  }
}
