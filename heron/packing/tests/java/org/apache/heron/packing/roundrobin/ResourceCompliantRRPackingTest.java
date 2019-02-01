/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.heron.packing.roundrobin;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

import org.apache.heron.api.generated.TopologyAPI;
import org.apache.heron.common.basics.ByteAmount;
import org.apache.heron.common.basics.Pair;
import org.apache.heron.packing.CommonPackingTests;
import org.apache.heron.packing.utils.PackingUtils;
import org.apache.heron.spi.packing.IPacking;
import org.apache.heron.spi.packing.IRepacking;
import org.apache.heron.spi.packing.InstanceId;
import org.apache.heron.spi.packing.PackingException;
import org.apache.heron.spi.packing.PackingPlan;
import org.apache.heron.spi.packing.Resource;

public class ResourceCompliantRRPackingTest extends CommonPackingTests {

  @Override
  protected IPacking getPackingImpl() {
    return new ResourceCompliantRRPacking();
  }

  @Override
  protected IRepacking getRepackingImpl() {
    return new ResourceCompliantRRPacking();
  }

  @Test (expected = PackingException.class)
  public void testFailureInsufficientContainerRam() throws Exception {
    topologyConfig.setContainerRamRequested(ByteAmount.ZERO);
    pack(getTopology(spoutParallelism, boltParallelism, topologyConfig));
  }

  @Test (expected = PackingException.class)
  public void testFailureInsufficientContainerCpu() throws Exception {
    topologyConfig.setContainerCpuRequested(1.0);
    pack(getTopology(spoutParallelism, boltParallelism, topologyConfig));
  }

  /**
   * Test the scenario where the max container size is the default
   */
  @Test
  public void testDefaultResources() throws Exception {
    doPackingTest(topology,
        instanceDefaultResources, boltParallelism,
        instanceDefaultResources, spoutParallelism,
        3, getDefaultMaxContainerResource());
  }

  /**
   * Test the scenario where the max container size is the default and padding is configured
   */
  @Test
  public void testDefaultContainerSizeWithPadding() throws Exception {
    int padding = 50;
    topologyConfig.setContainerPaddingPercentage(padding);
    topology = getTopology(spoutParallelism, boltParallelism, topologyConfig);

    doPackingTest(topology,
        instanceDefaultResources, boltParallelism,
        instanceDefaultResources, spoutParallelism,
        4, getDefaultMaxContainerResource());
  }

  /**
   * Test the scenario where container level resource config are set
   */
  @Test
  public void testContainerRequestedResourcesSingleContainer() throws Exception {
    int numContainers = 1;

    // Set up the topology and its config
    topologyConfig.setNumStmgrs(numContainers);
    // Explicit set resources for container
    ByteAmount containerRam = ByteAmount.fromGigabytes(10);
    ByteAmount containerDisk = ByteAmount.fromGigabytes(20);
    double containerCpu = 30;
    Resource containerResource = new Resource(containerCpu, containerRam, containerDisk);

    Resource padding = PackingUtils.finalizePadding(
        new Resource(containerCpu, containerRam, containerDisk),
        new Resource(PackingUtils.DEFAULT_CONTAINER_CPU_PADDING,
            PackingUtils.DEFAULT_CONTAINER_RAM_PADDING,
            PackingUtils.DEFAULT_CONTAINER_RAM_PADDING),
        PackingUtils.DEFAULT_CONTAINER_PADDING_PERCENTAGE);

    topologyConfig.setContainerRamRequested(containerRam);
    topologyConfig.setContainerDiskRequested(containerDisk);
    topologyConfig.setContainerCpuRequested(containerCpu);
    topology = getTopology(spoutParallelism, boltParallelism, topologyConfig);

    PackingPlan packingPlan = doPackingTest(topology,
        instanceDefaultResources, boltParallelism,
        instanceDefaultResources, spoutParallelism,
        numContainers, containerResource);

    for (PackingPlan.ContainerPlan containerPlan : packingPlan.getContainers()) {
      Assert.assertEquals(Math.round(totalInstances * instanceDefaultResources.getCpu()
              + padding.getCpu()),
          (long) containerPlan.getRequiredResource().getCpu());

      Assert.assertEquals(instanceDefaultResources.getRam()
              .multiply(totalInstances)
              .plus(padding.getRam()),
          containerPlan.getRequiredResource().getRam());

      Assert.assertEquals(instanceDefaultResources.getDisk()
              .multiply(totalInstances)
              .plus(padding.getDisk()),
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
    // each of the 7 instances will get their own container
    doTestContainerCountRequested(8, 7);
  }

  /**
   * Test the scenario RAM map config is partially set
   */
  @Test
  public void testCompleteRamMapRequested() throws Exception {
    int numContainers = 3;

    // Explicit set resources for container
    // the value should be ignored, since we set the complete component RAM map
    ByteAmount containerRam = ByteAmount.fromGigabytes(Long.MAX_VALUE);

    // Explicit set component RAM map
    ByteAmount boltRam = ByteAmount.fromGigabytes(1);

    topologyConfig.setContainerRamRequested(containerRam);
    topologyConfig.setComponentRam(BOLT_NAME, boltRam);

    topology = getTopology(spoutParallelism, boltParallelism, topologyConfig);

    doPackingTest(topology,
        instanceDefaultResources.cloneWithRam(boltRam), boltParallelism,
        instanceDefaultResources, spoutParallelism,
        numContainers, getDefaultMaxContainerResource().cloneWithRam(containerRam));
  }

  /**
   * Test the scenario RAM map config is fully set
   */
  @Test
  public void testPartialRamMap() throws Exception {
    int numContainers = 3;

    // Explicit set resources for container
    ByteAmount containerRam = ByteAmount.fromGigabytes(10);

    // Explicit set component RAM map
    ByteAmount boltRam = ByteAmount.fromGigabytes(1);
    ByteAmount spoutRam = ByteAmount.fromGigabytes(2);

    topologyConfig.setContainerRamRequested(containerRam);
    topologyConfig.setComponentRam(BOLT_NAME, boltRam);
    topologyConfig.setComponentRam(SPOUT_NAME, spoutRam);

    topology = getTopology(spoutParallelism, boltParallelism, topologyConfig);

    doPackingTest(topology,
        instanceDefaultResources.cloneWithRam(boltRam), boltParallelism,
        instanceDefaultResources.cloneWithRam(spoutRam), spoutParallelism,
        numContainers, getDefaultMaxContainerResource().cloneWithRam(containerRam));
  }

  /**
   * Test the scenario where the user defined number of containers is not sufficient.
   */
  @Test
  public void testInsufficientContainersWithOneAdjustment() throws Exception {
    int numContainers = 1;

    // Set up the topology and its config
    topologyConfig.setNumStmgrs(numContainers);

    // Explicit set resources for container
    ByteAmount containerRam = ByteAmount.fromGigabytes(2);
    topologyConfig.setContainerRamRequested(containerRam);

    topology = getTopology(spoutParallelism, boltParallelism, topologyConfig);
    doPackingTest(topology,
        instanceDefaultResources, boltParallelism,
        instanceDefaultResources, spoutParallelism,
        7, getDefaultMaxContainerResource().cloneWithRam(containerRam));
  }

  /**
   * Test the scenario where the user defined number of containers is not sufficient.
   */
  @Test
  public void testInsufficientContainersWithMultipleAdjustments() throws Exception {
    int numContainers = 1;

    // Set up the topology and its config
    topologyConfig.setNumStmgrs(numContainers);

    // Explicit set resources for container
    ByteAmount containerRam = ByteAmount.fromGigabytes(3);

    // Explicit set component RAM map
    ByteAmount boltRam = ByteAmount.fromGigabytes(1);
    ByteAmount spoutRam = ByteAmount.fromGigabytes(2);

    topologyConfig.setContainerRamRequested(containerRam);
    topologyConfig.setComponentRam(BOLT_NAME, boltRam);
    topologyConfig.setComponentRam(SPOUT_NAME, spoutRam);

    topology = getTopology(spoutParallelism, boltParallelism, topologyConfig);
    doPackingTest(topology,
        instanceDefaultResources.cloneWithRam(boltRam), boltParallelism,
        instanceDefaultResources.cloneWithRam(spoutRam), spoutParallelism,
        7, getDefaultMaxContainerResource().cloneWithRam(containerRam));
  }

  /**
   * test even packing of instances
   */
  @Test
  public void testEvenPacking() throws Exception {
    int numContainers = 3;
    int componentParallelism = 4;
    boltParallelism = componentParallelism;
    spoutParallelism = componentParallelism;
    // Set up the topology and its config
    topologyConfig.setNumStmgrs(numContainers);
    topology = getTopology(spoutParallelism, boltParallelism, topologyConfig);

    doPackingTest(topology,
        instanceDefaultResources, boltParallelism,
        instanceDefaultResources, spoutParallelism,
        numContainers, getDefaultMaxContainerResource());
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
    int numContainersBeforeRepack = 3;
    int numContainersAfterRepack = 5;

    doDefaultScalingTest(componentChanges, numContainersBeforeRepack, numContainersAfterRepack,
        getDefaultMaxContainerResource());
  }

  /**
   * Test the scenario RAM map config is partially set and scaling is requested
   */
  @Test
  public void testRepackPadding() throws Exception {
    int paddingPercentage = 50;
    topologyConfig.setContainerPaddingPercentage(paddingPercentage);
    // Explicit set component RAM map
    ByteAmount boltRam = ByteAmount.fromGigabytes(4);
    ByteAmount maxContainerRam = ByteAmount.fromGigabytes(10);
    topologyConfig.setComponentRam(BOLT_NAME, boltRam);
    topologyConfig.setContainerRamRequested(maxContainerRam);
    topology = getTopology(spoutParallelism, boltParallelism, topologyConfig);

    int numScalingInstances = 3;
    Map<String, Integer> componentChanges = new HashMap<>();
    componentChanges.put(BOLT_NAME, numScalingInstances);

    int numContainersBeforeRepack = 4;
    int numContainersAfterRepack = 6;

    doPackingAndScalingTest(topology, componentChanges,
        instanceDefaultResources.cloneWithRam(boltRam), boltParallelism,
        instanceDefaultResources, spoutParallelism,
        numContainersBeforeRepack, numContainersAfterRepack,
        getDefaultMaxContainerResource().cloneWithRam(maxContainerRam));
  }

  /**
   * Test the scenario RAM map config is partially set and scaling is requested
   */
  @Test
  public void testPartialRamMapScaling() throws Exception {
    // Explicit set resources for container
    ByteAmount maxContainerRam = ByteAmount.fromGigabytes(10);
    // Explicit set component RAM map
    ByteAmount boltRam = ByteAmount.fromGigabytes(4);
    topologyConfig.setContainerRamRequested(maxContainerRam);
    topologyConfig.setComponentRam(BOLT_NAME, boltRam);

    topology = getTopology(spoutParallelism, boltParallelism, topologyConfig);

    int numScalingInstances = 3;
    Map<String, Integer> componentChanges = new HashMap<>();
    componentChanges.put(BOLT_NAME, numScalingInstances);

    int numContainersBeforeRepack = 3;
    int numContainersAfterRepack = 4;
    doPackingAndScalingTest(topology, componentChanges,
        instanceDefaultResources.cloneWithRam(boltRam), boltParallelism,
        instanceDefaultResources, spoutParallelism,
        numContainersBeforeRepack, numContainersAfterRepack,
        getDefaultMaxContainerResource().cloneWithRam(maxContainerRam));
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
    int numContainersBeforeRepack = 3;
    int numContainersAfterRepack = 2;
    doDefaultScalingTest(componentChanges, numContainersBeforeRepack, numContainersAfterRepack,
        getDefaultMaxContainerResource());
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
    int numContainersBeforeRepack = 3;
    int numContainersAfterRepack = 1;
    doDefaultScalingTest(componentChanges, numContainersBeforeRepack, numContainersAfterRepack,
        getDefaultMaxContainerResource());
  }

  /**
   * Test the scenario where scaling down and up is simultaneously requested and padding is
   * configured
   */
  @Test
  public void scaleDownAndUpWithExtraPadding() throws Exception {
    int paddingPercentage = 50;
    int numContainers = 2;
    topologyConfig.setContainerPaddingPercentage(paddingPercentage);
    // Explicit set resources for container
    ByteAmount maxContainerRam = ByteAmount.fromGigabytes(12);
    // Explicit set component RAM map
    ByteAmount spoutRam = ByteAmount.fromGigabytes(2);
    topologyConfig.setContainerRamRequested(maxContainerRam);
    topologyConfig.setComponentRam(SPOUT_NAME, spoutRam);
    topologyConfig.setNumStmgrs(numContainers);

    boltParallelism = 2;
    spoutParallelism = 1;

    topology = getTopology(spoutParallelism, boltParallelism, topologyConfig);

    int spoutScalingUp = 1;
    int boltScalingDown = -2;

    Map<String, Integer> componentChanges = new HashMap<>();
    componentChanges.put(SPOUT_NAME, spoutScalingUp); // 2 spouts
    componentChanges.put(BOLT_NAME, boltScalingDown); // 0 bolts
    int numContainersBeforeRepack = 2;
    int numContainersAfterRepack = 1;

    doPackingAndScalingTest(topology, componentChanges,
        instanceDefaultResources, boltParallelism,
        instanceDefaultResources.cloneWithRam(spoutRam), spoutParallelism,
        numContainersBeforeRepack, numContainersAfterRepack,
        getDefaultMaxContainerResource().cloneWithRam(maxContainerRam));
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
    // Explicit set component RAM map
    ByteAmount spoutRam = ByteAmount.fromGigabytes(4);
    topologyConfig.setContainerRamRequested(maxContainerRam);
    topologyConfig.setComponentRam(SPOUT_NAME, spoutRam);
    topologyConfig.setNumStmgrs(numContainers);

    boltParallelism = 3;
    spoutParallelism = 1;

    topology = getTopology(spoutParallelism, boltParallelism, topologyConfig);

    int spoutScalingUp = 1;
    int boltScalingDown = -1;

    Map<String, Integer> componentChanges = new HashMap<>();
    componentChanges.put(SPOUT_NAME, spoutScalingUp); // 2 spouts
    componentChanges.put(BOLT_NAME, boltScalingDown); // 2 bolts
    int numContainersBeforeRepack = 2;
    int numContainersAfterRepack = 2;

    doPackingAndScalingTest(topology, componentChanges,
        instanceDefaultResources, boltParallelism,
        instanceDefaultResources.cloneWithRam(spoutRam), spoutParallelism,
        numContainersBeforeRepack, numContainersAfterRepack,
        getDefaultMaxContainerResource().cloneWithRam(maxContainerRam));
  }

  @Test
  public void scaleUpMultiple() throws Exception {
    int spoutScalingUp = 4;
    int boltScalingUp = 4;

    Map<String, Integer> componentChanges = new HashMap<>();
    componentChanges.put(SPOUT_NAME, spoutScalingUp); // 8 spouts
    componentChanges.put(BOLT_NAME, boltScalingUp); // 8 bolts
    int numContainersBeforeRepack = 3;
    int numContainersAfterRepack = 5;
    doDefaultScalingTest(componentChanges, numContainersBeforeRepack, numContainersAfterRepack,
        getDefaultMaxContainerResource());
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
    int numContainersBeforeRepack = 3;
    int numContainersAfterRepack = 4;
    doDefaultScalingTest(componentChanges, numContainersBeforeRepack, numContainersAfterRepack,
        getDefaultMaxContainerResource());
  }

  @Test(expected = PackingException.class)
  public void testScaleDownInvalidScaleFactor() throws Exception {
    //try to remove more spout instances than possible
    int spoutScalingDown = -5;
    Map<String, Integer> componentChanges = new HashMap<>();
    componentChanges.put(SPOUT_NAME, spoutScalingDown);

    int numContainersBeforeRepack = 3;
    doDefaultScalingTest(componentChanges, numContainersBeforeRepack, numContainersBeforeRepack,
        getDefaultMaxContainerResource());
  }

  @Test(expected = PackingException.class)
  public void testScaleDownInvalidComponent() throws Exception {
    //try to remove a component that does not exist
    Map<String, Integer> componentChanges = new HashMap<>();
    componentChanges.put("SPOUT_FAKE", -10);
    int numContainersBeforeRepack = 3;
    doDefaultScalingTest(componentChanges, numContainersBeforeRepack, numContainersBeforeRepack,
        getDefaultMaxContainerResource());
  }

  /**
   * Test invalid RAM for instance
   */
  @Test(expected = PackingException.class)
  public void testInvalidRamInstance() throws Exception {
    ByteAmount maxContainerRam = ByteAmount.fromGigabytes(10);
    ByteAmount boltRam = ByteAmount.ZERO;
    topologyConfig.setContainerRamRequested(maxContainerRam);
    topologyConfig.setComponentRam(BOLT_NAME, boltRam);
    topology = getTopology(spoutParallelism, boltParallelism, topologyConfig);

    doPackingTest(topology,
        instanceDefaultResources.cloneWithRam(boltRam), boltParallelism,
        instanceDefaultResources, spoutParallelism,
        0, getDefaultMaxContainerResource().cloneWithRam(maxContainerRam));
  }

  @Test
  public void testTwoContainersRequested() throws Exception {
    doTestContainerCountRequested(2, 2);
  }

  /**
   * Test the scenario where container level resource config are set
   */
  protected void doTestContainerCountRequested(int requestedContainers,
                                               int expectedContainer) throws Exception {

    // Explicit set resources for container
    topologyConfig.setContainerRamRequested(ByteAmount.fromGigabytes(10));
    topologyConfig.setContainerDiskRequested(ByteAmount.fromGigabytes(20));
    topologyConfig.setContainerCpuRequested(30);
    topologyConfig.setNumStmgrs(requestedContainers);

    TopologyAPI.Topology topologyExplicitResourcesConfig =
        getTopology(spoutParallelism, boltParallelism, topologyConfig);
    PackingPlan packingPlanExplicitResourcesConfig = pack(topologyExplicitResourcesConfig);

    Assert.assertEquals(expectedContainer,
        packingPlanExplicitResourcesConfig.getContainers().size());
    Assert.assertEquals(totalInstances, packingPlanExplicitResourcesConfig.getInstanceCount());

    // RAM for bolt/spout should be the value in component RAM map
    for (PackingPlan.ContainerPlan containerPlan
        : packingPlanExplicitResourcesConfig.getContainers()) {
      for (PackingPlan.InstancePlan instancePlan : containerPlan.getInstances()) {
        Assert.assertEquals(instanceDefaultResources, instancePlan.getResource());
      }
    }
  }

  /**
   * Test the scenario where scaling down removes instances from containers that are most imbalanced
   * (i.e., tending towards homogeneity) first. If there is a tie (e.g. AABB, AB), chooses from the
   * container with the fewest instances, to favor ultimately removing  containers. If there is
   * still a tie, favor removing from higher numbered containers
   */
  @Test
  public void testScaleDownOneComponentRemoveContainer() throws Exception {
    @SuppressWarnings({"unchecked", "rawtypes"})
    Pair<Integer, InstanceId>[] initialComponentInstances = new Pair[] {
        new Pair<>(1, new InstanceId(SPOUT_NAME, 1, 0)),
        new Pair<>(1, new InstanceId(SPOUT_NAME, 2, 1)),
        new Pair<>(1, new InstanceId(BOLT_NAME, 3, 0)),
        new Pair<>(3, new InstanceId(BOLT_NAME, 4, 1)),
        new Pair<>(3, new InstanceId(BOLT_NAME, 5, 2)),
        new Pair<>(4, new InstanceId(BOLT_NAME, 6, 3)),
        new Pair<>(4, new InstanceId(BOLT_NAME, 7, 4))
    };

    Map<String, Integer> componentChanges = new HashMap<>();
    componentChanges.put(BOLT_NAME, -2);

    @SuppressWarnings({"unchecked", "rawtypes"})
    Pair<Integer, InstanceId>[] expectedComponentInstances = new Pair[] {
        new Pair<>(1, new InstanceId(SPOUT_NAME, 1, 0)),
        new Pair<>(1, new InstanceId(SPOUT_NAME, 2, 1)),
        new Pair<>(1, new InstanceId(BOLT_NAME, 3, 0)),
        new Pair<>(3, new InstanceId(BOLT_NAME, 4, 1)),
        new Pair<>(3, new InstanceId(BOLT_NAME, 5, 2)),
    };

    doScaleDownTest(initialComponentInstances, componentChanges, expectedComponentInstances);
  }

  @Test
  public void testScaleDownTwoComponentsRemoveContainer() throws Exception {
    @SuppressWarnings({"unchecked", "rawtypes"})
    Pair<Integer, InstanceId>[] initialComponentInstances = new Pair[] {
        new Pair<>(1, new InstanceId(SPOUT_NAME, 1, 0)),
        new Pair<>(1, new InstanceId(SPOUT_NAME, 2, 1)),
        new Pair<>(1, new InstanceId(BOLT_NAME, 3, 0)),
        new Pair<>(1, new InstanceId(BOLT_NAME, 4, 1)),
        new Pair<>(3, new InstanceId(SPOUT_NAME, 5, 2)),
        new Pair<>(3, new InstanceId(SPOUT_NAME, 6, 3)),
        new Pair<>(3, new InstanceId(BOLT_NAME, 7, 2)),
        new Pair<>(3, new InstanceId(BOLT_NAME, 8, 3))
    };

    Map<String, Integer> componentChanges = new HashMap<>();
    componentChanges.put(SPOUT_NAME, -2);
    componentChanges.put(BOLT_NAME, -2);

    @SuppressWarnings({"unchecked", "rawtypes"})
    Pair<Integer, InstanceId>[] expectedComponentInstances = new Pair[] {
        new Pair<>(1, new InstanceId(SPOUT_NAME, 1, 0)),
        new Pair<>(1, new InstanceId(SPOUT_NAME, 2, 1)),
        new Pair<>(1, new InstanceId(BOLT_NAME, 3, 0)),
        new Pair<>(1, new InstanceId(BOLT_NAME, 4, 1)),
    };

    doScaleDownTest(initialComponentInstances, componentChanges, expectedComponentInstances);
  }

  @Test
  public void testScaleDownHomogenousFirst() throws Exception {
    @SuppressWarnings({"unchecked", "rawtypes"})
    Pair<Integer, InstanceId>[] initialComponentInstances = new Pair[] {
        new Pair<>(1, new InstanceId(SPOUT_NAME, 1, 0)),
        new Pair<>(1, new InstanceId(SPOUT_NAME, 2, 1)),
        new Pair<>(1, new InstanceId(BOLT_NAME, 3, 0)),
        new Pair<>(3, new InstanceId(BOLT_NAME, 4, 1)),
        new Pair<>(3, new InstanceId(BOLT_NAME, 5, 2)),
        new Pair<>(3, new InstanceId(BOLT_NAME, 6, 3)),
        new Pair<>(3, new InstanceId(BOLT_NAME, 7, 4))
    };

    Map<String, Integer> componentChanges = new HashMap<>();
    componentChanges.put(BOLT_NAME, -4);

    @SuppressWarnings({"unchecked", "rawtypes"})
    Pair<Integer, InstanceId>[] expectedComponentInstances = new Pair[] {
        new Pair<>(1, new InstanceId(SPOUT_NAME, 1, 0)),
        new Pair<>(1, new InstanceId(SPOUT_NAME, 2, 1)),
        new Pair<>(1, new InstanceId(BOLT_NAME, 3, 0))
    };

    doScaleDownTest(initialComponentInstances, componentChanges, expectedComponentInstances);
  }
}
