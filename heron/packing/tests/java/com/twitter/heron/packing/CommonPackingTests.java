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
package com.twitter.heron.packing;

import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.common.basics.ByteAmount;
import com.twitter.heron.common.basics.Pair;
import com.twitter.heron.spi.common.ClusterDefaults;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Context;
import com.twitter.heron.spi.common.Keys;
import com.twitter.heron.spi.packing.IPacking;
import com.twitter.heron.spi.packing.IRepacking;
import com.twitter.heron.spi.packing.InstanceId;
import com.twitter.heron.spi.packing.PackingException;
import com.twitter.heron.spi.packing.PackingPlan;
import com.twitter.heron.spi.packing.Resource;
import com.twitter.heron.spi.utils.TopologyTests;

/**
 * There is some common functionality in multiple packing plans. This class contains common tests.
 */
public abstract class CommonPackingTests {
  private static final String A  = "A";
  private static final String B = "B";

  protected static final String BOLT_NAME = "bolt";
  protected static final String SPOUT_NAME = "spout";
  protected static final int DEFAULT_CONTAINER_PADDING = 10;
  protected int spoutParallelism;
  protected int boltParallelism;
  protected Integer totalInstances;
  protected com.twitter.heron.api.Config topologyConfig;
  protected TopologyAPI.Topology topology;
  protected Resource instanceDefaultResources;

  protected abstract IPacking getPackingImpl();
  protected abstract IRepacking getRepackingImpl();

  @Before
  public void setUp() {
    this.spoutParallelism = 4;
    this.boltParallelism = 3;
    this.totalInstances = this.spoutParallelism + this.boltParallelism;

    // Set up the topology and its config. Tests can safely modify the config by reference after the
    // topology is created, but those changes will not be reflected in the underlying protobuf
    // object Config and Topology objects. This is typically fine for packing tests since they don't
    // access the protobuf values.
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

  protected static TopologyAPI.Topology getTopology(
      int spoutParallelism, int boltParallelism,
      com.twitter.heron.api.Config topologyConfig) {
    return TopologyTests.createTopology("testTopology", topologyConfig, SPOUT_NAME, BOLT_NAME,
        spoutParallelism, boltParallelism);
  }

  private static Config newTestConfig(TopologyAPI.Topology topology) {
    return Config.newBuilder()
            .put(Keys.topologyId(), topology.getId())
            .put(Keys.topologyName(), topology.getName())
            .putAll(ClusterDefaults.getDefaults())
            .build();
  }

  protected PackingPlan pack(TopologyAPI.Topology testTopology) {
    IPacking packing = getPackingImpl();
    packing.initialize(newTestConfig(testTopology), testTopology);
    return packing.pack();
  }

  private PackingPlan repack(TopologyAPI.Topology testTopology,
                               PackingPlan initialPackingPlan,
                               Map<String, Integer> componentChanges) {
    IRepacking repacking = getRepackingImpl();
    repacking.initialize(newTestConfig(testTopology), testTopology);
    return repacking.repack(initialPackingPlan, componentChanges);
  }

  protected PackingPlan doDefaultScalingTest(Map<String, Integer> componentChanges,
                                             int numContainersBeforeRepack) {
    return doScalingTest(topology, componentChanges,
        instanceDefaultResources.getRam(), boltParallelism,
        instanceDefaultResources.getRam(), spoutParallelism,
        numContainersBeforeRepack, totalInstances);
  }

  /**
   * Performs a scaling test for a specific topology. It first
   * computes an initial packing plan as a basis for scaling.
   * Given specific component parallelism changes, a new packing plan is produced.
   *
   * @param testTopology Input topology
   * @param componentChanges parallelism changes for scale up/down
   * @param boltRam ram allocated to bolts
   * @param testBoltParallelism bolt parallelism
   * @param spoutRam ram allocated to spouts
   * @param testSpoutParallelism spout parallelism
   * @param numContainersBeforeRepack number of containers that the initial packing plan should use
   * @param totalInstancesExpected number of instances expected before scaling
   * @return the new packing plan
   */
  protected PackingPlan doScalingTest(TopologyAPI.Topology testTopology,
                                      Map<String, Integer> componentChanges,
                                      ByteAmount boltRam,
                                      int testBoltParallelism, ByteAmount spoutRam,
                                      int testSpoutParallelism,
                                      int numContainersBeforeRepack,
                                      int totalInstancesExpected) {
    PackingPlan packingPlan = pack(testTopology);

    Assert.assertEquals(numContainersBeforeRepack, packingPlan.getContainers().size());
    Assert.assertEquals(totalInstancesExpected, (int) packingPlan.getInstanceCount());
    AssertPacking.assertContainers(packingPlan.getContainers(),
        BOLT_NAME, SPOUT_NAME, boltRam, spoutRam, null);
    AssertPacking.assertNumInstances(packingPlan.getContainers(), BOLT_NAME, testBoltParallelism);
    AssertPacking.assertNumInstances(packingPlan.getContainers(), SPOUT_NAME, testSpoutParallelism);

    PackingPlan newPackingPlan =
        repack(testTopology, packingPlan, componentChanges);
    AssertPacking.assertContainerRam(newPackingPlan.getContainers(),
        packingPlan.getMaxContainerResources().getRam());

    return newPackingPlan;
  }

  private void doScaleDownTest(Pair<Integer, InstanceId>[] initialComponentInstances,
                               Map<String, Integer> componentChanges,
                               Pair<Integer, InstanceId>[] expectedComponentInstances)
      throws ResourceExceededException {
    String topologyId = this.topology.getId();

    // The padding percentage used in repack() must be <= one as used in pack(), otherwise we can't
    // reconstruct the PackingPlan, see https://github.com/twitter/heron/issues/1577
    PackingPlan initialPackingPlan = PackingTestHelper.addToTestPackingPlan(
        topologyId, null, PackingTestHelper.toContainerIdComponentNames(initialComponentInstances),
        DEFAULT_CONTAINER_PADDING);
    AssertPacking.assertPackingPlan(topologyId, initialComponentInstances, initialPackingPlan);

    PackingPlan newPackingPlan = repack(this.topology, initialPackingPlan, componentChanges);

    AssertPacking.assertPackingPlan(topologyId, expectedComponentInstances, newPackingPlan);
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
        new Pair<>(1, new InstanceId(A, 1, 0)),
        new Pair<>(1, new InstanceId(A, 2, 1)),
        new Pair<>(1, new InstanceId(B, 3, 0)),
        new Pair<>(3, new InstanceId(B, 4, 1)),
        new Pair<>(3, new InstanceId(B, 5, 2)),
        new Pair<>(4, new InstanceId(B, 6, 3)),
        new Pair<>(4, new InstanceId(B, 7, 4))
    };

    Map<String, Integer> componentChanges = new HashMap<>();
    componentChanges.put(B, -2);

    @SuppressWarnings({"unchecked", "rawtypes"})
    Pair<Integer, InstanceId>[] expectedComponentInstances = new Pair[] {
        new Pair<>(1, new InstanceId(A, 1, 0)),
        new Pair<>(1, new InstanceId(A, 2, 1)),
        new Pair<>(1, new InstanceId(B, 3, 0)),
        new Pair<>(3, new InstanceId(B, 4, 1)),
        new Pair<>(3, new InstanceId(B, 5, 2)),
    };

    doScaleDownTest(initialComponentInstances, componentChanges, expectedComponentInstances);
  }

  @Test
  public void testScaleDownTwoComponentsRemoveContainer() throws Exception {
    @SuppressWarnings({"unchecked", "rawtypes"})
    Pair<Integer, InstanceId>[] initialComponentInstances = new Pair[] {
        new Pair<>(1, new InstanceId(A, 1, 0)),
        new Pair<>(1, new InstanceId(A, 2, 1)),
        new Pair<>(1, new InstanceId(B, 3, 0)),
        new Pair<>(1, new InstanceId(B, 4, 1)),
        new Pair<>(3, new InstanceId(A, 5, 2)),
        new Pair<>(3, new InstanceId(A, 6, 3)),
        new Pair<>(3, new InstanceId(B, 7, 2)),
        new Pair<>(3, new InstanceId(B, 8, 3))
    };

    Map<String, Integer> componentChanges = new HashMap<>();
    componentChanges.put(A, -2);
    componentChanges.put(B, -2);

    @SuppressWarnings({"unchecked", "rawtypes"})
    Pair<Integer, InstanceId>[] expectedComponentInstances = new Pair[] {
        new Pair<>(1, new InstanceId(A, 1, 0)),
        new Pair<>(1, new InstanceId(A, 2, 1)),
        new Pair<>(1, new InstanceId(B, 3, 0)),
        new Pair<>(1, new InstanceId(B, 4, 1)),
    };

    doScaleDownTest(initialComponentInstances, componentChanges, expectedComponentInstances);
  }

  @Test
  public void testScaleDownHomogenousFirst() throws Exception {
    @SuppressWarnings({"unchecked", "rawtypes"})
    Pair<Integer, InstanceId>[] initialComponentInstances = new Pair[] {
        new Pair<>(1, new InstanceId(A, 1, 0)),
        new Pair<>(1, new InstanceId(A, 2, 1)),
        new Pair<>(1, new InstanceId(B, 3, 0)),
        new Pair<>(3, new InstanceId(B, 4, 1)),
        new Pair<>(3, new InstanceId(B, 5, 2)),
        new Pair<>(3, new InstanceId(B, 6, 3)),
        new Pair<>(3, new InstanceId(B, 7, 4))
    };

    Map<String, Integer> componentChanges = new HashMap<>();
    componentChanges.put(B, -4);

    @SuppressWarnings({"unchecked", "rawtypes"})
    Pair<Integer, InstanceId>[] expectedComponentInstances = new Pair[] {
        new Pair<>(1, new InstanceId(A, 1, 0)),
        new Pair<>(1, new InstanceId(A, 2, 1)),
        new Pair<>(1, new InstanceId(B, 3, 0))
    };

    doScaleDownTest(initialComponentInstances, componentChanges, expectedComponentInstances);
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

  @Test(expected = PackingException.class)
  public void testScaleDownInvalidScaleFactor() throws Exception {

    //try to remove more spout instances than possible
    int spoutScalingDown = -5;
    Map<String, Integer> componentChanges = new HashMap<>();
    componentChanges.put(SPOUT_NAME, spoutScalingDown);

    int numContainersBeforeRepack = 2;
    doDefaultScalingTest(componentChanges, numContainersBeforeRepack);
  }

  @Test(expected = PackingException.class)
  public void testScaleDownInvalidComponent() throws Exception {
    Map<String, Integer> componentChanges = new HashMap<>();
    componentChanges.put("SPOUT_FAKE", -10); //try to remove a component that does not exist
    int numContainersBeforeRepack = 2;
    doDefaultScalingTest(componentChanges, numContainersBeforeRepack);
  }

  /**
   * Test invalid ram for instance
   */
  @Test(expected = PackingException.class)
  public void testInvalidRamInstance() throws Exception {
    ByteAmount maxContainerRam = ByteAmount.fromGigabytes(10);
    int defaultNumInstancesperContainer = 4;

    // Explicit set component ram map
    ByteAmount boltRam = ByteAmount.ZERO;

    topologyConfig.setContainerMaxRamHint(maxContainerRam);
    topologyConfig.setComponentRam(BOLT_NAME, boltRam);

    TopologyAPI.Topology topologyExplicitRamMap =
        getTopology(spoutParallelism, boltParallelism, topologyConfig);
    PackingPlan packingPlanExplicitRamMap = pack(topologyExplicitRamMap);
    Assert.assertEquals(totalInstances, packingPlanExplicitRamMap.getInstanceCount());
    AssertPacking.assertNumInstances(packingPlanExplicitRamMap.getContainers(), BOLT_NAME, 3);
    AssertPacking.assertNumInstances(packingPlanExplicitRamMap.getContainers(), SPOUT_NAME, 4);
    AssertPacking.assertContainerRam(packingPlanExplicitRamMap.getContainers(),
        instanceDefaultResources.getRam().multiply(defaultNumInstancesperContainer));
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
    topologyConfig.put(com.twitter.heron.api.Config.TOPOLOGY_STMGRS, requestedContainers);

    TopologyAPI.Topology topologyExplicitResourcesConfig =
        getTopology(spoutParallelism, boltParallelism, topologyConfig);
    PackingPlan packingPlanExplicitResourcesConfig = pack(topologyExplicitResourcesConfig);

    Assert.assertEquals(expectedContainer,
        packingPlanExplicitResourcesConfig.getContainers().size());
    Assert.assertEquals(totalInstances, packingPlanExplicitResourcesConfig.getInstanceCount());

    // Ram for bolt/spout should be the value in component ram map
    for (PackingPlan.ContainerPlan containerPlan
        : packingPlanExplicitResourcesConfig.getContainers()) {
      for (PackingPlan.InstancePlan instancePlan : containerPlan.getInstances()) {
        Assert.assertEquals(instanceDefaultResources, instancePlan.getResource());
      }
    }
  }
}
