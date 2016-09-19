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

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.packing.binpacking.FirstFitDecreasingPacking;
import com.twitter.heron.spi.common.ClusterDefaults;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Keys;
import com.twitter.heron.spi.packing.PackingPlan;
import com.twitter.heron.spi.utils.TopologyTests;

public class PackingUtilsTest {

  private static final String BOLT_NAME = "bolt";
  private static final String SPOUT_NAME = "spout";

  private int spoutParallelism;
  private int boltParallelism;
  private com.twitter.heron.api.Config topologyConfig;
  private TopologyAPI.Topology topology;
  private int totalInstances;

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

  @Before
  public void setUp() {
    this.spoutParallelism = 4;
    this.boltParallelism = 3;
    this.totalInstances = spoutParallelism + boltParallelism;
    // Set up the topology and its config
    this.topologyConfig = new com.twitter.heron.api.Config();
    this.topology = getTopology(spoutParallelism, boltParallelism, topologyConfig);

    Config config = Config.newBuilder()
        .put(Keys.topologyId(), topology.getId())
        .put(Keys.topologyName(), topology.getName())
        .putAll(ClusterDefaults.getDefaults())
        .build();
  }

  /**
   * Tests the sorting of containers based on the container Id.
   */
  @Test
  public void testContainerSortOnId() {

    PackingPlan packingPlan = getFirstFitDecreasingPackingPlan(topology);

    Assert.assertEquals(packingPlan.getContainers().size(), 2);
    Assert.assertEquals((long) packingPlan.getInstanceCount(), totalInstances);
    AssertPacking.assertNumInstances(packingPlan.getContainers(), BOLT_NAME, boltParallelism);
    AssertPacking.assertNumInstances(packingPlan.getContainers(), SPOUT_NAME, spoutParallelism);

    PackingPlan.ContainerPlan[] currentContainers = packingPlan.getContainers().toArray(
        new PackingPlan.ContainerPlan[1]);

    Assert.assertEquals(currentContainers.length, packingPlan.getContainers().size());

    for (int i = 0; i < currentContainers.length; i++) {
      Assert.assertEquals((currentContainers[i]).getId(), i + 1);
    }
  }
}
