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

package org.apache.heron.packing;

import java.util.Map;
import java.util.Optional;

import org.junit.Assert;
import org.junit.Before;

import org.apache.heron.api.generated.TopologyAPI;
import org.apache.heron.common.basics.ByteAmount;
import org.apache.heron.common.basics.Pair;
import org.apache.heron.common.utils.topology.TopologyTests;
import org.apache.heron.packing.exceptions.ConstraintViolationException;
import org.apache.heron.packing.utils.PackingUtils;
import org.apache.heron.spi.common.Config;
import org.apache.heron.spi.common.Context;
import org.apache.heron.spi.packing.IPacking;
import org.apache.heron.spi.packing.IRepacking;
import org.apache.heron.spi.packing.InstanceId;
import org.apache.heron.spi.packing.PackingPlan;
import org.apache.heron.spi.packing.Resource;
import org.apache.heron.spi.utils.PackingTestUtils;

import static org.apache.heron.packing.AssertPacking.DELTA;

/**
 * There is some common functionality in multiple packing plans. This class contains common tests.
 */
public abstract class CommonPackingTests {
  protected static final String BOLT_NAME = "bolt";
  protected static final String SPOUT_NAME = "spout";
  protected static final int DEFAULT_CONTAINER_PADDING_PERCENT = 10;
  protected int spoutParallelism;
  protected int boltParallelism;
  protected Integer totalInstances;
  protected org.apache.heron.api.Config topologyConfig;
  protected TopologyAPI.Topology topology;
  protected Resource instanceDefaultResources;
  protected int numContainers;

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
    this.topologyConfig = new org.apache.heron.api.Config();
    this.topologyConfig.setTopologyContainerMaxNumInstances(4);
    this.topology = getTopology(spoutParallelism, boltParallelism, topologyConfig);

    Config config = PackingTestUtils.newTestConfig(this.topology);
    this.instanceDefaultResources = new Resource(
        Context.instanceCpu(config), Context.instanceRam(config), Context.instanceDisk(config));
  }

  protected static TopologyAPI.Topology getTopology(int spoutParallelism, int boltParallelism,
                                                    org.apache.heron.api.Config topologyConfig) {
    return TopologyTests.createTopology("testTopology", topologyConfig, SPOUT_NAME, BOLT_NAME,
        spoutParallelism, boltParallelism);
  }

  protected PackingPlan pack(TopologyAPI.Topology testTopology) {
    IPacking packing = getPackingImpl();
    packing.initialize(PackingTestUtils.newTestConfig(testTopology), testTopology);
    return packing.pack();
  }

  private PackingPlan repack(TopologyAPI.Topology testTopology,
                               PackingPlan initialPackingPlan,
                               Map<String, Integer> componentChanges) {
    IRepacking repacking = getRepackingImpl();
    repacking.initialize(PackingTestUtils.newTestConfig(testTopology), testTopology);
    return repacking.repack(initialPackingPlan, componentChanges);
  }

  protected Resource getDefaultMaxContainerResource() {
    return getDefaultMaxContainerResource(
        PackingUtils.DEFAULT_MAX_NUM_INSTANCES_PER_CONTAINER);
  }

  protected Resource getDefaultMaxContainerResource(int maxNumInstancesPerContainer) {
    return new Resource(this.instanceDefaultResources.getCpu() * maxNumInstancesPerContainer,
        this.instanceDefaultResources.getRam().multiply(maxNumInstancesPerContainer),
        this.instanceDefaultResources.getDisk().multiply(maxNumInstancesPerContainer));
  }

  protected Resource getDefaultUnspecifiedContainerResource(int testNumInstances,
                                                            int testNumContainers,
                                                            Resource padding) {
    int largestContainerSize = (int) Math.ceil((double) testNumInstances / testNumContainers);
    return new Resource(largestContainerSize + padding.getCpu(),
        ByteAmount.fromGigabytes(largestContainerSize).plus(padding.getRam()),
        ByteAmount.fromGigabytes(largestContainerSize).plus(padding.getDisk()));
  }

  protected PackingPlan doDefaultScalingTest(Map<String, Integer> componentChanges,
                                             int numContainersBeforeRepack,
                                             int numContainersAfterRepack,
                                             Resource maxContainerResource) {
    return doPackingAndScalingTest(topology, componentChanges,
        instanceDefaultResources, boltParallelism,
        instanceDefaultResources, spoutParallelism,
        numContainersBeforeRepack, numContainersAfterRepack, maxContainerResource);
  }

  /**
   * Performs a scaling test for a specific topology. It first
   * computes an initial packing plan as a basis for scaling.
   * Given specific component parallelism changes, a new packing plan is produced.
   *
   * @param testTopology Input topology
   * @param componentChanges parallelism changes for scale up/down
   * @param boltRes RAM allocated to bolts
   * @param testBoltParallelism bolt parallelism
   * @param spoutRes RAM allocated to spouts
   * @param testSpoutParallelism spout parallelism
   * @param numContainersBeforeRepack number of containers that the initial packing plan should use
   * @param numContainersAfterRepack number of instances expected before scaling
   * @return the new packing plan
   */
  protected PackingPlan doPackingAndScalingTest(TopologyAPI.Topology testTopology,
                                                Map<String, Integer> componentChanges,
                                                Resource boltRes, int testBoltParallelism,
                                                Resource spoutRes, int testSpoutParallelism,
                                                int numContainersBeforeRepack,
                                                int numContainersAfterRepack,
                                                Resource maxContainerResource) {
    PackingPlan packingPlan = doPackingTest(testTopology, boltRes, testBoltParallelism,
        spoutRes, testSpoutParallelism, numContainersBeforeRepack, maxContainerResource);
    PackingPlan newPackingPlan = doScalingTest(testTopology, packingPlan, componentChanges,
        boltRes, testBoltParallelism, spoutRes, testSpoutParallelism,
        numContainersAfterRepack, maxContainerResource);
    return newPackingPlan;
  }

  protected PackingPlan doPackingTest(TopologyAPI.Topology testTopology,
                                      Resource boltRes, int testBoltParallelism,
                                      Resource spoutRes, int testSpoutParallelism,
                                      int testNumContainers,
                                      Resource maxContainerResource) {
    PackingPlan packingPlan = pack(testTopology);

    Assert.assertEquals(testNumContainers, packingPlan.getContainers().size());
    Assert.assertEquals(testBoltParallelism + testSpoutParallelism,
        (int) packingPlan.getInstanceCount());
    AssertPacking.assertNumInstances(packingPlan.getContainers(), BOLT_NAME, testBoltParallelism);
    AssertPacking.assertNumInstances(packingPlan.getContainers(), SPOUT_NAME, testSpoutParallelism);

    AssertPacking.assertInstanceRam(packingPlan.getContainers(), BOLT_NAME, SPOUT_NAME,
        boltRes.getRam(), spoutRes.getRam());
    AssertPacking.assertInstanceCpu(packingPlan.getContainers(), BOLT_NAME, SPOUT_NAME,
        boltRes.getCpu(), spoutRes.getCpu());
    AssertPacking.assertInstanceIndices(packingPlan.getContainers(), BOLT_NAME, SPOUT_NAME);

    AssertPacking.assertContainerRam(packingPlan.getContainers(), maxContainerResource.getRam());
    AssertPacking.assertContainerCpu(packingPlan.getContainers(), maxContainerResource.getCpu());

    return packingPlan;
  }

  protected PackingPlan doPackingTestWithPartialResource(TopologyAPI.Topology testTopology,
                                                         Optional<ByteAmount> boltRam,
                                                         Optional<Double> boltCpu,
                                                         int testBoltParallelism,
                                                         Optional<ByteAmount> spoutRam,
                                                         Optional<Double> spoutCpu,
                                                         int testSpoutParallelism,
                                                         int testNumContainers,
                                                         Resource padding,
                                                         Resource maxContainerResource) {
    PackingPlan packingPlan = pack(testTopology);
    Assert.assertEquals(testNumContainers, packingPlan.getContainers().size());
    Assert.assertEquals(testBoltParallelism + testSpoutParallelism,
        (int) packingPlan.getInstanceCount());
    AssertPacking.assertNumInstances(packingPlan.getContainers(), BOLT_NAME, testBoltParallelism);
    AssertPacking.assertNumInstances(packingPlan.getContainers(), SPOUT_NAME, testSpoutParallelism);

    for (PackingPlan.ContainerPlan containerPlan : packingPlan.getContainers()) {
      int instancesCount = containerPlan.getInstances().size();

      if (!boltRam.isPresent() && !spoutRam.isPresent()) {
        ByteAmount instanceRam = maxContainerResource.getRam()
            .minus(padding.getRam()).divide(instancesCount);
        for (PackingPlan.InstancePlan instancePlan : containerPlan.getInstances()) {
          Assert.assertEquals(instanceRam, instancePlan.getResource().getRam());
        }
      } else if (!boltRam.isPresent() || !spoutRam.isPresent()) {
        String explicitComponent = boltRam.isPresent() ? BOLT_NAME : SPOUT_NAME;
        String implicitComponent = boltRam.isPresent() ? SPOUT_NAME : BOLT_NAME;
        ByteAmount explicitRam = boltRam.orElseGet(spoutRam::get);
        int explicitCount = 0;
        for (PackingPlan.InstancePlan instancePlan : containerPlan.getInstances()) {
          if (instancePlan.getComponentName().equals(explicitComponent)) {
            Assert.assertEquals(explicitRam, instancePlan.getResource().getRam());
            explicitCount++;
          }
        }
        int implicitCount = instancesCount - explicitCount;
        for (PackingPlan.InstancePlan instancePlan : containerPlan.getInstances()) {
          if (instancePlan.getComponentName().equals(implicitComponent)) {
            Assert.assertEquals(
                maxContainerResource.getRam()
                    .minus(explicitRam.multiply(explicitCount))
                    .minus(padding.getRam())
                    .divide(implicitCount),
                instancePlan.getResource().getRam());
          }
        }
      }

      if (!boltCpu.isPresent() && !spoutCpu.isPresent()) {
        double instanceCpu = (maxContainerResource.getCpu() - padding.getCpu()) / instancesCount;
        for (PackingPlan.InstancePlan instancePlan : containerPlan.getInstances()) {
          Assert.assertEquals(instanceCpu, instancePlan.getResource().getCpu(), DELTA);
        }
      } else if (!boltCpu.isPresent() || !spoutCpu.isPresent()) {
        String explicitComponent = boltCpu.isPresent() ? BOLT_NAME : SPOUT_NAME;
        String implicitComponent = boltCpu.isPresent() ? SPOUT_NAME : BOLT_NAME;
        double explicitCpu = boltCpu.orElseGet(spoutCpu::get);
        int explicitCount = 0;
        for (PackingPlan.InstancePlan instancePlan : containerPlan.getInstances()) {
          if (instancePlan.getComponentName().equals(explicitComponent)) {
            Assert.assertEquals(explicitCpu, instancePlan.getResource().getCpu(), DELTA);
            explicitCount++;
          }
        }
        int implicitCount = instancesCount - explicitCount;
        for (PackingPlan.InstancePlan instancePlan : containerPlan.getInstances()) {
          if (instancePlan.getComponentName().equals(implicitComponent)) {
            Assert.assertEquals(
                (maxContainerResource.getCpu()
                    - explicitCpu * explicitCount
                    - padding.getCpu()) / implicitCount,
                instancePlan.getResource().getCpu(), DELTA);
          }
        }
      }
    }

    AssertPacking.assertInstanceIndices(packingPlan.getContainers(), BOLT_NAME, SPOUT_NAME);
    AssertPacking.assertContainerRam(packingPlan.getContainers(), maxContainerResource.getRam());
    AssertPacking.assertContainerCpu(packingPlan.getContainers(), maxContainerResource.getCpu());

    return packingPlan;
  }

  protected PackingPlan doScalingTest(TopologyAPI.Topology testTopology,
                                      PackingPlan packingPlan,
                                      Map<String, Integer> componentChanges,
                                      Resource boltRes, int testBoltParallelism,
                                      Resource spoutRes, int testSpoutParallelism,
                                      int testNumContainers,
                                      Resource maxContainerResource) {
    PackingPlan newPackingPlan = repack(testTopology, packingPlan, componentChanges);
    Assert.assertEquals(testNumContainers, newPackingPlan.getContainers().size());
    AssertPacking.assertInstanceRam(newPackingPlan.getContainers(), BOLT_NAME, SPOUT_NAME,
        boltRes.getRam(), spoutRes.getRam());
    AssertPacking.assertInstanceCpu(newPackingPlan.getContainers(), BOLT_NAME, SPOUT_NAME,
        boltRes.getCpu(), spoutRes.getCpu());
//    AssertPacking.assertInstanceIndices(newPackingPlan.getContainers(), BOLT_NAME, SPOUT_NAME);
    AssertPacking.assertNumInstances(newPackingPlan.getContainers(), BOLT_NAME,
        testBoltParallelism + componentChanges.getOrDefault(BOLT_NAME, 0));
    AssertPacking.assertNumInstances(newPackingPlan.getContainers(), SPOUT_NAME,
        testSpoutParallelism + componentChanges.getOrDefault(SPOUT_NAME, 0));

    AssertPacking.assertContainerRam(newPackingPlan.getContainers(), maxContainerResource.getRam());
    AssertPacking.assertContainerCpu(newPackingPlan.getContainers(), maxContainerResource.getCpu());

    return newPackingPlan;
  }

  protected PackingPlan doScalingTestWithPartialResource(TopologyAPI.Topology testTopology,
                                                         PackingPlan packingPlan,
                                                         Map<String, Integer> componentChanges,
                                                         Optional<ByteAmount> boltRam,
                                                         Optional<Double> boltCpu,
                                                         int testBoltParallelism,
                                                         Optional<ByteAmount> spoutRam,
                                                         Optional<Double> spoutCpu,
                                                         int testSpoutParallelism,
                                                         int testNumContainers,
                                                         Resource padding,
                                                         Resource maxContainerResource) {
    System.out.println(packingPlan);
    PackingPlan newPackingPlan = repack(testTopology, packingPlan, componentChanges);
    System.out.println(newPackingPlan);
    Assert.assertEquals(testNumContainers, newPackingPlan.getContainers().size());
    Assert.assertEquals(testBoltParallelism + testSpoutParallelism
            + componentChanges.getOrDefault(BOLT_NAME, 0)
            + componentChanges.getOrDefault(SPOUT_NAME, 0),
        (int) newPackingPlan.getInstanceCount());
    AssertPacking.assertNumInstances(newPackingPlan.getContainers(), BOLT_NAME,
        testBoltParallelism + componentChanges.getOrDefault(BOLT_NAME, 0));
    AssertPacking.assertNumInstances(newPackingPlan.getContainers(), SPOUT_NAME,
        testSpoutParallelism + componentChanges.getOrDefault(SPOUT_NAME, 0));

    for (PackingPlan.ContainerPlan containerPlan : newPackingPlan.getContainers()) {
      int instancesCount = containerPlan.getInstances().size();

      if (!boltRam.isPresent() && !spoutRam.isPresent()) {
        ByteAmount instanceRam = maxContainerResource.getRam()
            .minus(padding.getRam()).divide(instancesCount);
        for (PackingPlan.InstancePlan instancePlan : containerPlan.getInstances()) {
          Assert.assertEquals(instanceRam, instancePlan.getResource().getRam());
        }
      } else if (!boltRam.isPresent() || !spoutRam.isPresent()) {
        String explicitComponent = boltRam.isPresent() ? BOLT_NAME : SPOUT_NAME;
        String implicitComponent = boltRam.isPresent() ? SPOUT_NAME : BOLT_NAME;
        ByteAmount explicitRam = boltRam.orElseGet(spoutRam::get);
        int explicitCount = 0;
        for (PackingPlan.InstancePlan instancePlan : containerPlan.getInstances()) {
          if (instancePlan.getComponentName().equals(explicitComponent)) {
            Assert.assertEquals(explicitRam, instancePlan.getResource().getRam());
            explicitCount++;
          }
        }
        int implicitCount = instancesCount - explicitCount;
        for (PackingPlan.InstancePlan instancePlan : containerPlan.getInstances()) {
          if (instancePlan.getComponentName().equals(implicitComponent)) {
            Assert.assertEquals(
                maxContainerResource.getRam()
                    .minus(explicitRam.multiply(explicitCount))
                    .minus(padding.getRam())
                    .divide(implicitCount),
                instancePlan.getResource().getRam());
          }
        }
      }

      if (!boltCpu.isPresent() && !spoutCpu.isPresent()) {
        double instanceCpu = (maxContainerResource.getCpu() - padding.getCpu()) / instancesCount;
        for (PackingPlan.InstancePlan instancePlan : containerPlan.getInstances()) {
          Assert.assertEquals(instanceCpu, instancePlan.getResource().getCpu(), DELTA);
        }
      } else if (!boltCpu.isPresent() || !spoutCpu.isPresent()) {
        String explicitComponent = boltCpu.isPresent() ? BOLT_NAME : SPOUT_NAME;
        String implicitComponent = boltCpu.isPresent() ? SPOUT_NAME : BOLT_NAME;
        double explicitCpu = boltCpu.orElseGet(spoutCpu::get);
        int explicitCount = 0;
        for (PackingPlan.InstancePlan instancePlan : containerPlan.getInstances()) {
          if (instancePlan.getComponentName().equals(explicitComponent)) {
            Assert.assertEquals(explicitCpu, instancePlan.getResource().getCpu(), DELTA);
            explicitCount++;
          }
        }
        int implicitCount = instancesCount - explicitCount;
        for (PackingPlan.InstancePlan instancePlan : containerPlan.getInstances()) {
          if (instancePlan.getComponentName().equals(implicitComponent)) {
            Assert.assertEquals(
                (maxContainerResource.getCpu()
                    - explicitCpu * explicitCount
                    - padding.getCpu()) / implicitCount,
                instancePlan.getResource().getCpu(), DELTA);
          }
        }
      }
    }

    AssertPacking.assertInstanceIndices(newPackingPlan.getContainers(), BOLT_NAME, SPOUT_NAME);
    AssertPacking.assertContainerRam(newPackingPlan.getContainers(), maxContainerResource.getRam());
    AssertPacking.assertContainerCpu(newPackingPlan.getContainers(), maxContainerResource.getCpu());

    return newPackingPlan;
  }

  protected void doScaleDownTest(Pair<Integer, InstanceId>[] initialComponentInstances,
                               Map<String, Integer> componentChanges,
                               Pair<Integer, InstanceId>[] expectedComponentInstances)
      throws ConstraintViolationException {
    String topologyId = this.topology.getId();

    // The padding percentage used in repack() must be <= one as used in pack(), otherwise we can't
    // reconstruct the PackingPlan, see https://github.com/apache/incubator-heron/issues/1577
    PackingPlan initialPackingPlan = PackingTestHelper.addToTestPackingPlan(
        topologyId, null, PackingTestHelper.toContainerIdComponentNames(initialComponentInstances),
        DEFAULT_CONTAINER_PADDING_PERCENT);
    AssertPacking.assertPackingPlan(topologyId, initialComponentInstances, initialPackingPlan);

    PackingPlan newPackingPlan = repack(this.topology, initialPackingPlan, componentChanges);

    AssertPacking.assertPackingPlan(topologyId, expectedComponentInstances, newPackingPlan);
  }
}
