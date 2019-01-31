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

package org.apache.heron.spi.packing;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

import org.apache.heron.common.basics.ByteAmount;
import org.apache.heron.spi.utils.PackingTestUtils;

public class PackingPlanTest {
  class TestInstance {
    public InstanceId id;
    public int ramInG;

    TestInstance(InstanceId id, int ramInG) {
      this.id = id;
      this.ramInG = ramInG;
    }
  }

  private static PackingPlan generatePacking(Map<Integer, List<TestInstance>> basePacking) {
    Resource resource
        = new Resource(1.0, ByteAmount.fromGigabytes(1), ByteAmount.fromGigabytes(10));

    Set<PackingPlan.ContainerPlan> containerPlans = new HashSet<>();

    for (int containerId : basePacking.keySet()) {
      List<TestInstance> instanceList = basePacking.get(containerId);

      Set<PackingPlan.InstancePlan> instancePlans = new HashSet<>();

      for (TestInstance instance : instanceList) {
        String componentName = instance.id.getComponentName();
        Resource instanceResource = new Resource(1.0,
            ByteAmount.fromGigabytes(instance.ramInG), ByteAmount.fromGigabytes(10));
        instancePlans.add(new PackingPlan.InstancePlan(instance.id, instanceResource));
      }

      PackingPlan.ContainerPlan containerPlan =
          new PackingPlan.ContainerPlan(containerId, instancePlans, resource);

      containerPlans.add(containerPlan);
    }

    return new PackingPlan("", containerPlans);
  }

  @Test
  public void testComponentRamDistribution() {
    Map<Integer, List<TestInstance>> packing = new HashMap<>();
    packing.put(1, Arrays.asList(
        new TestInstance(new InstanceId("spout", 1, 0), 3),
        new TestInstance(new InstanceId("bolt", 3, 0), 2)));
    PackingPlan packingPlan = generatePacking(packing);

    String ramDistStr = packingPlan.getComponentRamDistribution();
    Assert.assertEquals("spout:3221225472,bolt:2147483648", ramDistStr);
  }

  @Test
  public void testComponentRamDistributionUnbalanced() {
    Map<Integer, List<TestInstance>> packing = new HashMap<>();
    packing.put(1, Arrays.asList(
        new TestInstance(new InstanceId("spout", 1, 0), 3),
        new TestInstance(new InstanceId("bolt", 6, 0), 3)));
    packing.put(2, Arrays.asList(new TestInstance(new InstanceId("spout", 2, 1), 2)));
    packing.put(3, Arrays.asList(new TestInstance(new InstanceId("bolt", 3, 2), 1)));
    packing.put(4, Arrays.asList(new TestInstance(new InstanceId("bolt", 4, 3), 2)));
    packing.put(5, Arrays.asList(new TestInstance(new InstanceId("bolt", 5, 4), 3)));
    PackingPlan packingPlan = generatePacking(packing);

    String ramDistStr = packingPlan.getComponentRamDistribution();
    Assert.assertEquals("spout:2147483648,bolt:1073741824", ramDistStr);
  }

  @Test
  public void testPackingPlanSerde() {
    Map<Integer, List<TestInstance>> packing = new HashMap<>();
    packing.put(1, Arrays.asList(
        new TestInstance(new InstanceId("spout", 1, 0), 3),
        new TestInstance(new InstanceId("bolt", 3, 0), 2)));
    packing.put(2, Arrays.asList(
        new TestInstance(new InstanceId("spout", 2, 1), 3)));

    PackingPlan packingPlan = generatePacking(packing);

    PackingPlanProtoSerializer serializer = new PackingPlanProtoSerializer();
    PackingPlanProtoDeserializer deserializer = new PackingPlanProtoDeserializer();

    PackingPlan newPackingPlan = deserializer.fromProto(serializer.toProto(packingPlan));
    Assert.assertEquals("Packing plan not the same after converting to protobuf object and back",
        newPackingPlan, packingPlan);
    Assert.assertEquals("Packing plan RAM distribution not the same after converting to "
            + "protobuf object and back",
        newPackingPlan.getComponentRamDistribution(), packingPlan.getComponentRamDistribution());
  }

  @Test
  public void cloneWithHomogeneousScheduledResourceWillReturnUpdatedPacking() {
    PackingPlan.ContainerPlan largeContainer = PackingTestUtils.testContainerPlan(1, 0, 1, 2);
    PackingPlan.ContainerPlan smallContainer = PackingTestUtils.testContainerPlan(2, 3);

    Assert.assertTrue(largeContainer.getRequiredResource().getCpu()
        > smallContainer.getRequiredResource().getCpu());
    Assert.assertTrue(largeContainer.getRequiredResource().getRam()
        .greaterThan(smallContainer.getRequiredResource().getRam()));
    Assert.assertFalse(largeContainer.getScheduledResource().isPresent());
    Assert.assertFalse(smallContainer.getScheduledResource().isPresent());

    Set<PackingPlan.ContainerPlan> containers = new LinkedHashSet<>();
    containers.add(largeContainer);
    containers.add(smallContainer);

    PackingPlan plan = new PackingPlan("id", containers).cloneWithHomogeneousScheduledResource();

    PackingPlan.ContainerPlan[] containerArray
        = plan.getContainers().toArray(new PackingPlan.ContainerPlan[2]);
    Assert.assertEquals(2, plan.getContainers().size());

    largeContainer = containerArray[0];
    smallContainer = containerArray[1];

    Assert.assertTrue(largeContainer.getRequiredResource().getCpu()
        > smallContainer.getRequiredResource().getCpu());
    Assert.assertTrue(largeContainer.getRequiredResource().getRam()
        .greaterThan(smallContainer.getRequiredResource().getRam()));
    Assert.assertTrue(largeContainer.getScheduledResource().isPresent());
    Assert.assertTrue(smallContainer.getScheduledResource().isPresent());
    Assert.assertEquals(largeContainer.getScheduledResource().get().getCpu(),
        smallContainer.getScheduledResource().get().getCpu(), 0.1);
    Assert.assertEquals(largeContainer.getScheduledResource().get().getRam(),
        smallContainer.getScheduledResource().get().getRam());
  }
}
