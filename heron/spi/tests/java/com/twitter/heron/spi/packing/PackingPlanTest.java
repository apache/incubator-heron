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

package com.twitter.heron.spi.packing;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

import com.twitter.heron.spi.common.Constants;

public class PackingPlanTest {
  private static PackingPlan generatePacking(Map<Integer, List<InstanceId>> basePacking) {
    Resource resource = new Resource(1.0, 1 * Constants.GB, 10 * Constants.GB);

    Set<PackingPlan.ContainerPlan> containerPlans = new HashSet<>();

    for (int containerId : basePacking.keySet()) {
      List<InstanceId> instanceList = basePacking.get(containerId);

      Set<PackingPlan.InstancePlan> instancePlans = new HashSet<>();

      for (InstanceId instanceId : instanceList) {
        String componentName = instanceId.getComponentName();
        Resource instanceResource;
        if ("bolt".equals(componentName)) {
          instanceResource = new Resource(1.0, 2 * Constants.GB, 10 * Constants.GB);
        } else {
          instanceResource = new Resource(1.0, 3 * Constants.GB, 10 * Constants.GB);
        }
        instancePlans.add(new PackingPlan.InstancePlan(instanceId, instanceResource));
      }

      PackingPlan.ContainerPlan containerPlan =
          new PackingPlan.ContainerPlan(containerId, instancePlans, resource);

      containerPlans.add(containerPlan);
    }

    return new PackingPlan("", containerPlans);
  }

  @Test
  public void testComponentRamDistribution() {
    Map<Integer, List<InstanceId>> packing = new HashMap<>();
    packing.put(1, Arrays.asList(
        new InstanceId("spout", 1, 0),
        new InstanceId("bolt", 3, 0)));
    PackingPlan packingPlan = generatePacking(packing);

    String ramDistStr = packingPlan.getComponentRamDistribution();
    Assert.assertEquals("spout:3221225472,bolt:2147483648", ramDistStr);
  }

  @Test
  public void testPackingPlanSerde() {
    Map<Integer, List<InstanceId>> packing = new HashMap<>();
    packing.put(1, Arrays.asList(
        new InstanceId("spout", 1, 0),
        new InstanceId("bolt", 3, 0)));
    packing.put(2, Arrays.asList(
        new InstanceId("spout", 2, 1)));

    PackingPlan packingPlan = generatePacking(packing);

    PackingPlanProtoSerializer serializer = new PackingPlanProtoSerializer();
    PackingPlanProtoDeserializer deserializer = new PackingPlanProtoDeserializer();

    PackingPlan newPackingPlan = deserializer.fromProto(serializer.toProto(packingPlan));
    Assert.assertEquals("Packing plan not the same after converting to protobuf object and back",
        newPackingPlan, packingPlan);
    Assert.assertEquals("Packing plan ram distribution not the same after converting to "
            + "protobuf object and back",
        newPackingPlan.getComponentRamDistribution(), packingPlan.getComponentRamDistribution());
  }
}
