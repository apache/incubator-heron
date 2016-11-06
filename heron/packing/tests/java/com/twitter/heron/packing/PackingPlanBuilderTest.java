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

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

import com.twitter.heron.spi.common.Constants;
import com.twitter.heron.spi.packing.InstanceId;
import com.twitter.heron.spi.packing.PackingPlan;
import com.twitter.heron.spi.packing.Resource;
import com.twitter.heron.spi.utils.PackingTestUtils;

public class PackingPlanBuilderTest {

  /**
   * Tests the getContainers method.
   */
  @Test
  public void testGetContainers() {

    int paddingPercentage = 10;
    Map<Integer, List<InstanceId>> packing = new HashMap<>();
    packing.put(7, Arrays.asList(
        new InstanceId("spout", 1, 0),
        new InstanceId("bolt", 2, 0)));
    packing.put(3, Arrays.asList(
        new InstanceId("spout", 3, 0),
        new InstanceId("bolt", 4, 0)));

    PackingPlan packingPlan = generatePacking(packing);
    Map<Integer, Container> containers =
        PackingPlanBuilder.getContainers(packingPlan, paddingPercentage);
    Assert.assertEquals(packing.size(), containers.size());
    for (Integer containerId : packing.keySet()) {
      Container foundContainer = containers.get(containerId);
      Assert.assertEquals(paddingPercentage, foundContainer.getPaddingPercentage());
      Assert.assertEquals(packingPlan.getMaxContainerResources(), foundContainer.getCapacity());
      Assert.assertEquals(2, foundContainer.getInstances().size());
    }
  }

  /**
   * Tests the sorting of containers based on the container Id.
   */
  @Test
  public void testContainerSortOnId() {

    Integer[] containerIds = {5, 4, 1, 2, 3};
    Integer[] instanceIds = {1, 2, 3};
    Set<PackingPlan.ContainerPlan> containers = generateContainers(containerIds, instanceIds);

    PackingPlan.ContainerPlan[] currentContainers =
        PackingPlanBuilder.sortOnContainerId(containers);

    Assert.assertEquals(containerIds.length, currentContainers.length);
    for (int i = 0; i < currentContainers.length; i++) {
      Assert.assertEquals((currentContainers[i]).getId(), i + 1);
    }
  }

  private static Set<PackingPlan.ContainerPlan> generateContainers(Integer[] containerIds,
                                                                   Integer[] instanceIds) {
    Set<PackingPlan.ContainerPlan> containerPlan = new HashSet<>();
    for (int containerId : containerIds) {
      containerPlan.add(PackingTestUtils.testContainerPlan(containerId, instanceIds));
    }
    return containerPlan;
  }

  private static PackingPlan generatePacking(Map<Integer, List<InstanceId>> basePacking)
      throws RuntimeException {
    Resource resource = new Resource(2.0, 6 * Constants.GB, 25 * Constants.GB);

    Set<PackingPlan.ContainerPlan> containerPlans = new HashSet<>();

    for (int containerId : basePacking.keySet()) {
      List<InstanceId> instanceList = basePacking.get(containerId);

      Set<PackingPlan.InstancePlan> instancePlans = new HashSet<>();

      for (InstanceId instanceId : instanceList) {
        String componentName = instanceId.getComponentName();
        Resource instanceResource;
        switch (componentName) {
          case "bolt":
            instanceResource = new Resource(1.0, 2 * Constants.GB, 10 * Constants.GB);
            break;
          case "spout":
            instanceResource = new Resource(1.0, 3 * Constants.GB, 10 * Constants.GB);
            break;
          default:
            throw new RuntimeException(String.format("%s is not a valid component name",
                componentName));
        }
        instancePlans.add(new PackingPlan.InstancePlan(instanceId, instanceResource));
      }
      PackingPlan.ContainerPlan containerPlan =
          new PackingPlan.ContainerPlan(containerId, instancePlans, resource);

      containerPlans.add(containerPlan);
    }

    return new PackingPlan("", containerPlans);
  }
}
