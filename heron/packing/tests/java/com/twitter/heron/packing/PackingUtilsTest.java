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

import java.util.ArrayList;
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

public class PackingUtilsTest {

  private static Set<PackingPlan.ContainerPlan> generateContainers(Integer[] containerIds,
                                                                   Integer[] instanceIds) {
    Set<PackingPlan.ContainerPlan> containerPlan = new HashSet<>();
    for (int containerId : containerIds) {
      containerPlan.add(PackingTestUtils.testContainerPlan(containerId, instanceIds));
    }
    return containerPlan;
  }

  private static PackingPlan generatePacking(Map<Integer, List<InstanceId>> basePacking) {
    Resource resource = new Resource(2.0, 6 * Constants.GB, 25 * Constants.GB);

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


  /**
   * Tests the sorting of containers based on the container Id.
   */
  @Test
  public void testContainerSortOnId() {

    Integer[] containerIds = {5, 4, 1, 2, 3};
    Integer[] instanceIds = {1, 2, 3};
    Set<PackingPlan.ContainerPlan> containers = generateContainers(containerIds, instanceIds);

    PackingPlan.ContainerPlan[] currentContainers = PackingUtils.sortOnContainerId(containers);

    Assert.assertEquals(containerIds.length, currentContainers.length);
    for (int i = 0; i < currentContainers.length; i++) {
      Assert.assertEquals((currentContainers[i]).getId(), i + 1);
    }
  }

  /**
   * Tests the increaseBy method for long values
   */
  @Test
  public void testIncreaseByLong() {
    long value = 1024;
    int padding = 1;
    long expectedResult = 1034;
    Assert.assertEquals(expectedResult, PackingUtils.increaseBy(value, padding));
  }

  /**
   * Tests the increaseBy method for double values
   */
  @Test
  public void testIncreaseByDouble() {
    double value = 10.0;
    int padding = 1;
    double expectedResult = 10.1;
    Assert.assertEquals(0, Double.compare(PackingUtils.increaseBy(value, padding), expectedResult));
  }

  /**
   * Tests containerAllocation method
   */
  @Test
  public void testAllocateContainer() {
    ArrayList<Container> containers = new ArrayList<>();
    int expectedNumContainers = 1;
    Assert.assertEquals(expectedNumContainers, PackingUtils.allocateNewContainer(containers,
        new Resource(1, 1, 1), 10));
  }


  /**
   * Tests the component scale up and down methods.
   */
  @Test
  public void testComponentScaling() {

    Map<String, Integer> componentChanges = new HashMap<>();
    componentChanges.put("spout", -2);
    componentChanges.put("bolt1", 2);
    componentChanges.put("bolt2", -1);

    Map<String, Integer> componentToScaleUp = PackingUtils.getComponentsToScaleUp(componentChanges);
    Assert.assertEquals(1, componentToScaleUp.size());
    Assert.assertEquals(2, (int) componentToScaleUp.get("bolt1"));

    Map<String, Integer> componentToScaleDown =
        PackingUtils.getComponentsToScaleDown(componentChanges);
    Assert.assertEquals(2, componentToScaleDown.size());
    Assert.assertEquals(-2, (int) componentToScaleDown.get("spout"));
    Assert.assertEquals(-1, (int) componentToScaleDown.get("bolt2"));
  }

  /**
   * Tests the component scale up and down methods.
   */
  @Test
  public void testRemoveContainers() {

    Map<Integer, List<InstanceId>> allocation = new HashMap<>();
    allocation.put(1, new ArrayList<InstanceId>());

    ArrayList<InstanceId> instances = new ArrayList<InstanceId>();
    for (int i = 1; i <= 3; i++) {
      instances.add(new InstanceId("instance", 1, i));
    }
    allocation.put(2, instances);

    ArrayList<InstanceId> instances2 = new ArrayList<InstanceId>();
    instances2.add(new InstanceId("instance", 2, 1));
    allocation.put(3, instances2);

    allocation.put(4, new ArrayList<InstanceId>());

    PackingUtils.removeEmptyContainers(allocation);
    Assert.assertEquals(2, allocation.size());
    Assert.assertEquals(instances, allocation.get(2));
    Assert.assertEquals(instances2, allocation.get(3));
  }

  /**
   * Tests the getContainers method.
   */
  @Test
  public void testGetContainers() {

    int paddingPercentage = 10;
    Map<Integer, List<InstanceId>> packing = new HashMap<>();
    packing.put(1, Arrays.asList(
        new InstanceId("spout", 1, 0),
        new InstanceId("bolt", 2, 0)));
    packing.put(2, Arrays.asList(
        new InstanceId("spout", 3, 0),
        new InstanceId("bolt", 4, 0)));

    PackingPlan packingPlan = generatePacking(packing);
    ArrayList<Container> containers = PackingUtils.getContainers(packingPlan, paddingPercentage);
    Assert.assertEquals(2, containers.size());
    for (int i = 0; i < 2; i++) {
      Assert.assertEquals(paddingPercentage, containers.get(i).getPaddingPercentage());
      Assert.assertEquals(packingPlan.getMaxContainerResources(), containers.get(i).getCapacity());
      Assert.assertEquals(2, containers.get(i).getInstances().size());
    }
  }

  /**
   * Tests the getAllocation method.
   */
  @Test
  public void testGetAllocation() {

    Map<Integer, List<InstanceId>> packing = new HashMap<>();
    packing.put(1, Arrays.asList(
        new InstanceId("spout", 2, 0),
        new InstanceId("bolt", 2, 0)));
    packing.put(2, Arrays.asList(
        new InstanceId("spout", 3, 0),
        new InstanceId("bolt", 3, 0)));

    PackingPlan packingPlan = generatePacking(packing);
    Map<Integer, List<InstanceId>> allocation = PackingUtils.getAllocation(packingPlan);
    Assert.assertEquals(2, allocation.size());
    for (int i = 1; i <= 2; i++) {
      Assert.assertEquals(2, allocation.get(i).size());
    }
    for (int container = 1; container <= 2; container++) {
      Assert.assertEquals("spout", allocation.get(container).get(0).getComponentName());
      Assert.assertEquals("bolt", allocation.get(container).get(1).getComponentName());
      for (int i = 0; i < 2; i++) {
        Assert.assertEquals(0, allocation.get(container).get(i).getComponentIndex());
        Assert.assertEquals(container + 1, allocation.get(container).get(i).getTaskId());
      }
    }
  }
}
