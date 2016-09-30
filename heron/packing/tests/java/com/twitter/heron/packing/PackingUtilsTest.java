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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

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
}
