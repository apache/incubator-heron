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
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.junit.Assert;

import com.twitter.heron.spi.packing.PackingPlan;

/**
 * Utility methods for common test assertions related to packing
 */
public final class AssertPacking {

  private AssertPacking() {
  }

  /**
   * Verifies that the containerPlan has at least one bolt named boltName with ram equal to
   * expectedBoltRam and likewise for spouts. If notExpectedContainerRam is not null, verifies that
   * the container ram is not that.
   */
  public static void assertContainers(Set<PackingPlan.ContainerPlan> containerPlans,
                                      String boltName, String spoutName,
                                      long expectedBoltRam, long expectedSpoutRam,
                                      Long notExpectedContainerRam) {
    boolean boltFound = false;
    boolean spoutFound = false;
    List<Integer> expectedInstanceIndecies = new ArrayList<>();
    List<Integer> foundInstanceIndecies = new ArrayList<>();
    int expectedInstanceIndex = 1;
    // Ram for bolt should be the value in component ram map
    for (PackingPlan.ContainerPlan containerPlan : containerPlans) {
      if (notExpectedContainerRam != null) {
        Assert.assertNotEquals(
            notExpectedContainerRam, (Long) containerPlan.getRequiredResource().getRam());
      }
      for (PackingPlan.InstancePlan instancePlan : containerPlan.getInstances()) {
        expectedInstanceIndecies.add(expectedInstanceIndex++);
        foundInstanceIndecies.add(instancePlan.getTaskId());
        if (instancePlan.getComponentName().equals(boltName)) {
          Assert.assertEquals(expectedBoltRam, instancePlan.getResource().getRam());
          boltFound = true;
        }
        if (instancePlan.getComponentName().equals(spoutName)) {
          Assert.assertEquals(expectedSpoutRam, instancePlan.getResource().getRam());
          spoutFound = true;
        }
      }
    }
    Assert.assertTrue("Bolt not found in any of the container plans: " + boltName, boltFound);
    Assert.assertTrue("Spout not found in any of the container plans: " + spoutName, spoutFound);

    Collections.sort(foundInstanceIndecies);
    Assert.assertEquals("Unexpected instance global id set found.",
        expectedInstanceIndecies, foundInstanceIndecies);
  }

  /**
   * Verifies that the containerPlan contains a specific number of instances for the given component.
   */
  public static void assertNumInstances(Set<PackingPlan.ContainerPlan> containerPlans,
                                        String component, int numInstances) {
    int instancesFound = 0;
    for (PackingPlan.ContainerPlan containerPlan : containerPlans) {
      for (PackingPlan.InstancePlan instancePlan : containerPlan.getInstances()) {
        if (instancePlan.getComponentName().equals(component)) {
          instancesFound++;
        }
      }
    }
    Assert.assertEquals(numInstances, instancesFound);
  }

  /**
   * Verifies that the RAM allocated for every container in a packing plan is less than a given
   * maximum value.
   */
  public static void assertContainerRam(Set<PackingPlan.ContainerPlan> containerPlans,
                                        long maxRamforResources) {
    for (PackingPlan.ContainerPlan containerPlan : containerPlans) {
      Assert.assertTrue(String.format("Container with id: %d requires more RAM (%d) than"
              + " the maximum RAM allowed (%d)", containerPlan.getId(),
          containerPlan.getRequiredResource().getRam(), maxRamforResources),
          containerPlan.getRequiredResource().getRam() <= maxRamforResources);
    }
  }
}
