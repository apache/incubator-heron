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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.twitter.heron.common.basics.ByteAmount;
import com.twitter.heron.common.basics.Pair;
import com.twitter.heron.spi.packing.InstanceId;
import com.twitter.heron.spi.packing.PackingPlan;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Utility methods for common test assertions related to packing
 */
public final class AssertPacking {

  private AssertPacking() { }

  /**
   * Verifies that the containerPlan has at least one bolt named boltName with ram equal to
   * expectedBoltRam and likewise for spouts. If notExpectedContainerRam is not null, verifies that
   * the container ram is not that.
   */
  public static void assertContainers(Set<PackingPlan.ContainerPlan> containerPlans,
                                      String boltName, String spoutName,
                                      ByteAmount expectedBoltRam, ByteAmount expectedSpoutRam,
                                      ByteAmount notExpectedContainerRam) {
    boolean boltFound = false;
    boolean spoutFound = false;
    List<Integer> expectedInstanceIndecies = new ArrayList<>();
    List<Integer> foundInstanceIndecies = new ArrayList<>();
    int expectedInstanceIndex = 1;
    // Ram for bolt should be the value in component ram map
    for (PackingPlan.ContainerPlan containerPlan : containerPlans) {
      if (notExpectedContainerRam != null) {
        assertNotEquals(
            notExpectedContainerRam, containerPlan.getRequiredResource().getRam());
      }
      for (PackingPlan.InstancePlan instancePlan : containerPlan.getInstances()) {
        expectedInstanceIndecies.add(expectedInstanceIndex++);
        foundInstanceIndecies.add(instancePlan.getTaskId());
        if (instancePlan.getComponentName().equals(boltName)) {
          assertEquals("Unexpected bolt ram", expectedBoltRam, instancePlan.getResource().getRam());
          boltFound = true;
        }
        if (instancePlan.getComponentName().equals(spoutName)) {
          assertEquals(
              "Unexpected spout ram", expectedSpoutRam, instancePlan.getResource().getRam());
          spoutFound = true;
        }
      }
    }
    assertTrue("Bolt not found in any of the container plans: " + boltName, boltFound);
    assertTrue("Spout not found in any of the container plans: " + spoutName, spoutFound);

    Collections.sort(foundInstanceIndecies);
    assertEquals("Unexpected instance global id set found.",
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
    assertEquals(numInstances, instancesFound);
  }

  /**
   * Verifies that the RAM allocated for every container in a packing plan is less than a given
   * maximum value.
   */
  public static void assertContainerRam(Set<PackingPlan.ContainerPlan> containerPlans,
                                        ByteAmount maxRamforResources) {
    for (PackingPlan.ContainerPlan containerPlan : containerPlans) {
      assertTrue(String.format("Container with id %d requires more RAM (%s) than"
              + " the maximum RAM allowed (%s)", containerPlan.getId(),
          containerPlan.getRequiredResource().getRam(), maxRamforResources),
          containerPlan.getRequiredResource().getRam().lessOrEqual(maxRamforResources));
    }
  }

  public static void assertPackingPlan(String expectedTopologyName,
                                       Pair<Integer, InstanceId>[] expectedComponentInstances,
                                       PackingPlan plan) {
    assertEquals(expectedTopologyName, plan.getId());
    assertEquals("Unexpected number of instances: " + plan.getContainers(),
        expectedComponentInstances.length, plan.getInstanceCount().intValue());

    // for every instance on a given container...
    Set<Integer> expectedContainerIds = new HashSet<>();
    for (Pair<Integer, InstanceId> expectedComponentInstance : expectedComponentInstances) {
      // verify the expected container exists
      int containerId = expectedComponentInstance.first;
      InstanceId instanceId = expectedComponentInstance.second;
      assertTrue(String.format("Container with id %s not found", containerId),
          plan.getContainer(containerId).isPresent());
      expectedContainerIds.add(containerId);

      // and that the instance exists on it
      boolean instanceFound = false;
      PackingPlan.ContainerPlan containerPlan = plan.getContainer(containerId).get();
      for (PackingPlan.InstancePlan instancePlan : containerPlan.getInstances()) {
        if (instancePlan.getTaskId() == instanceId.getTaskId()) {
          instanceFound = true;
          assertEquals("Wrong componentName for task " + instancePlan.getTaskId(),
              instanceId.getComponentName(), instancePlan.getComponentName());
          assertEquals("Wrong getComponentIndex for task " + instancePlan.getTaskId(),
              instanceId.getComponentIndex(), instancePlan.getComponentIndex());
          break;
        }
      }
      assertTrue(String.format("Container (%s) did not include expected instance with taskId %d",
          containerPlan, instanceId.getTaskId()), instanceFound);
    }

    Map<Integer, PackingPlan.InstancePlan> taskIds = new HashMap<>();
    Map<String, Set<PackingPlan.InstancePlan>> componentInstances = new HashMap<>();
    for (PackingPlan.ContainerPlan containerPlan : plan.getContainers()) {
      for (PackingPlan.InstancePlan instancePlan : containerPlan.getInstances()) {

        // check for taskId collisions
        PackingPlan.InstancePlan collisionInstance =  taskIds.get(instancePlan.getTaskId());
        assertNull(String.format("Task id collision between instance %s and %s",
            instancePlan, collisionInstance), collisionInstance);
        taskIds.put(instancePlan.getTaskId(), instancePlan);

        // check for componentIndex collisions
        Set<PackingPlan.InstancePlan> instances =
            componentInstances.get(instancePlan.getComponentName());
        if (instances != null) {
          for (PackingPlan.InstancePlan instance : instances) {
            assertTrue(String.format(
                "Component index collision between instance %s and %s", instance, instancePlan),
                instance.getComponentIndex() != instancePlan.getComponentIndex());
          }
        }
        if (componentInstances.get(instancePlan.getComponentName()) == null) {
          componentInstances.put(instancePlan.getComponentName(),
              new HashSet<PackingPlan.InstancePlan>());
        }
        componentInstances.get(instancePlan.getComponentName()).add(instancePlan);
      }
    }
    assertEquals(expectedContainerIds.size(), plan.getContainers().size());
  }
}
