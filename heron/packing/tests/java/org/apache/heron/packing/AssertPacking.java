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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.heron.common.basics.ByteAmount;
import org.apache.heron.common.basics.Pair;
import org.apache.heron.spi.packing.InstanceId;
import org.apache.heron.spi.packing.PackingPlan;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Utility methods for common test assertions related to packing
 */
public final class AssertPacking {

  public static final double DELTA = 0.1;

  private AssertPacking() { }

  /**
   * Verifies that the containerPlan has at least one bolt named boltName with RAM equal to
   * expectedBoltRam and likewise for spouts. If notExpectedContainerRam is not null, verifies that
   * the container RAM is not that.
   */
  public static void assertInstanceRam(Set<PackingPlan.ContainerPlan> containerPlans,
                                      String boltName, String spoutName,
                                      ByteAmount expectedBoltRam, ByteAmount expectedSpoutRam) {
    // RAM for bolt should be the value in component RAM map
    for (PackingPlan.ContainerPlan containerPlan : containerPlans) {
      for (PackingPlan.InstancePlan instancePlan : containerPlan.getInstances()) {
        if (instancePlan.getComponentName().equals(boltName)) {
          assertEquals("Unexpected bolt RAM",
              expectedBoltRam, instancePlan.getResource().getRam());
        }
        if (instancePlan.getComponentName().equals(spoutName)) {
          assertEquals("Unexpected spout RAM",
              expectedSpoutRam, instancePlan.getResource().getRam());
        }
      }
    }
  }

  /**
   * Verifies that the containerPlan has at least one bolt named boltName with CPU equal to
   * expectedBoltCpu and likewise for spouts. If notExpectedContainerCpu is not null, verifies that
   * the container CPU is not that.
   */
  public static void assertInstanceCpu(Set<PackingPlan.ContainerPlan> containerPlans,
                                       String boltName, String spoutName,
                                       Double expectedBoltCpu, Double expectedSpoutCpu) {
    // CPU for bolt should be the value in component CPU map
    for (PackingPlan.ContainerPlan containerPlan : containerPlans) {
      for (PackingPlan.InstancePlan instancePlan : containerPlan.getInstances()) {
        if (instancePlan.getComponentName().equals(boltName)) {
          assertEquals("Unexpected bolt CPU",
              expectedBoltCpu, instancePlan.getResource().getCpu(), DELTA);
        }
        if (instancePlan.getComponentName().equals(spoutName)) {
          assertEquals("Unexpected spout CPU",
              expectedSpoutCpu, instancePlan.getResource().getCpu(), DELTA);
        }
      }
    }
  }

  public static void assertInstanceIndices(Set<PackingPlan.ContainerPlan> containerPlans,
                                           String boltName, String spoutName) {
    List<Integer> expectedInstanceIndices = new ArrayList<>();
    List<Integer> foundInstanceIndices = new ArrayList<>();
    int expectedInstanceIndex = 1;
    // CPU for bolt should be the value in component CPU map
    for (PackingPlan.ContainerPlan containerPlan : containerPlans) {
      for (PackingPlan.InstancePlan instancePlan : containerPlan.getInstances()) {
        expectedInstanceIndices.add(expectedInstanceIndex++);
        foundInstanceIndices.add(instancePlan.getTaskId());
      }
    }

    Collections.sort(foundInstanceIndices);
    assertEquals("Unexpected instance global id set found.",
        expectedInstanceIndices, foundInstanceIndices);
  }

  /**
   * Verifies that the containerPlan contains a specific number of instances for the given component.
   */
  public static void assertNumInstances(Set<PackingPlan.ContainerPlan> containerPlans,
                                        String component, int numInstances) {
    int instancesFound = (int) containerPlans.stream()
        .flatMap(containerPlan -> containerPlan.getInstances().stream())
        .filter(instancePlan -> instancePlan.getComponentName().equals(component))
        .count();
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

  /**
   * Verifies that the CPU allocated for every container in a packing plan is less than a given
   * maximum value.
   */
  public static void assertContainerCpu(Set<PackingPlan.ContainerPlan> containerPlans,
                                        double maxCpuforResources) {
    for (PackingPlan.ContainerPlan containerPlan : containerPlans) {
      assertTrue(String.format("Container with id %d requires more CPU (%.3f) than"
              + " the maximum CPU allowed (%.3f)", containerPlan.getId(),
          containerPlan.getRequiredResource().getCpu(), maxCpuforResources),
          containerPlan.getRequiredResource().getCpu() <= maxCpuforResources);
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
        componentInstances.computeIfAbsent(instancePlan.getComponentName(), k -> new HashSet<>());
        componentInstances.get(instancePlan.getComponentName()).add(instancePlan);
      }
    }
    assertEquals(expectedContainerIds.size(), plan.getContainers().size());
  }
}
