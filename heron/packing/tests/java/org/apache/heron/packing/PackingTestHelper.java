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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.heron.common.basics.ByteAmount;
import org.apache.heron.common.basics.Pair;
import org.apache.heron.packing.builder.PackingPlanBuilder;
import org.apache.heron.packing.constraints.MinRamConstraint;
import org.apache.heron.packing.constraints.ResourceConstraint;
import org.apache.heron.packing.exceptions.ConstraintViolationException;
import org.apache.heron.packing.utils.PackingUtils;
import org.apache.heron.spi.packing.InstanceId;
import org.apache.heron.spi.packing.PackingPlan;
import org.apache.heron.spi.packing.Resource;

/**
 * Utility methods for common test methods related to packing
 */
public final class PackingTestHelper {

  private PackingTestHelper() { }

  public static PackingPlan createTestPackingPlan(String topologyName,
                                                  Pair<Integer, String>[] instances,
                                                  int containerPadding)
      throws ConstraintViolationException {
    return generateTestPackingPlan(topologyName, null, instances, null, containerPadding);
  }

  public static PackingPlan addToTestPackingPlan(String topologyName,
                                                 PackingPlan previousPackingPlan,
                                                 Pair<Integer, String>[] instances,
                                                 int containerPadding)
      throws ConstraintViolationException {
    return generateTestPackingPlan(
        topologyName, previousPackingPlan, instances, null, containerPadding);
  }

  public static PackingPlan removeFromTestPackingPlan(String topologyName,
                                                      PackingPlan previousPackingPlan,
                                                      Pair<Integer, String>[] instances,
                                                      int containerPadding)
      throws ConstraintViolationException {
    return generateTestPackingPlan(
        topologyName, previousPackingPlan, null, instances, containerPadding);
  }

  /**
   * Returns a PackingPlan to use for testing scale up/down that has instances of specific
   * components on specific containers.
   */
  private static PackingPlan generateTestPackingPlan(String topologyName,
                                                     PackingPlan previousPackingPlan,
                                                     Pair<Integer, String>[] addInstances,
                                                     Pair<Integer, String>[] removeInstances,
                                                     int containerPadding)
      throws ConstraintViolationException {
    PackingPlanBuilder builder = new PackingPlanBuilder(topologyName, previousPackingPlan);

    int instanceCount = 0;
    if (previousPackingPlan != null) {
      instanceCount = previousPackingPlan.getInstanceCount();
    } else if (addInstances != null) {
      instanceCount = addInstances.length;
    }

    // use basic default resource to allow all instances to fit on a single container, if that's
    // what the tester desired. We can extend this to permit passing custom resource requirements
    // as needed.
    Resource defaultInstanceResource = new Resource(1,
        ByteAmount.fromMegabytes(192), ByteAmount.fromMegabytes(1));

    Map<String, Resource> componentResourceMap = PackingUtils.getComponentResourceMap(
        toParallelismMap(previousPackingPlan, addInstances, removeInstances).keySet(),
        new HashMap<>(), new HashMap<>(), new HashMap<>(), defaultInstanceResource);

    builder.setDefaultInstanceResource(defaultInstanceResource);
    builder.setRequestedComponentResource(componentResourceMap);
    builder.setMaxContainerResource(new Resource(
        instanceCount,
        ByteAmount.fromMegabytes(192).multiply(instanceCount),
        ByteAmount.fromMegabytes(instanceCount)));
    builder.setInstanceConstraints(Collections.singletonList(new MinRamConstraint()));
    builder.setPackingConstraints(Collections.singletonList(new ResourceConstraint()));

    if (addInstances != null) {
      for (Pair<Integer, String> componentInstance : addInstances) {
        builder.addInstance(componentInstance.first, componentInstance.second);
      }
    }

    if (removeInstances != null) {
      for (Pair<Integer, String> componentInstance : removeInstances) {
        builder.removeInstance(componentInstance.first, componentInstance.second);
      }
    }
    return builder.build();
  }

  public static Pair<Integer, String>[] toContainerIdComponentNames(
      Pair<Integer, InstanceId>[] containerIdInstanceIds) {

    @SuppressWarnings({"unchecked", "rawtypes"})
    Pair<Integer, String>[] containerIdComponentNames = new Pair[containerIdInstanceIds.length];
    int i = 0;
    for (Pair<Integer, InstanceId> containerIdInstanceId : containerIdInstanceIds) {
      containerIdComponentNames[i++] = new Pair<>(
          containerIdInstanceId.first, containerIdInstanceId.second.getComponentName());
    }
    return containerIdComponentNames;
  }

  public static Map<String, Integer> toParallelismMap(
      PackingPlan packingPlan,
      Pair<Integer, String>[] containerIdInstanceIdsToAdd,
      Pair<Integer, String>[] containerIdInstanceIdsToRemove) {
    Map<String, Integer> parallelismMap = new HashMap<>();
    if (packingPlan != null) {
      parallelismMap = new HashMap<>(packingPlan.getComponentCounts());
    }
    if (containerIdInstanceIdsToAdd != null) {
      for (Pair<Integer, String> containerIdInstanceId : containerIdInstanceIdsToAdd) {
        String componentName = containerIdInstanceId.second;
        parallelismMap.put(componentName, parallelismMap.getOrDefault(componentName, 0) + 1);
      }
    }
    if (containerIdInstanceIdsToRemove != null) {
      for (Pair<Integer, String> containerIdInstanceId : containerIdInstanceIdsToRemove) {
        String componentName = containerIdInstanceId.second;
        if (parallelismMap.containsKey(componentName)) {
          parallelismMap.put(componentName, parallelismMap.get(componentName) - 1);
        }
      }
    }

    return parallelismMap;
  }
}
