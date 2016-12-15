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

import com.twitter.heron.common.basics.ByteAmount;
import com.twitter.heron.common.basics.Pair;
import com.twitter.heron.packing.builder.PackingPlanBuilder;
import com.twitter.heron.spi.packing.InstanceId;
import com.twitter.heron.spi.packing.PackingPlan;
import com.twitter.heron.spi.packing.Resource;

/**
 * Utility methods for common test methods related to packing
 */
public final class PackingTestHelper {

  private PackingTestHelper() { }

  public static PackingPlan createTestPackingPlan(String topologyName,
                                                  Pair<Integer, String>[] instances,
                                                  int containerPadding)
      throws ResourceExceededException {
    return generateTestPackingPlan(topologyName, null, instances, null, containerPadding);
  }

  public static PackingPlan addToTestPackingPlan(String topologyName,
                                                 PackingPlan previousPackingPlan,
                                                 Pair<Integer, String>[] instances,
                                                 int containerPadding)
      throws ResourceExceededException {
    return generateTestPackingPlan(
        topologyName, previousPackingPlan, instances, null, containerPadding);
  }

  public static PackingPlan removeFromTestPackingPlan(String topologyName,
                                                      PackingPlan previousPackingPlan,
                                                      Pair<Integer, String>[] instances,
                                                      int containerPadding)
      throws ResourceExceededException {
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
      throws ResourceExceededException {
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
    builder.setDefaultInstanceResource(
        new Resource(1, ByteAmount.fromMegabytes(192), ByteAmount.fromMegabytes(1)));
    builder.setMaxContainerResource(new Resource(
        instanceCount,
        ByteAmount.fromMegabytes(192).multiply(instanceCount),
        ByteAmount.fromMegabytes(instanceCount)));

    // This setting is important, see https://github.com/twitter/heron/issues/1577
    builder.setRequestedContainerPadding(containerPadding);

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
}
