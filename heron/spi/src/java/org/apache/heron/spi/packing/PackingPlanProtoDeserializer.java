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

import java.util.HashSet;
import java.util.Set;

import org.apache.heron.common.basics.ByteAmount;
import org.apache.heron.proto.system.PackingPlans;

/**
 * Converts to a org.apache.heron.spi.packing.PackingPlan object it's protobuf equivalent
 */
public class PackingPlanProtoDeserializer {

  public PackingPlan fromProto(PackingPlans.PackingPlan packingPlan) {
    Set<PackingPlan.ContainerPlan> containers = new HashSet<>();
    for (PackingPlans.ContainerPlan containerPlan : packingPlan.getContainerPlansList()) {
      containers.add(convert(containerPlan));
    }

    return new PackingPlan(packingPlan.getId(), containers);
  }

  private PackingPlan.ContainerPlan convert(PackingPlans.ContainerPlan containerPlan) {
    Set<PackingPlan.InstancePlan> instances = new HashSet<>();
    for (PackingPlans.InstancePlan instancePlan : containerPlan.getInstancePlansList()) {
      instances.add(convert(instancePlan));
    }

    return new PackingPlan.ContainerPlan(
        containerPlan.getId(),
        instances,
        convert(containerPlan.getRequiredResource()),
        convert(containerPlan.getScheduledResource()));
  }

  private PackingPlan.InstancePlan convert(PackingPlans.InstancePlan instancePlan) {
    return new PackingPlan.InstancePlan(
        new InstanceId(
            instancePlan.getComponentName(),
            instancePlan.getTaskId(),
            instancePlan.getComponentIndex()),
        convert(instancePlan.getResource()));
  }

  private Resource convert(PackingPlans.Resource resource) {
    Resource result = null;
    if (resource != null && resource.isInitialized()) {
      result = new Resource(resource.getCpu(),
          ByteAmount.fromBytes(resource.getRam()),
          ByteAmount.fromBytes(resource.getDisk()));
    }
    return result;
  }
}
