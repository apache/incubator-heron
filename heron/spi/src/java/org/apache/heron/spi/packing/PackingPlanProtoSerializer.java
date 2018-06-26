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

import org.apache.heron.proto.system.PackingPlans;

/**
 * Converts org.apache.heron.spi.packing.PackingPlan objects into their protobuf equivalent
 */
public class PackingPlanProtoSerializer {

  public PackingPlans.PackingPlan toProto(PackingPlan packingPlan) {
    PackingPlans.PackingPlan.Builder builder = PackingPlans.PackingPlan.newBuilder()
        .setId(packingPlan.getId());

    for (PackingPlan.ContainerPlan containerPlan : packingPlan.getContainers()) {
      builder.addContainerPlans(builder(containerPlan));
    }

    return builder.build();
  }

  private PackingPlans.ContainerPlan.Builder builder(PackingPlan.ContainerPlan containerPlan) {
    PackingPlans.ContainerPlan.Builder builder = PackingPlans.ContainerPlan.newBuilder()
        .setId(containerPlan.getId())
        .setRequiredResource(builder(containerPlan.getRequiredResource()));

    if (containerPlan.getScheduledResource().isPresent()) {
      builder.setScheduledResource(builder(containerPlan.getScheduledResource().get()));
    }

    for (PackingPlan.InstancePlan instancePlan : containerPlan.getInstances()) {
      builder.addInstancePlans(builder(instancePlan));
    }

    return builder;
  }

  private PackingPlans.InstancePlan.Builder builder(PackingPlan.InstancePlan instancePlan) {
    return PackingPlans.InstancePlan.newBuilder()
        .setComponentName(instancePlan.getComponentName())
        .setTaskId(instancePlan.getTaskId())
        .setComponentIndex(instancePlan.getComponentIndex())
        .setResource(builder(instancePlan.getResource()));
  }

  private PackingPlans.Resource.Builder builder(Resource resource) {
    return PackingPlans.Resource.newBuilder()
        .setCpu(resource.getCpu())
        .setRam(resource.getRam().asBytes())
        .setDisk(resource.getDisk().asBytes());
  }
}
