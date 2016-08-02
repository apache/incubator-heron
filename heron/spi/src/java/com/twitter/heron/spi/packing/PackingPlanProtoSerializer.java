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
package com.twitter.heron.spi.packing;

import com.twitter.heron.proto.system.PackingPlans;

/**
 * Converts com.twitter.heron.spi.packing.PackingPlan objects into their protobuf equivalent
 */
public class PackingPlanProtoSerializer {

  public PackingPlans.PackingPlan toProto(PackingPlan packingPlan) {
    PackingPlans.PackingPlan.Builder builder = PackingPlans.PackingPlan.newBuilder()
        .setId(packingPlan.getId())
        .setInstanceDistribution(packingPlan.getInstanceDistribution())
        .setComponentRamDistribution(packingPlan.getComponentRamDistribution())
        .setResource(builder(packingPlan.getResource()));

    for (PackingPlan.ContainerPlan containerPlan : packingPlan.getContainers().values()) {
      builder.addContainerPlans(builder(containerPlan));
    }

    return builder.build();
  }

  private PackingPlans.ContainerPlan.Builder builder(PackingPlan.ContainerPlan containerPlan) {
    PackingPlans.ContainerPlan.Builder builder = PackingPlans.ContainerPlan.newBuilder()
        .setId(containerPlan.id)
        .setResource(builder(containerPlan.resource));

    for (PackingPlan.InstancePlan instancePlan : containerPlan.instances.values()) {
      builder.addInstancePlans(builder(instancePlan));
    }

    return builder;
  }

  private PackingPlans.InstancePlan.Builder builder(PackingPlan.InstancePlan instancePlan) {
    return PackingPlans.InstancePlan.newBuilder()
        .setId(instancePlan.id)
        .setComponentName(instancePlan.componentName)
        .setResource(builder(instancePlan.resource));
  }

  private PackingPlans.Resource.Builder builder(PackingPlan.Resource resource) {
    return PackingPlans.Resource.newBuilder()
        .setCpu(resource.cpu)
        .setRam(resource.ram)
        .setDisk(resource.disk);
  }
}
