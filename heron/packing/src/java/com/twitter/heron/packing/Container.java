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

import java.util.HashSet;

import com.google.common.base.Optional;

import com.twitter.heron.spi.packing.PackingPlan;
import com.twitter.heron.spi.packing.Resource;

/**
 * Class that describes a container used to place Heron Instances with specific memory, Cpu and disk
 * requirements. Each container has limited ram, CpuCores and disk resources.
 */
public class Container {

  private HashSet<PackingPlan.InstancePlan> instances;

  private Resource capacity;

  /**
   * Creates a container with a specific capacity
   * @param capacity the capacity of the container in terms of cpu, ram and disk
   */
  public Container(Resource capacity) {
    this.capacity = capacity;
    this.instances = new HashSet<PackingPlan.InstancePlan>();
  }

  /**
   * Check whether the container can accommodate a new instance with specific resource requirements
   *
   * @return true if the container has space otherwise return false
   */
  private boolean hasSpace(Resource resource) {
    Resource usedResources = this.getTotalUsedResources();
    return usedResources.getRam() + resource.getRam() <= this.capacity.getRam()
        && usedResources.getCpu() + resource.getCpu() <= this.capacity.getCpu()
        && usedResources.getDisk() + resource.getDisk() <= this.capacity.getDisk();
  }

  /**
   * Computes the used resources of the container by taking into account the resources
   * allocated for each instance.
   *
   * @return a Resource object that describes the used cpu, ram and disk in the container.
   */
  private Resource getTotalUsedResources() {
    long usedRam = 0;
    double usedCpuCores = 0;
    long usedDisk = 0;
    for (PackingPlan.InstancePlan instancePlan : this.instances) {
      Resource resource = instancePlan.getResource();
      usedRam += resource.getRam();
      usedCpuCores += resource.getCpu();
      usedDisk += resource.getDisk();
    }
    return new Resource(usedCpuCores, usedRam, usedDisk);
  }

  /**
   * Update the resources currently used by the container, when a new instance with specific
   * resource requirements has been assigned to the container.
   *
   * @return true if the instance can be added to the container, false otherwise
   */
  public boolean add(PackingPlan.InstancePlan instancePlan) {
    if (this.hasSpace(instancePlan.getResource())) {
      this.instances.add(instancePlan);
      return true;
    } else {
      return false;
    }
  }

  /**
   * Remove an instance of a particular component from a container and update its
   * corresponding resources.
   *
   * @return the corresponding instance plan if the instance is removed the container.
   * Return void if an instance is not found
   */
  public Optional<PackingPlan.InstancePlan> removeAnyInstanceOfComponent(String component) {
    Optional<PackingPlan.InstancePlan> instancePlan = getAnyInstanceOfComponent(component);
    if (instancePlan.isPresent()) {
      PackingPlan.InstancePlan plan = instancePlan.get();
      this.instances.remove(plan);
      return instancePlan;
    }
    return Optional.absent();
  }

  /**
   * Find whether any instance of a particular component is assigned to the container
   *
   * @return the instancePlan that corresponds to the instance if it is found, void otherwise
   */
  public Optional<PackingPlan.InstancePlan> getAnyInstanceOfComponent(String component) {
    for (PackingPlan.InstancePlan instancePlan : this.instances) {
      if (instancePlan.getComponentName().equals(component)) {
        return Optional.of(instancePlan);
      }
    }
    return Optional.absent();
  }
}
