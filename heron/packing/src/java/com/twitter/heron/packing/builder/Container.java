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
package com.twitter.heron.packing.builder;

import java.util.HashSet;

import com.google.common.base.Optional;

import com.twitter.heron.common.basics.ByteAmount;
import com.twitter.heron.packing.ResourceExceededException;
import com.twitter.heron.packing.utils.PackingUtils;
import com.twitter.heron.spi.packing.PackingException;
import com.twitter.heron.spi.packing.PackingPlan;
import com.twitter.heron.spi.packing.Resource;

/**
 * Class that describes a container used to place Heron Instances with specific memory, Cpu and disk
 * requirements. Each container has limited ram, CpuCores and disk resources.
 */
public class Container {

  private int containerId;
  private HashSet<PackingPlan.InstancePlan> instances;
  private Resource capacity;
  private int paddingPercentage;

  /**
   * Creates a container with a specific capacity which will maintain a specific percentage
   * of its resources for padding.
   *
   * @param capacity the capacity of the container in terms of cpu, ram and disk
   * @param paddingPercentage the padding percentage
   */
  Container(int containerId, Resource capacity, int paddingPercentage) {
    this.containerId = containerId;
    this.capacity = capacity;
    this.instances = new HashSet<PackingPlan.InstancePlan>();
    this.paddingPercentage = paddingPercentage;
  }

  public int getContainerId() {
    return containerId;
  }

  public HashSet<PackingPlan.InstancePlan> getInstances() {
    return instances;
  }

  public Resource getCapacity() {
    return capacity;
  }

  int getPaddingPercentage() {
    return paddingPercentage;
  }

  /**
   * Update the resources currently used by the container, when a new instance with specific
   * resource requirements has been assigned to the container.
   */
  void add(PackingPlan.InstancePlan instancePlan) throws ResourceExceededException {
    if (this.instances.contains(instancePlan)) {
      throw new PackingException(String.format(
          "Instance %s already exists in container %s", instancePlan, toString()));
    }
    assertHasSpace(instancePlan.getResource());
    this.instances.add(instancePlan);
  }

  /**
   * Remove an instance of a particular component from a container and update its
   * corresponding resources.
   *
   * @return the corresponding instance plan if the instance is removed the container.
   * Return void if an instance is not found
   */
  Optional<PackingPlan.InstancePlan> removeAnyInstanceOfComponent(String component) {
    Optional<PackingPlan.InstancePlan> instancePlan = getAnyInstanceOfComponent(component);
    if (instancePlan.isPresent()) {
      PackingPlan.InstancePlan plan = instancePlan.get();
      this.instances.remove(plan);
      return instancePlan;
    }
    return Optional.absent();
  }

  @Override
  public String toString() {
    return String.format("{containerId=%s, instances=%s, capacity=%s, paddingPercentage=%s}",
        containerId, instances, capacity, paddingPercentage);
  }

  /**
   * Find whether any instance of a particular component is assigned to the container
   *
   * @return an optional including the InstancePlan if found
   */
  private Optional<PackingPlan.InstancePlan> getAnyInstanceOfComponent(String componentName) {
    for (PackingPlan.InstancePlan instancePlan : this.instances) {
      if (instancePlan.getComponentName().equals(componentName)) {
        return Optional.of(instancePlan);
      }
    }
    return Optional.absent();
  }

  /**
   * Return the instance of componentName with a matching componentIndex if it exists
   *
   * @return an optional including the InstancePlan if found
   */
  Optional<PackingPlan.InstancePlan> getInstance(String componentName, int componentIndex) {
    for (PackingPlan.InstancePlan instancePlan : this.instances) {
      if (instancePlan.getComponentName().equals(componentName)
          && instancePlan.getComponentIndex() == componentIndex) {
        return Optional.of(instancePlan);
      }
    }
    return Optional.absent();
  }

  /**
   * Return the instance of with a given taskId if it exists
   *
   * @return an optional including the InstancePlan if found
   */
  Optional<PackingPlan.InstancePlan> getInstance(int taskId) {
    for (PackingPlan.InstancePlan instancePlan : this.instances) {
      if (instancePlan.getTaskId() == taskId) {
        return Optional.of(instancePlan);
      }
    }
    return Optional.absent();
  }

  /**
   * Check whether the container can accommodate a new instance with specific resource requirements
   */
  private void assertHasSpace(Resource resource) throws ResourceExceededException {
    Resource usedResources = this.getTotalUsedResources();
    ByteAmount newRam =
        usedResources.getRam().plus(resource.getRam()).increaseBy(paddingPercentage);
    double newCpu = Math.round(
        PackingUtils.increaseBy(usedResources.getCpu() + resource.getCpu(), paddingPercentage));
    ByteAmount newDisk =
        usedResources.getDisk().plus(resource.getDisk()).increaseBy(paddingPercentage);

    if (newRam.greaterThan(this.capacity.getRam())) {
      throw new ResourceExceededException(String.format("Adding %s bytes of ram to existing %s "
          + "bytes with %d percent padding would exceed capacity %s",
          resource.getRam(), usedResources.getRam(), paddingPercentage, this.capacity.getRam()));
    }
    if (newCpu > this.capacity.getCpu()) {
      throw new ResourceExceededException(String.format("Adding %s cores to existing %s "
          + "cores with %d percent padding would exceed capacity %s",
          resource.getCpu(), usedResources.getCpu(), paddingPercentage, this.capacity.getCpu()));
    }
    if (newDisk.greaterThan(this.capacity.getDisk())) {
      throw new ResourceExceededException(String.format("Adding %s bytes of disk to existing %s "
          + "bytes with %s percent padding would exceed capacity %s",
          resource.getDisk(), usedResources.getDisk(), paddingPercentage, this.capacity.getDisk()));
    }
  }

  /**
   * Computes the used resources of the container by taking into account the resources
   * allocated for each instance.
   *
   * @return a Resource object that describes the used cpu, ram and disk in the container.
   */
  private Resource getTotalUsedResources() {
    ByteAmount usedRam = ByteAmount.ZERO;
    double usedCpuCores = 0;
    ByteAmount usedDisk = ByteAmount.ZERO;
    for (PackingPlan.InstancePlan instancePlan : this.instances) {
      Resource resource = instancePlan.getResource();
      usedRam = usedRam.plus(resource.getRam());
      usedCpuCores += resource.getCpu();
      usedDisk = usedDisk.plus(resource.getDisk());
    }
    return new Resource(usedCpuCores, usedRam, usedDisk);
  }
}
