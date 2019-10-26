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

package org.apache.heron.packing.builder;

import java.util.HashSet;

import com.google.common.base.Optional;

import org.apache.heron.spi.packing.PackingException;
import org.apache.heron.spi.packing.PackingPlan;
import org.apache.heron.spi.packing.Resource;

/**
 * Class that describes a container used to place Heron Instances with specific memory, CPU and disk
 * requirements. Each container has limited RAM, CpuCores and disk resources.
 */
public class Container {

  private int containerId;
  private HashSet<PackingPlan.InstancePlan> instances;
  private Resource capacity;
  private Resource padding;

  /**
   * Creates a container with a specific capacity which will maintain a specific percentage
   * of its resources for padding.
   *
   * @param capacity the capacity of the container in terms of CPU, RAM and disk
   * @param padding the padding
   */
  Container(int containerId, Resource capacity, Resource padding) {
    this.containerId = containerId;
    this.capacity = capacity;
    this.instances = new HashSet<PackingPlan.InstancePlan>();
    this.padding = padding;
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

  public Resource getPadding() {
    return padding;
  }

  /**
   * Update the resources currently used by the container, when a new instance with specific
   * resource requirements has been assigned to the container.
   */
  void add(PackingPlan.InstancePlan instancePlan) {
    if (this.instances.contains(instancePlan)) {
      throw new PackingException(String.format(
          "Instance %s already exists in container %s", instancePlan, toString()));
    }
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
    return String.format("{containerId=%s, instances=%s, capacity=%s, padding=%s}",
        containerId, instances, capacity, padding);
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
   * Computes the used resources of the container by taking into account the resources
   * allocated for each instance.
   *
   * @return a Resource object that describes the used CPU, RAM and disk in the container.
   */
  public Resource getTotalUsedResources() {
    return getInstances().stream()
        .map(PackingPlan.InstancePlan::getResource)
        .reduce(Resource.EMPTY_RESOURCE, Resource::plus)
        .plus(getPadding());
  }
}
