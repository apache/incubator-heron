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

import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;

import org.apache.heron.common.basics.ByteAmount;

public class PackingPlan {
  private final String id;
  private final Map<Integer, ContainerPlan> containersMap;
  private final Set<ContainerPlan> containers;

  public PackingPlan(String id, Set<ContainerPlan> containers) {
    this.id = id;
    this.containers = ImmutableSet.copyOf(containers);
    containersMap = new HashMap<>();
    for (ContainerPlan containerPlan : containers) {
      containersMap.put(containerPlan.getId(), containerPlan);
    }
  }

  /**
   * Computes the maximum of all the resources required by the containers in the packing plan. If
   * the PackingPlan has already been scheduled, the scheduled resources will be used over the
   * required resources.
   *
   * @return maximum Resources found in all containers.
   */
  public Resource getMaxContainerResources() {
    double maxCpu = 0;
    ByteAmount maxRam = ByteAmount.ZERO;
    ByteAmount maxDisk = ByteAmount.ZERO;
    for (ContainerPlan containerPlan : getContainers()) {
      Resource containerResource =
          containerPlan.getScheduledResource().or(containerPlan.getRequiredResource());
      maxCpu = Math.max(maxCpu, containerResource.getCpu());
      maxRam = maxRam.max(containerResource.getRam());
      maxDisk = maxDisk.max(containerResource.getDisk());
    }

    return new Resource(maxCpu, maxRam, maxDisk);
  }

  /**
   * Creates a clone of {@link PackingPlan}. It also computes the maximum of all the resources
   * required by containers in the packing plan and updates the containers of the clone with the
   * max resource information
   */
  public PackingPlan cloneWithHomogeneousScheduledResource() {
    Resource maxResource = getMaxContainerResources();
    Set<ContainerPlan> updatedContainers = new LinkedHashSet<>();
    for (ContainerPlan container : getContainers()) {
      updatedContainers.add(container.cloneWithScheduledResource(maxResource));
    }

    return new PackingPlan(getId(), updatedContainers);
  }

  public String getId() {
    return id;
  }

  public Set<ContainerPlan> getContainers() {
    return containers;
  }

  public Map<Integer, ContainerPlan> getContainersMap() {
    return containersMap;
  }

  public Optional<ContainerPlan> getContainer(int containerId) {
    return Optional.fromNullable(this.containersMap.get(containerId));
  }

  public Integer getInstanceCount() {
    Integer totalInstances = 0;
    for (Integer count : getComponentCounts().values()) {
      totalInstances += count;
    }
    return totalInstances;
  }

  /**
   * Return a map containing the count of all of the components, keyed by name
   */
  public Map<String, Integer> getComponentCounts() {
    Map<String, Integer> componentCounts = new HashMap<>();
    for (ContainerPlan containerPlan : getContainers()) {
      for (InstancePlan instancePlan : containerPlan.getInstances()) {
        Integer count = 0;
        if (componentCounts.containsKey(instancePlan.getComponentName())) {
          count = componentCounts.get(instancePlan.getComponentName());
        }
        componentCounts.put(instancePlan.getComponentName(), ++count);
      }
    }
    return componentCounts;
  }

  /**
   * Get the formatted String describing component RAM distribution from PackingPlan,
   * used by executor
   *
   * @return String describing component RAM distribution
   */
  public String getComponentRamDistribution() {
    // Generate a map with the minimal RAM size for each component
    Map<String, ByteAmount> ramMap = new HashMap<>();
    for (ContainerPlan containerPlan : this.getContainers()) {
      for (InstancePlan instancePlan : containerPlan.getInstances()) {
        ByteAmount newRam = instancePlan.getResource().getRam();
        ByteAmount currentRam = ramMap.get(instancePlan.getComponentName());
        if (currentRam == null || currentRam.asBytes() > newRam.asBytes()) {
          ramMap.put(instancePlan.getComponentName(), newRam);
        }
      }
    }

    // Convert it into a formatted String
    StringBuilder ramMapBuilder = new StringBuilder();
    for (String component : ramMap.keySet()) {
      ramMapBuilder.append(String.format("%s:%d,", component, ramMap.get(component).asBytes()));
    }

    // Remove the duplicated "," at the end
    ramMapBuilder.deleteCharAt(ramMapBuilder.length() - 1);
    return ramMapBuilder.toString();
  }

  @Override
  public String toString() {
    return String.format("{plan-id: %s, containers-list: %s}",
        getId(), getContainers().toString());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    PackingPlan that = (PackingPlan) o;

    return getId().equals(that.getId())
        && getContainers().equals(that.getContainers());
  }

  @Override
  public int hashCode() {
    int result = getId().hashCode();
    result = 31 * result + getContainers().hashCode();
    return result;
  }

  public static class InstancePlan {
    private final String componentName;
    private final int taskId;
    private final int componentIndex;
    private final Resource resource;

    public InstancePlan(InstanceId instanceId, Resource resource) {
      this.componentName = instanceId.getComponentName();
      this.taskId = instanceId.getTaskId();
      this.componentIndex = instanceId.getComponentIndex();
      this.resource = resource;
    }

    public String getComponentName() {
      return componentName;
    }

    public int getTaskId() {
      return taskId;
    }

    public int getComponentIndex() {
      return componentIndex;
    }

    public Resource getResource() {
      return resource;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      InstancePlan that = (InstancePlan) o;

      return getComponentName().equals(that.getComponentName())
          && getTaskId() == that.getTaskId()
          && getComponentIndex() == that.getComponentIndex()
          && getResource().equals(that.getResource());
    }

    @Override
    public int hashCode() {
      int result = getComponentName().hashCode();
      result = 31 * result + ((Integer) getTaskId()).hashCode();
      result = 31 * result + ((Integer) getComponentIndex()).hashCode();
      result = 31 * result + getResource().hashCode();
      return result;
    }

    @Override
    public String toString() {
      return String.format(
          "{component-name: %s, task-id: %s, component-index: %s, instance-resource: %s}",
          getComponentName(), getTaskId(), getComponentIndex(), getResource().toString());
    }
  }

  public static class ContainerPlan {
    private final int id;
    private final Set<InstancePlan> instances;
    private final Resource requiredResource;
    private final Optional<Resource> scheduledResource;

    public ContainerPlan(int id, Set<InstancePlan> instances, Resource requiredResource) {
      this(id, instances, requiredResource, null);
    }

    public ContainerPlan(int id,
                         Set<InstancePlan> instances,
                         Resource requiredResource,
                         Resource scheduledResource) {
      this.id = id;
      this.instances = ImmutableSet.copyOf(instances);
      this.requiredResource = requiredResource;
      this.scheduledResource = Optional.fromNullable(scheduledResource);
    }

    public int getId() {
      return id;
    }

    public Set<InstancePlan> getInstances() {
      return instances;
    }

    public Resource getRequiredResource() {
      return requiredResource;
    }

    public Optional<Resource> getScheduledResource() {
      return scheduledResource;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      ContainerPlan that = (ContainerPlan) o;

      return id == that.id
          && getInstances().equals(that.getInstances())
          && getRequiredResource().equals(that.getRequiredResource())
          && getScheduledResource().equals(that.getScheduledResource());
    }

    @Override
    public int hashCode() {
      int result = id;
      result = 31 * result + getInstances().hashCode();
      result = 31 * result + getRequiredResource().hashCode();
      if (scheduledResource.isPresent()) {
        result = 31 * result + getScheduledResource().get().hashCode();
      }
      return result;
    }

    /**
     * Returns a {@link ContainerPlan} with updated scheduledResource
     */
    private ContainerPlan cloneWithScheduledResource(Resource resource) {
      return new ContainerPlan(getId(), getInstances(), getRequiredResource(), resource);
    }

    @Override
    public String toString() {
      String str = String.format("{container-id: %s, instances-list: %s, required-resource: %s",
          id, getInstances().toString(), getRequiredResource());
      if (scheduledResource.isPresent()) {
        str = String.format("%s, scheduled-resource: %s", str, getScheduledResource().get());
      }

      return str + "}";
    }
  }
}
