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

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;

public class PackingPlan {
  private final String id;
  private final Map<String, ContainerPlan> containersMap;
  private final Set<ContainerPlan> containers;
  private final Resource resource;

  public PackingPlan(String id, Set<ContainerPlan> containers, Resource resource) {
    this.id = id;
    this.containers = ImmutableSet.copyOf(containers);
    this.resource = resource;
    containersMap = new HashMap<>();
    for (ContainerPlan containerPlan : containers) {
      containersMap.put(containerPlan.getId(), containerPlan);
    }
  }

  public String getId() {
    return id;
  }

  public Resource getResource() {
    return resource;
  }

  public Set<ContainerPlan> getContainers() {
    return containers;
  }

  public Optional<ContainerPlan> getContainer(String containerId) {
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
   * Get the String describing instance distribution from PackingPlan, used by executor
   *
   * @return String describing instance distribution
   */
  public String getInstanceDistribution() {
    StringBuilder[] containerBuilder = new StringBuilder[this.getContainers().size()];
    for (PackingPlan.ContainerPlan container : this.getContainers()) {
      int index = Integer.parseInt(container.id);
      containerBuilder[index - 1] = new StringBuilder();

      for (PackingPlan.InstancePlan instance : container.getInstances()) {
        String[] tokens = instance.getId().split(":");
        containerBuilder[index - 1].append(
            String.format("%s:%s:%s:", tokens[1], tokens[2], tokens[3]));
      }
      containerBuilder[index - 1].deleteCharAt(containerBuilder[index - 1].length() - 1);
    }

    StringBuilder packingBuilder = new StringBuilder();
    for (int i = 0; i < containerBuilder.length; ++i) {
      StringBuilder builder = containerBuilder[i];
      packingBuilder.append(String.format("%d:%s,", i + 1, builder.toString()));
    }
    packingBuilder.deleteCharAt(packingBuilder.length() - 1);

    return packingBuilder.toString();
  }

  /**
   * Get the formatted String describing component ram distribution from PackingPlan,
   * used by executor
   *
   * @return String describing component ram distribution
   */
  public String getComponentRamDistribution() {
    Map<String, Long> ramMap = new HashMap<>();
    // The implementation assumes instances for the same component require same ram
    for (ContainerPlan containerPlan : this.getContainers()) {
      for (InstancePlan instancePlan : containerPlan.getInstances()) {
        ramMap.put(instancePlan.getComponentName(), instancePlan.getResource().getRam());
      }
    }

    // Convert it into a formatted String
    StringBuilder ramMapBuilder = new StringBuilder();
    for (String component : ramMap.keySet()) {
      ramMapBuilder.append(String.format("%s:%d,", component, ramMap.get(component)));
    }

    // Remove the duplicated "," at the end
    ramMapBuilder.deleteCharAt(ramMapBuilder.length() - 1);
    return ramMapBuilder.toString();
  }

  @Override
  public String toString() {
    return String.format("{plan-id: %s, containers-list: %s, plan-resource: %s}",
        getId(), getContainers().toString(), getResource());
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
        && getContainers().equals(that.getContainers())
        && getResource().equals(that.getResource());
  }

  @Override
  public int hashCode() {
    int result = getId().hashCode();
    result = 31 * result + getContainers().hashCode();
    result = 31 * result + getResource().hashCode();
    return result;
  }

  public static class InstancePlan {
    private final String id;
    private final String componentName;
    private final Resource resource;

    public InstancePlan(String id, String componentName, Resource resource) {
      this.id = id;
      this.componentName = componentName;
      this.resource = resource;
    }

    public String getId() {
      return id;
    }

    public String getComponentName() {
      return componentName;
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

      return getId().equals(that.getId())
          && getComponentName().equals(that.getComponentName())
          && getResource().equals(that.getResource());
    }

    @Override
    public int hashCode() {
      int result = getId().hashCode();
      result = 31 * result + getComponentName().hashCode();
      result = 31 * result + getResource().hashCode();
      return result;
    }

    @Override
    public String toString() {
      return String.format("{instance-id: %s, componentName: %s, instance-resource: %s}",
          getId(), getComponentName(), getResource().toString());
    }
  }

  public static class ContainerPlan {
    private final String id;
    private final Set<InstancePlan> instances;
    private final Resource resource;

    public ContainerPlan(String id, Set<InstancePlan> instances, Resource resource) {
      this.id = id;
      this.instances = ImmutableSet.copyOf(instances);
      this.resource = resource;
    }

    public String getId() {
      return id;
    }

    public Set<InstancePlan> getInstances() {
      return instances;
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

      ContainerPlan that = (ContainerPlan) o;

      return id.equals(that.id)
          && getInstances().equals(that.getInstances())
          && getResource().equals(that.getResource());
    }

    @Override
    public int hashCode() {
      int result = id.hashCode();
      result = 31 * result + getInstances().hashCode();
      result = 31 * result + getResource().hashCode();
      return result;
    }

    @Override
    public String toString() {
      return String.format("{container-id: %s, instances-list: %s, container-resource: %s}",
          id, getInstances().toString(), getResource());
    }
  }
}
