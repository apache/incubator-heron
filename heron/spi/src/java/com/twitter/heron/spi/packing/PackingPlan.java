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
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class PackingPlan {
  public final String id;
  public final Map<String, ContainerPlan> containers;
  public final Resource resource;

  private final Set<ContainerPlan> containerSet;

  public PackingPlan(String id, Map<String, ContainerPlan> containers, Resource resource) {
    this.id = id;
    this.containers = containers;
    this.resource = resource;
    containerSet = new HashSet<>(containers.values());
  }

  @Override
  public String toString() {
    return String.format("{plan-id: %s, containers-list: %s, plan-resource: %s}",
        id, containers.toString(), resource);
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
    for (ContainerPlan containerPlan : containers.values()) {
      for (InstancePlan instancePlan : containerPlan.instances.values()) {
        Integer count = 0;
        if (componentCounts.containsKey(instancePlan.componentName)) {
          count = componentCounts.get(instancePlan.componentName);
        }
        componentCounts.put(instancePlan.componentName, ++count);
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
    StringBuilder[] containerBuilder = new StringBuilder[this.containers.size()];
    for (PackingPlan.ContainerPlan container : this.containers.values()) {
      int index = Integer.parseInt(container.id);
      containerBuilder[index - 1] = new StringBuilder();

      for (PackingPlan.InstancePlan instance : container.instances.values()) {
        String[] tokens = instance.id.split(":");
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

  public String getId() {
    return id;
  }

  public Resource getResource() {
    return resource;
  }

  /**
   * Get the containers created in this packing plan
   *
   * @return Map describing the container info
   */
  public Map<String, ContainerPlan> getContainers() {
    return this.containers;
  }

  public Set<ContainerPlan> getContainerSet() {
    return this.containerSet;
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
    for (ContainerPlan containerPlan : this.containers.values()) {
      for (InstancePlan instancePlan : containerPlan.instances.values()) {
        ramMap.put(instancePlan.componentName, instancePlan.resource.ram);
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
    public final String id;
    public final String componentName;
    public final Resource resource;

    public InstancePlan(String id, String componentName, Resource resource) {
      this.id = id;
      this.componentName = componentName;
      this.resource = resource;
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

      return id.equals(that.id)
          && componentName.equals(that.componentName)
          && resource.equals(that.resource);
    }

    @Override
    public int hashCode() {
      int result = id.hashCode();
      result = 31 * result + componentName.hashCode();
      result = 31 * result + resource.hashCode();
      return result;
    }

    @Override
    public String toString() {
      return String.format("{instance-id: %s, componentName: %s, instance-resource: %s}",
          id, componentName, resource.toString());
    }
  }

  public static class ContainerPlan {
    public final String id;
    public final Map<String, InstancePlan> instances;
    public final Resource resource;

    public ContainerPlan(String id,
                         Map<String, InstancePlan> instances,
                         Resource resource) {
      this.id = id;
      this.instances = instances;
      this.resource = resource;
    }

    public String getId() {
      return id;
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
          && instances.equals(that.instances)
          && resource.equals(that.resource);
    }

    @Override
    public int hashCode() {
      int result = id.hashCode();
      result = 31 * result + instances.hashCode();
      result = 31 * result + resource.hashCode();
      return result;
    }

    @Override
    public String toString() {
      return String.format("{container-id: %s, instances-list: %s, container-resource: %s}",
          id, instances.toString(), resource);
    }
  }
}
