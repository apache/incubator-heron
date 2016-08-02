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

public class PackingPlan {
  public final String id;
  public final Map<String, ContainerPlan> containers;
  public final Resource resource;

  public PackingPlan(String id, Map<String, ContainerPlan> containers, Resource resource) {
    this.id = id;
    this.containers = containers;
    this.resource = resource;
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

  /**
   * Type definition of packing structure output.
   */
  public static class Resource {
    public double cpu;
    public long ram;
    public long disk;

    public Resource(double cpu, long ram, long disk) {
      this.cpu = cpu;
      this.ram = ram;
      this.disk = disk;
    }

    @Override
    public boolean equals(Object obj) {
      if (obj instanceof Resource) {
        Resource r = (Resource) obj;
        return (this.cpu == r.cpu) && (this.ram == r.ram) && (this.disk == r.disk);
      } else {
        return false;
      }
    }

    @Override
    public int hashCode() {
      return (Long.hashCode(ram) << 2) & (Long.hashCode(disk) << 1) & (Double.hashCode(cpu));
    }

    @Override
    public String toString() {
      return String.format("{cpu: %f, ram: %d, disk: %d}", cpu, ram, disk);
    }
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

    @Override
    public String toString() {
      return String.format("{container-id: %s, instances-list: %s, container-resource: %s}",
          id, instances.toString(), resource);
    }
  }
}
