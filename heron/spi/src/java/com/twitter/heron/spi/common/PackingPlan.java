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

package com.twitter.heron.spi.common;

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

  /**
   * Pack the packing plan into a String describing instance distribution, used by executor
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
