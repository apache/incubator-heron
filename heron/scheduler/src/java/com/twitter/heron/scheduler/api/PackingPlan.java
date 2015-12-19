package com.twitter.heron.scheduler.api;

import java.util.Map;

public class PackingPlan {
  /**
   * Type definition of packing structure output.
   */
  public static class Resource {
    public Double cpu;
    public Long ram;
    public Long disk;

    public Resource(Double cpu, Long ram, Long disk) {
      this.cpu = cpu;
      this.ram = ram;
      this.disk = disk;
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
  }

  public final String id;
  public final Map<String, ContainerPlan> containers;
  public final Resource resource;

  public PackingPlan(String id, Map<String, ContainerPlan> containers, Resource resource) {
    this.id = id;
    this.containers = containers;
    this.resource = resource;
  }
}
