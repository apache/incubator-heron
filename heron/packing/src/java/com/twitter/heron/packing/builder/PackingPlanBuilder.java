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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.logging.Logger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;

import com.twitter.heron.packing.ResourceExceededException;
import com.twitter.heron.packing.utils.PackingUtils;
import com.twitter.heron.spi.packing.InstanceId;
import com.twitter.heron.spi.packing.PackingException;
import com.twitter.heron.spi.packing.PackingPlan;
import com.twitter.heron.spi.packing.Resource;

/**
 * Class the help with building packing plans.
 */
public class PackingPlanBuilder {
  private static final Logger LOG = Logger.getLogger(PackingPlanBuilder.class.getName());

  private final String topologyId;
  private final PackingPlan existingPacking;
  private Resource defaultInstanceResource;
  private Resource maxContainerResource;
  private Map<String, Long> componentRamMap;
  private int requestedContainerPadding;
  private int numContainers;

  private Map<Integer, Container> containers;

  public PackingPlanBuilder(String topologyId) {
    this(topologyId, null);
  }

  public PackingPlanBuilder(String topologyId, PackingPlan existingPacking) {
    this.topologyId = topologyId;
    this.existingPacking = existingPacking;
    this.numContainers = 0;
    this.requestedContainerPadding = 0;
    this.componentRamMap = new HashMap<>();
  }

  // set resource settings
  public PackingPlanBuilder setDefaultInstanceResource(Resource resource) {
    this.defaultInstanceResource = resource;
    return this;
  }

  public PackingPlanBuilder setMaxContainerResource(Resource resource) {
    this.maxContainerResource = resource;
    return this;
  }

  public PackingPlanBuilder setRequestedComponentRam(Map<String, Long> ramMap) {
    this.componentRamMap = ramMap;
    return this;
  }

  public PackingPlanBuilder setRequestedContainerPadding(int percent) {
    this.requestedContainerPadding = percent;
    return this;
  }

  // Calling updateNumContainers will produce that many containers, starting with id 1. The build()
  // method will prune out empty containers from the plan.
  public PackingPlanBuilder updateNumContainers(int count) {
    this.numContainers = count;
    return this;
  }

  // adds an instance to a container with id containerId. If that container does not exist, it will
  // be lazily initialized, which could result in more containers than those requested using the
  // updateNumContainers method
  public PackingPlanBuilder addInstance(Integer containerId,
                                        InstanceId instanceId) throws ResourceExceededException {
    initContainer(containerId);
    assertNewInstance(instanceId);

    Resource instanceResource = PackingUtils.getResourceRequirement(
        instanceId.getComponentName(), this.componentRamMap, this.defaultInstanceResource,
        this.maxContainerResource, this.requestedContainerPadding);

    try {
      containers.get(containerId).add(new PackingPlan.InstancePlan(instanceId, instanceResource));
    } catch (ResourceExceededException e) {
      throw new ResourceExceededException(String.format(
          "Insufficient container resources to add instance %s with resources %s to container %d.",
          instanceId, instanceResource, containerId), e);
    }

    LOG.fine(String.format("Added to container %d instance %s", containerId, instanceId));
    return this;
  }

  private void assertNewInstance(InstanceId instanceId) throws PackingException {
    for (Container container : containers.values()) {
      Optional<PackingPlan.InstancePlan> matchingInstance =
          container.getInstance(instanceId.getTaskId());
      if (matchingInstance.isPresent()) {
        throw new PackingException(String.format(
            "Can not add instance %s due to taskId collision with instance %s",
            instanceId, matchingInstance.get()));
      }

//      TODO: enable this after fixing https://github.com/twitter/heron/issues/1579
//      matchingInstance =
//          container.getInstance(instanceId.getComponentName(), instanceId.getComponentIndex());
//      if (matchingInstance.isPresent()) {
//        throw new PackingException(String.format(
//            "Can not add instance %s due to componentIndex collision with instance %s",
//            instanceId, matchingInstance.get()));
//      }
    }
  }

  public void removeInstance(Integer containerId, String componentName) throws PackingException {
    initContainers();
    Container container = containers.get(containerId);

    if (container == null) {
      throw new PackingException(String.format("Failed to remove component '%s' because container "
              + "with id %d does not exist.", componentName, containerId));
    }
    if (!container.removeAnyInstanceOfComponent(componentName).isPresent()) {
      throw new PackingException(String.format("Failed to remove component '%s' because container "
              + "with id %d does not include that component'", componentName, containerId));
    }
  }

  // build container plan sets by summing up instance resources
  public PackingPlan build() {
    assertResourceSettings();
    Set<PackingPlan.ContainerPlan> containerPlans = buildContainerPlans(
        this.containers, this.componentRamMap,
        this.defaultInstanceResource, this.requestedContainerPadding);

    return new PackingPlan(topologyId, containerPlans);
  }

  private void initContainers() {
    assertResourceSettings();
    Map<Integer, Container> newContainerMap = this.containers;

    // if this is the first time called, initialize container map with empty or existing containers
    if (newContainerMap == null) {
      if (this.existingPacking == null) {
        newContainerMap = new HashMap<>();
        for (int containerId = 1; containerId <= numContainers; containerId++) {
          newContainerMap.put(containerId,
              new Container(this.maxContainerResource, this.requestedContainerPadding));
        }
      } else {
        try {
          newContainerMap = getContainers(this.existingPacking, this.requestedContainerPadding);
        } catch (ResourceExceededException e) {
          throw new PackingException(
              "Could not initialize containers using existing packing plan", e);
        }
      }
    }

    if (this.numContainers > newContainerMap.size()) {
      SortedSet<Integer> sortedIds = new TreeSet<>(newContainerMap.keySet());
      int nextContainerId = sortedIds.last() + 1;
      for (int i = 0; i < numContainers - newContainerMap.size(); i++) {
        newContainerMap.put(nextContainerId++, new Container(
            newContainerMap.get(sortedIds.first()).getCapacity(), this.requestedContainerPadding));
      }
    }

    this.containers = newContainerMap;
  }

  private void initContainer(int containerId) {
    initContainers();
    if (this.containers.get(containerId) == null) {
      this.containers.put(containerId, new Container(
          this.maxContainerResource, this.requestedContainerPadding));
    }
  }

  private void assertResourceSettings() {
    if (this.defaultInstanceResource == null) {
      throw new PackingException(
          "defaultInstanceResource must be set on PackingPlanBuilder before modifying containers");
    }
    if (this.maxContainerResource == null) {
      throw new PackingException(
          "maxContainerResource must be set on PackingPlanBuilder before modifying containers");
    }
  }

  /**
   * Estimate the per instance and topology resources for the packing plan based on the ramMap,
   * instance defaults and paddingPercentage.
   *
   * @return container plans
   */
  private static Set<PackingPlan.ContainerPlan> buildContainerPlans(
      Map<Integer, Container> containerInstances,
      Map<String, Long> ramMap,
      Resource instanceDefaults,
      double paddingPercentage) {
    Set<PackingPlan.ContainerPlan> containerPlans = new HashSet<>();

    for (Integer containerId : containerInstances.keySet()) {
      Container container = containerInstances.get(containerId);
      if (container.getInstances().size() == 0) {
        continue;
      }

      long containerRam = 0;
      long containerDiskInBytes = 0;
      double containerCpu = 0;

      // Calculate the resource required for single instance
      Set<PackingPlan.InstancePlan> instancePlans = new HashSet<>();

      for (PackingPlan.InstancePlan instancePlan : container.getInstances()) {
        InstanceId instanceId = new InstanceId(instancePlan.getComponentName(),
            instancePlan.getTaskId(), instancePlan.getComponentIndex());
        long instanceRam;
        if (ramMap.containsKey(instanceId.getComponentName())) {
          instanceRam = ramMap.get(instanceId.getComponentName());
        } else {
          instanceRam = instanceDefaults.getRam();
        }
        containerRam += instanceRam;

        // Currently not yet support disk or cpu config for different components,
        // so just use the default value.
        long instanceDisk = instanceDefaults.getDisk();
        containerDiskInBytes += instanceDisk;

        double instanceCpu = instanceDefaults.getCpu();
        containerCpu += instanceCpu;

        // Insert it into the map
        instancePlans.add(new PackingPlan.InstancePlan(instanceId,
            new Resource(instanceCpu, instanceRam, instanceDisk)));
      }

      containerCpu += (paddingPercentage * containerCpu) / 100;
      containerRam += (paddingPercentage * containerRam) / 100;
      containerDiskInBytes += (paddingPercentage * containerDiskInBytes) / 100;

      Resource resource =
          new Resource(Math.round(containerCpu), containerRam, containerDiskInBytes);

      PackingPlan.ContainerPlan containerPlan =
          new PackingPlan.ContainerPlan(containerId, instancePlans, resource);

      containerPlans.add(containerPlan);
    }

    return containerPlans;
  }

  /**
   * Sort the container plans based on the container Ids
   *
   * @return sorted array of container plans
   */
  @VisibleForTesting
  static PackingPlan.ContainerPlan[] sortOnContainerId(Set<PackingPlan.ContainerPlan> containers) {
    ArrayList<Integer> containerIds = new ArrayList<>();
    PackingPlan.ContainerPlan[] currentContainers =
        new PackingPlan.ContainerPlan[containers.size()];
    for (PackingPlan.ContainerPlan container : containers) {
      containerIds.add(container.getId());
    }
    Collections.sort(containerIds);
    for (PackingPlan.ContainerPlan container : containers) {
      int position = containerIds.indexOf(container.getId());
      currentContainers[position] = container;
    }
    return currentContainers;
  }

  /**
   * Generates the containers that correspond to the current packing plan
   * along with their associated instances.
   *
   * @return Map of containers for the current packing plan, keyed by containerId
   */
  @VisibleForTesting
  static Map<Integer, Container> getContainers(PackingPlan currentPackingPlan,
                                               int paddingPercentage)
      throws ResourceExceededException {
    Map<Integer, Container> containers = new HashMap<>();

    //sort containers based on containerIds;
    PackingPlan.ContainerPlan[] currentContainerPlans =
        sortOnContainerId(currentPackingPlan.getContainers());

    Resource capacity = currentPackingPlan.getMaxContainerResources();
    for (PackingPlan.ContainerPlan currentContainerPlan : currentContainerPlans) {
      Container container = new Container(capacity, paddingPercentage);
      for (PackingPlan.InstancePlan instancePlan : currentContainerPlan.getInstances()) {
        try {
          container.add(instancePlan);
        } catch (ResourceExceededException e) {
          throw new ResourceExceededException(String.format(
              "Insufficient container resources to add instancePlan %s to container %s",
              instancePlan, container), e);
        }
      }
      containers.put(currentContainerPlan.getId(), container);
    }
    return containers;
  }
}
