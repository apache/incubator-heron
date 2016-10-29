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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import com.google.common.base.Optional;

import com.twitter.heron.spi.packing.InstanceId;
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

  public PackingPlanBuilder updateNumContainers(int count) {
    this.numContainers = count;
    return this;
  }

  // add or remove instances
  public PackingPlanBuilder addInstance(Integer containerId,
                                        InstanceId instanceId) throws ResourceExceededException {
    initContainers();

    Resource instanceResource = PackingUtils.getResourceRequirement(
        instanceId.getComponentName(), this.componentRamMap, this.defaultInstanceResource,
        this.maxContainerResource, this.requestedContainerPadding);

    if (!containers.get(containerId).add(
        new PackingPlan.InstancePlan(instanceId, instanceResource))) {
      throw new ResourceExceededException(String.format(
          "Insufficient container resources to add instance %s with resources %s to container %d.",
          instanceId, instanceResource, containerId));
    }

    LOG.fine(String.format("Added to container %d instance %s", containerId, instanceId));
    return this;
  }

  public boolean removeInstance(Integer containerId, String componentName) {
    initContainers();
    Optional<PackingPlan.InstancePlan> instancePlan =
        containers.get(containerId).removeAnyInstanceOfComponent(componentName);
    return instancePlan.isPresent();
  }

  // build container plan sets by summing up instance resources
  public PackingPlan build() {
    Set<PackingPlan.ContainerPlan> containerPlans = PackingUtils.buildContainerPlans(
        this.containers, this.componentRamMap,
        this.defaultInstanceResource, this.requestedContainerPadding);

    return new PackingPlan(topologyId, containerPlans);
  }

  private void initContainers() {
    // TODO: verify required fields like resources and padding are set
    if (this.containers != null) {
      for (int containerId = containers.size() + 1; containerId <= numContainers; containerId++) {
        containers.put(containerId,
            new Container(containers.get(1).getCapacity(), this.requestedContainerPadding));
      }
      return;
    }

    Map<Integer, Container> newContainerMap = new HashMap<>();
    if (this.existingPacking == null) {
      for (int containerId = 1; containerId <= numContainers; containerId++) {
        newContainerMap.put(containerId,
            new Container(this.maxContainerResource, this.requestedContainerPadding));
      }
    } else {
      // TODO: there is a bug here where the impl below assumes contiguous 1-based container ids,
      // which might not be the case.
      List<Container> newContainers = PackingUtils.getContainers(
          this.existingPacking, this.requestedContainerPadding);
      int containerId = 1;
      for (Container container : newContainers) {
        newContainerMap.put(containerId++, container);
      }
      for (int newContainerId = newContainers.size() + 1;
           newContainerId <= numContainers; newContainerId++) {
        PackingUtils.allocateNewContainer(
            newContainers, newContainers.get(0).getCapacity(), this.requestedContainerPadding);
        newContainerMap.put(newContainerId,
            new Container(newContainerMap.get(1).getCapacity(), this.requestedContainerPadding));
      }
    }

    this.containers = newContainerMap;
  }
}
