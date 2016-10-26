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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import com.google.common.base.Optional;

import com.twitter.heron.spi.common.Constants;
import com.twitter.heron.spi.packing.InstanceId;
import com.twitter.heron.spi.packing.PackingPlan;
import com.twitter.heron.spi.packing.Resource;

/**
 * Class the help with building packing plans.
 */
public class PackingPlanBuilder {
  private static final Logger LOG = Logger.getLogger(PackingPlanBuilder.class.getName());
  private static final long MIN_RAM_PER_INSTANCE = 192L * Constants.MB;

  private final String topologyId;
  private final PackingPlan existingPacking;
  private Resource defaultInstanceResource;
  private Resource maxContainerResource;
  private Map<String, Long> componentRamMap;
  private int requestedContainerPadding;
  private int numContainers;

  private Map<Integer, List<InstanceId>> containerInstances;
  private List<Container> containers;

  public PackingPlanBuilder(String topologyId) {
    this(topologyId, null);
  }

  public PackingPlanBuilder(String topologyId, PackingPlan existingPacking) {
    this.topologyId = topologyId;
    this.existingPacking = existingPacking;
    this.containerInstances = new HashMap<Integer, List<InstanceId>>();
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

  public PackingPlanBuilder resetNumContainers(int count) { // can be called repeatedly
    this.containers = null;
    this.numContainers = count;
    return this;
  }

  // add or remove instances
  public PackingPlanBuilder addInstance(Integer containerId,
                                        InstanceId instanceId) throws ResourceExceededException {
    initContainers();
    if (containerInstances.get(containerId) == null) {
      containerInstances.put(containerId, new ArrayList<InstanceId>());
    }

    long ramRequirement = getRamRequirement(instanceId.getComponentName());
    Resource instanceResource = this.defaultInstanceResource.cloneWithRam(ramRequirement);
    if (!containers.get(containerId - 1)
        .add(new PackingPlan.InstancePlan(instanceId, instanceResource))) {
      throw new ResourceExceededException(); //TODO
    }
    containerInstances.get(containerId).add(instanceId);
    LOG.info(String.format("Added to container %d instance %s", containerId, instanceId));
    return this;
  }

  public boolean removeInstance(Integer containerId, String componentName) {
    initContainers();
    Optional<PackingPlan.InstancePlan> instancePlan =
        containers.get(containerId - 1).removeAnyInstanceOfComponent(componentName);
    if (!instancePlan.isPresent()) {
      return false;
    }

    InstanceId instanceId = new InstanceId(instancePlan.get().getComponentName(),
                                           instancePlan.get().getTaskId(),
                                           instancePlan.get().getComponentIndex());
    containerInstances.get(containerId).remove(instanceId);
    return true;
  }

  // build container plan sets by summing up instance resources
  // mostly impl from PackingUtils.buildContainerPlans
  public PackingPlan build() {
    PackingUtils.removeEmptyContainers(this.containerInstances);
    Set<PackingPlan.ContainerPlan> containerPlans = PackingUtils.buildContainerPlans(
        this.containerInstances, this.componentRamMap,
        this.defaultInstanceResource, this.requestedContainerPadding);

    return new PackingPlan(topologyId, containerPlans);
  }

  private void initContainers() {
    // TODO: verify required fields like numContainers are set
    if (this.containers != null) {
      return;
    }

    Map<Integer, List<InstanceId>> newContainerInstances;
    ArrayList<Container> newContainers;
    if (this.existingPacking == null) {
      newContainerInstances = new HashMap<Integer, List<InstanceId>>();
      newContainers = new ArrayList<>();

      for (int i = 0; i <= numContainers - 1; i++) {
        if (newContainerInstances.get(i) == null) {
          newContainerInstances.put(i, new ArrayList<InstanceId>());
        }
        PackingUtils.allocateNewContainer(
            newContainers, this.maxContainerResource, this.requestedContainerPadding);
      }
    } else {
      newContainerInstances = PackingUtils.getAllocation(this.existingPacking);

      newContainers =
          PackingUtils.getContainers(this.existingPacking, this.requestedContainerPadding);
      for (int i = newContainers.size(); i <= numContainers - 1; i++) {
        PackingUtils.allocateNewContainer(
            newContainers, newContainers.get(0).getCapacity(), this.requestedContainerPadding);
      }
    }

    this.containerInstances = newContainerInstances;
    this.containers = newContainers;
  }

  // TODO: exception handling
  private long getRamRequirement(String component) {
    if (componentRamMap.containsKey(component)) {
      if (!PackingUtils.isValidInstance(
          this.defaultInstanceResource.cloneWithRam(componentRamMap.get(component)),
          MIN_RAM_PER_INSTANCE, this.maxContainerResource, this.requestedContainerPadding)) {
        throw new RuntimeException("The topology configuration does not have "
            + "valid resource requirements. Please make sure that the instance resource "
            + "requirements do not exceed the maximum per-container resources.");
      } else {
        return componentRamMap.get(component);
      }
    } else {
      if (!PackingUtils.isValidInstance(this.defaultInstanceResource,
          MIN_RAM_PER_INSTANCE, this.maxContainerResource, this.requestedContainerPadding)) {
        throw new RuntimeException("The topology configuration does not have "
            + "valid resource requirements. Please make sure that the instance resource "
            + "requirements do not exceed the maximum per-container resources.");
      } else {
        return this.defaultInstanceResource.getRam();
      }
    }
  }
}
