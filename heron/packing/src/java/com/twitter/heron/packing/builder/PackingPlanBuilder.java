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
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.TreeSet;
import java.util.logging.Logger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;

import com.twitter.heron.common.basics.ByteAmount;
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
  private Map<String, ByteAmount> componentRamMap;
  private int requestedContainerPadding;
  private int numContainers;

  private Map<Integer, Container> containers;
  private TreeSet<Integer> taskIds; // globally unique ids assigned to instances
  private HashMap<String, TreeSet<Integer>> componentIndexes; // componentName -> componentIndexes

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

  public PackingPlanBuilder setRequestedComponentRam(Map<String, ByteAmount> ramMap) {
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
                                        String componentName) throws ResourceExceededException {
    initContainer(containerId);

    Integer taskId = taskIds.isEmpty() ? 1 : taskIds.last() + 1;
    Integer componentIndex = componentIndexes.get(componentName) != null
        ? componentIndexes.get(componentName).last() + 1 : 0;

    InstanceId instanceId = new InstanceId(componentName, taskId, componentIndex);

    Resource instanceResource = PackingUtils.getResourceRequirement(
        componentName, this.componentRamMap, this.defaultInstanceResource,
        this.maxContainerResource, this.requestedContainerPadding);

    try {
      addToContainer(containers.get(containerId),
          new PackingPlan.InstancePlan(instanceId, instanceResource),
          this.componentIndexes, this.taskIds);
    } catch (ResourceExceededException e) {
      throw new ResourceExceededException(String.format(
          "Insufficient container resources to add instance %s with resources %s to container %d.",
          instanceId, instanceResource, containerId), e);
    }

    LOG.fine(String.format("Added to container %d instance %s", containerId, instanceId));
    return this;
  }

  @SuppressWarnings("JavadocMethod")
  /**
   * Add an instance to the first container possible ranked by score.
   * @return containerId of the container the instance was added to
   * @throws com.twitter.heron.packing.ResourceExceededException if the instance could not be added
   */
  public int addInstance(Scorer<Container> scorer,
                         String componentName) throws ResourceExceededException {
    List<Scorer<Container>> scorers = new LinkedList<>();
    scorers.add(scorer);
    return addInstance(scorers, componentName);
  }

  @SuppressWarnings("JavadocMethod")
  /**
   * Add an instance to the first container possible ranked by score. If a scoring tie exists,
   * uses the next scorer in the scorers list to break the tie.
   * @return containerId of the container the instance was added to
   * @throws com.twitter.heron.packing.ResourceExceededException if the instance could not be added
   */
  private int addInstance(List<Scorer<Container>> scorers, String componentName)
      throws ResourceExceededException {
    initContainers();
    for (Container container : sortContainers(scorers, this.containers.values())) {
      try {
        addInstance(container.getContainerId(), componentName);
        return container.getContainerId();
      } catch (ResourceExceededException e) {
        // ignore since we'll continue trying
      }
    }
    //Not enough containers.
    throw new ResourceExceededException(String.format(
        "Insufficient resources to add '%s' instance to any of the %d containers.",
        componentName, this.containers.size()));
  }

  public void removeInstance(Integer containerId, String componentName) throws PackingException {
    initContainers();
    Container container = containers.get(containerId);

    if (container == null) {
      throw new PackingException(String.format("Failed to remove component '%s' because container "
              + "with id %d does not exist.", componentName, containerId));
    }
    Optional<PackingPlan.InstancePlan> instancePlan =
        container.removeAnyInstanceOfComponent(componentName);
    if (instancePlan.isPresent()) {
      taskIds.remove(instancePlan.get().getTaskId());
      if (componentIndexes.get(componentName) != null) {
        componentIndexes.get(componentName).remove(instancePlan.get().getComponentIndex());
      }
    } else {
      throw new PackingException(String.format("Failed to remove component '%s' because container "
              + "with id %d does not include that component'", componentName, containerId));
    }
  }

  @SuppressWarnings("JavadocMethod")
  /**
   * Remove an instance from the first container possible ranked by score.
   * @return containerId of the container the instance was removed from
   * @throws com.twitter.heron.spi.packing.PackingException if the instance could not be removed
   */
  public int removeInstance(Scorer<Container> scorer, String componentName) {
    List<Scorer<Container>> scorers = new LinkedList<>();
    scorers.add(scorer);
    return removeInstance(scorers, componentName);
  }

  @SuppressWarnings("JavadocMethod")
  /**
   * Remove an instance from the first container possible ranked by score. If a scoring tie exists,
   * uses the next scorer in the scorers list to break the tie.
   * @return containerId of the container the instance was removed from
   * @throws com.twitter.heron.spi.packing.PackingException if the instance could not be removed
   */
  public int removeInstance(List<Scorer<Container>> scorers, String componentName) {
    initContainers();
    for (Container container : sortContainers(scorers, this.containers.values())) {
      try {
        removeInstance(container.getContainerId(), componentName);
        return container.getContainerId();
      } catch (PackingException e) {
        // ignore since we keep trying
      }
    }
    throw new PackingException("Cannot remove instance. No more instances of component "
        + componentName + " exist in the containers.");
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
    HashMap<String, TreeSet<Integer>> newComponentIndexes = this.componentIndexes;
    TreeSet<Integer> newTaskIds = this.taskIds;

    if (newComponentIndexes == null) {
      newComponentIndexes = new HashMap<>();
    }

    if (newTaskIds == null) {
      newTaskIds = new TreeSet<>();
    }

    // if this is the first time called, initialize container map with empty or existing containers
    if (newContainerMap == null) {
      if (this.existingPacking == null) {
        newContainerMap = new HashMap<>();
        for (int containerId = 1; containerId <= numContainers; containerId++) {
          newContainerMap.put(containerId, new Container(
              containerId, this.maxContainerResource, this.requestedContainerPadding));
        }
      } else {
        try {
          newContainerMap = getContainers(this.existingPacking, this.requestedContainerPadding,
              newComponentIndexes, newTaskIds);
        } catch (ResourceExceededException e) {
          throw new PackingException(
              "Could not initialize containers using existing packing plan", e);
        }
      }
    }

    if (this.numContainers > newContainerMap.size()) {
      List<Scorer<Container>> scorers = new ArrayList<>();
      scorers.add(new ContainerIdScorer());
      List<Container> sortedContainers = sortContainers(scorers, newContainerMap.values());

      int nextContainerId = sortedContainers.get(sortedContainers.size() - 1).getContainerId() + 1;
      Resource capacity =
          newContainerMap.get(sortedContainers.get(0).getContainerId()).getCapacity();

      for (int i = 0; i < numContainers - newContainerMap.size(); i++) {
        newContainerMap.put(nextContainerId,
            new Container(nextContainerId, capacity, this.requestedContainerPadding));
        nextContainerId++;
      }
    }

    this.containers = newContainerMap;
    this.componentIndexes = newComponentIndexes;
    this.taskIds = newTaskIds;
  }

  private void initContainer(int containerId) {
    initContainers();
    if (this.containers.get(containerId) == null) {
      this.containers.put(containerId, new Container(
          containerId, this.maxContainerResource, this.requestedContainerPadding));
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
      Map<String, ByteAmount> ramMap,
      Resource instanceDefaults,
      int paddingPercentage) {
    Set<PackingPlan.ContainerPlan> containerPlans = new LinkedHashSet<>();

    for (Integer containerId : containerInstances.keySet()) {
      Container container = containerInstances.get(containerId);
      if (container.getInstances().size() == 0) {
        continue;
      }

      ByteAmount containerRam = ByteAmount.ZERO;
      ByteAmount containerDiskInBytes = ByteAmount.ZERO;
      double containerCpu = 0;

      // Calculate the resource required for single instance
      Set<PackingPlan.InstancePlan> instancePlans = new HashSet<>();

      for (PackingPlan.InstancePlan instancePlan : container.getInstances()) {
        InstanceId instanceId = new InstanceId(instancePlan.getComponentName(),
            instancePlan.getTaskId(), instancePlan.getComponentIndex());
        ByteAmount instanceRam;
        if (ramMap.containsKey(instanceId.getComponentName())) {
          instanceRam = ramMap.get(instanceId.getComponentName());
        } else {
          instanceRam = instanceDefaults.getRam();
        }
        containerRam = containerRam.plus(instanceRam);

        // Currently not yet support disk or cpu config for different components,
        // so just use the default value.
        ByteAmount instanceDisk = instanceDefaults.getDisk();
        containerDiskInBytes = containerDiskInBytes.plus(instanceDisk);

        double instanceCpu = instanceDefaults.getCpu();
        containerCpu += instanceCpu;

        // Insert it into the map
        instancePlans.add(new PackingPlan.InstancePlan(instanceId,
            new Resource(instanceCpu, instanceRam, instanceDisk)));
      }

      containerCpu += (paddingPercentage * containerCpu) / 100;
      containerRam = containerRam.increaseBy(paddingPercentage);
      containerDiskInBytes = containerDiskInBytes.increaseBy(paddingPercentage);

      Resource resource =
          new Resource(Math.round(containerCpu), containerRam, containerDiskInBytes);

      PackingPlan.ContainerPlan containerPlan =
          new PackingPlan.ContainerPlan(containerId, instancePlans, resource);

      containerPlans.add(containerPlan);
    }

    return containerPlans;
  }

  /**
   * Generates the containers that correspond to the current packing plan
   * along with their associated instances.
   *
   * @return Map of containers for the current packing plan, keyed by containerId
   */
  @VisibleForTesting
  static Map<Integer, Container> getContainers(
      PackingPlan currentPackingPlan, int paddingPercentage,
      Map<String, TreeSet<Integer>> componentIndexes, TreeSet<Integer> taskIds)
      throws ResourceExceededException {
    Map<Integer, Container> containers = new HashMap<>();

    Resource capacity = currentPackingPlan.getMaxContainerResources();
    for (PackingPlan.ContainerPlan currentContainerPlan : currentPackingPlan.getContainers()) {
      Container container =
          new Container(currentContainerPlan.getId(), capacity, paddingPercentage);
      for (PackingPlan.InstancePlan instancePlan : currentContainerPlan.getInstances()) {
        try {
          addToContainer(container, instancePlan, componentIndexes, taskIds);
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

  @VisibleForTesting
  static List<Container> sortContainers(List<Scorer<Container>> scorers,
                                        Collection<Container> containers) {
    List<Container> sorted = new ArrayList<>(containers);
    Collections.sort(sorted, new ChainedContainerComparator<>(scorers));
    return sorted;
  }

  /**
   * Add instancePlan to container and update the componentIndexes and taskIds indexes
   */
  private static void addToContainer(Container container,
                                     PackingPlan.InstancePlan instancePlan,
                                     Map<String, TreeSet<Integer>> componentIndexes,
                                     Set<Integer> taskIds) throws ResourceExceededException {
    container.add(instancePlan);
    String componentName = instancePlan.getComponentName();

    if (componentIndexes.get(componentName) == null) {
      componentIndexes.put(componentName, new TreeSet<Integer>());
    }
    componentIndexes.get(componentName).add(instancePlan.getComponentIndex());
    taskIds.add(instancePlan.getTaskId());
  }

  private static class ChainedContainerComparator<T> implements Comparator<T> {
    private final Comparator<T> comparator;
    private final ChainedContainerComparator<T> tieBreaker;

    ChainedContainerComparator(List<Scorer<T>> scorers) {
      this((Queue<Scorer<T>>) new LinkedList<Scorer<T>>(scorers));
    }

    ChainedContainerComparator(Queue<Scorer<T>> scorers) {
      if (scorers.isEmpty()) {
        this.comparator = new EqualsComparator<T>();
        this.tieBreaker = null;
      } else {
        this.comparator = new ContainerComparator<T>(scorers.remove());
        this.tieBreaker = new ChainedContainerComparator<T>(scorers);
      }
    }

    @Override
    public int compare(T thisOne, T thatOne) {

      int delta = comparator.compare(thisOne, thatOne);
      if (delta != 0 || this.tieBreaker == null) {
        return delta;
      }
      return tieBreaker.compare(thisOne, thatOne);
    }
  }

  private static class ContainerComparator<T> implements Comparator<T> {
    private Scorer<T> scorer;

    ContainerComparator(Scorer<T> scorer) {
      this.scorer = scorer;
    }

    @Override
    public int compare(T thisOne, T thatOne) {
      int sign = 1;
      if (!scorer.sortAscending()) {
        sign = -1;
      }
      return sign * (getScore(thisOne) - getScore(thatOne));
    }

    private int getScore(T container) {
      return (int) (1000 * scorer.getScore(container));
    }
  }

  private static class EqualsComparator<T> implements Comparator<T> {
    @Override
    public int compare(T thisOne, T thatOne) {
      return 0;
    }
  }
}
