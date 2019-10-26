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

package org.apache.heron.packing.builder;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
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

import org.apache.heron.packing.constraints.InstanceConstraint;
import org.apache.heron.packing.constraints.PackingConstraint;
import org.apache.heron.packing.exceptions.ConstraintViolationException;
import org.apache.heron.packing.exceptions.ResourceExceededException;
import org.apache.heron.spi.packing.InstanceId;
import org.apache.heron.spi.packing.PackingException;
import org.apache.heron.spi.packing.PackingPlan;
import org.apache.heron.spi.packing.Resource;

/**
 * Class that helps with building packing plans.
 */
public class PackingPlanBuilder {
  private static final Logger LOG = Logger.getLogger(PackingPlanBuilder.class.getName());

  private final String topologyId;
  private final PackingPlan existingPacking;

  private Resource defaultInstanceResource;
  private Resource maxContainerResource;
  private Map<String, Resource> componentResourceMap;
  private Resource requestedContainerPadding;
  private List<PackingConstraint> packingConstraints;
  private List<InstanceConstraint> instanceConstraints;

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
    this.requestedContainerPadding = Resource.EMPTY_RESOURCE;
    this.packingConstraints = new ArrayList<>();
    this.instanceConstraints = new ArrayList<>();
  }

  public Map<Integer, Container> getContainers() {
    return containers;
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

  public PackingPlanBuilder setRequestedComponentResource(Map<String, Resource> resourceMap) {
    this.componentResourceMap = resourceMap;
    return this;
  }

  public PackingPlanBuilder setRequestedContainerPadding(Resource padding) {
    this.requestedContainerPadding = padding;
    return this;
  }

  public PackingPlanBuilder setPackingConstraints(List<PackingConstraint> packingConstraintLst) {
    this.packingConstraints = packingConstraintLst;
    return this;
  }

  public PackingPlanBuilder setInstanceConstraints(List<InstanceConstraint> instanceConstraintLst) {
    this.instanceConstraints = instanceConstraintLst;
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
                                        String componentName) throws ConstraintViolationException {
    // create container if not existed
    initContainer(containerId);

    Integer taskId = taskIds.isEmpty() ? 1 : taskIds.last() + 1;
    Integer componentIndex = componentIndexes.containsKey(componentName)
        ? componentIndexes.get(componentName).last() + 1 : 0;
    InstanceId instanceId = new InstanceId(componentName, taskId, componentIndex);

    Resource instanceResource = componentResourceMap.getOrDefault(componentName,
        defaultInstanceResource);

    Container container = containers.get(containerId);
    PackingPlan.InstancePlan instancePlan
        = new PackingPlan.InstancePlan(instanceId, instanceResource);

    // Check constraints
    for (InstanceConstraint constraint : instanceConstraints) {
      constraint.validate(instancePlan);
    }
    for (PackingConstraint constraint : packingConstraints) {
      constraint.validate(container, instancePlan);
    }
    addToContainer(container, instancePlan, this.componentIndexes, this.taskIds);

    LOG.finest(String.format("Added to container %d instance %s", containerId, instanceId));
    return this;
  }

  @SuppressWarnings("JavadocMethod")
  /**
   * Add an instance to the first container possible ranked by score.
   * @return containerId of the container the instance was added to
   * @throws ResourceExceededException if the instance could not be added
   */
  public int addInstance(Scorer<Container> scorer,
                         String componentName) throws ResourceExceededException {
    List<Scorer<Container>> scorers = new LinkedList<>();
    scorers.add(scorer);
    return addInstanceToExistingContainers(scorers, componentName);
  }

  @SuppressWarnings("JavadocMethod")
  /**
   * Add an instance to the first container possible ranked by score.
   * If a scoring tie exists, uses the next scorer in the scorers list to break the tie.
   * @return containerId of the container the instance was added to
   * @throws ResourceExceededException if no existing container can accommodate the instance
   */
  public int addInstanceToExistingContainers(List<Scorer<Container>> scorers, String componentName)
      throws ResourceExceededException {
    initContainers();
    for (Container container : sortContainers(scorers, this.containers.values())) {
      try {
        addInstance(container.getContainerId(), componentName);
        return container.getContainerId();
      } catch (ConstraintViolationException e) {
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
      if (componentIndexes.containsKey(componentName)) {
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
   * @throws org.apache.heron.spi.packing.PackingException if the instance could not be removed
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
   * @throws org.apache.heron.spi.packing.PackingException if the instance could not be removed
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
    Set<PackingPlan.ContainerPlan> containerPlans = buildContainerPlans(this.containers);
    return new PackingPlan(topologyId, containerPlans);
  }

  private void initContainers() {
    assertResourceSettings();

    if (this.componentIndexes == null) {
      this.componentIndexes = new HashMap<>();
    }

    if (this.taskIds == null) {
      this.taskIds = new TreeSet<>();
    }

    // if this is the first time called, initialize container map with empty or existing containers
    if (this.containers == null) {
      if (this.existingPacking == null) {
        this.containers = new HashMap<>();
        for (int containerId = 1; containerId <= numContainers; containerId++) {
          this.containers.put(containerId, new Container(
              containerId, this.maxContainerResource, this.requestedContainerPadding));
        }
      } else {
        this.containers = getContainers(this.existingPacking,
            this.maxContainerResource,
            this.requestedContainerPadding,
            this.componentIndexes, this.taskIds);
      }
    }

    if (this.numContainers > this.containers.size()) {
      List<Scorer<Container>> scorers = new ArrayList<>();
      scorers.add(new ContainerIdScorer());
      List<Container> sortedContainers = sortContainers(scorers, this.containers.values());

      int nextContainerId = sortedContainers.get(sortedContainers.size() - 1).getContainerId() + 1;
      Resource capacity =
          this.containers.get(sortedContainers.get(0).getContainerId()).getCapacity();

      for (int i = 0; i < numContainers - this.containers.size(); i++) {
        this.containers.put(nextContainerId,
            new Container(nextContainerId, capacity, this.requestedContainerPadding));
        nextContainerId++;
      }
    }
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
      Map<Integer, Container> containerInstances) {
    Set<PackingPlan.ContainerPlan> containerPlans = new LinkedHashSet<>();

    for (Integer containerId : containerInstances.keySet()) {
      Container container = containerInstances.get(containerId);
      if (container.getInstances().size() == 0) {
        continue;
      }

      Resource totalUsedResources = container.getTotalUsedResources();
      Resource resource = new Resource(
          Math.round(totalUsedResources.getCpu()),
          totalUsedResources.getRam(), totalUsedResources.getDisk());

      PackingPlan.ContainerPlan containerPlan =
          new PackingPlan.ContainerPlan(containerId, container.getInstances(), resource);

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
      PackingPlan currentPackingPlan, Resource maxContainerResource, Resource padding,
      Map<String, TreeSet<Integer>> componentIndexes, TreeSet<Integer> taskIds) {
    Map<Integer, Container> containers = new HashMap<>();

    Resource capacity = maxContainerResource;
    for (PackingPlan.ContainerPlan currentContainerPlan : currentPackingPlan.getContainers()) {
      Container container =
          new Container(currentContainerPlan.getId(), capacity, padding);
      for (PackingPlan.InstancePlan instancePlan : currentContainerPlan.getInstances()) {
        addToContainer(container, instancePlan, componentIndexes, taskIds);
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
                                     Set<Integer> taskIds) {
    container.add(instancePlan);
    String componentName = instancePlan.getComponentName();

    // update componentIndex and taskIds
    componentIndexes.computeIfAbsent(componentName, k -> new TreeSet<Integer>());
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
