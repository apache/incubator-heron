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

package com.twitter.heron.packing.roundrobin;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import com.google.common.base.Optional;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.common.basics.Pair;
import com.twitter.heron.packing.Container;
import com.twitter.heron.packing.PackingUtils;
import com.twitter.heron.packing.RamRequirement;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Constants;
import com.twitter.heron.spi.common.Context;
import com.twitter.heron.spi.packing.IPacking;
import com.twitter.heron.spi.packing.IRepacking;
import com.twitter.heron.spi.packing.InstanceId;
import com.twitter.heron.spi.packing.PackingPlan;
import com.twitter.heron.spi.packing.Resource;
import com.twitter.heron.spi.utils.TopologyUtils;

import static com.twitter.heron.api.Config.TOPOLOGY_CONTAINER_CPU_REQUESTED;
import static com.twitter.heron.api.Config.TOPOLOGY_CONTAINER_DISK_REQUESTED;
import static com.twitter.heron.api.Config.TOPOLOGY_CONTAINER_PADDING_PERCENTAGE;
import static com.twitter.heron.api.Config.TOPOLOGY_CONTAINER_RAM_REQUESTED;

/**
 * ResourceCompliantRoundRobin packing algorithm
 * <p>
 * This IPacking implementation generates a PackingPlan using a round robin algorithm.
 * <p>
 * Following semantics are guaranteed:
 * 1. Supports heterogeneous containers.
 * The user provides the number of containers to use as well as
 * the maximum container size and a padding percentage.
 * The padding percentage whose values range from [0, 100], determines the per container
 * resources allocated for system-related processes (e.g., the stream manager).
 * <p>
 * 2. The user provides the maximum CPU, RAM and Disk that can be used by each container through
 * the com.twitter.heron.api.Config.TOPOLOGY_CONTAINER_CPU_REQUESTED,
 * com.twitter.heron.api.Config.TOPOLOGY_CONTAINER_RAM_REQUESTED,
 * com.twitter.heron.api.Config.TOPOLOGY_CONTAINER_DISK_REQUESTED parameters.
 * If the parameters are not specified then a default value is used for the maximum container
 * size.
 * <p>
 * 3. The user provides a percentage of each container size that will be used for padding
 * through the com.twitter.heron.api.Config.TOPOLOGY_CONTAINER_PADDING_PERCENTAGE
 * If the parameter is not specified then a default value of 10 is used (10% of the container size)
 * <p>
 * 4. The ram required for one instance is calculated as:
 * value in com.twitter.heron.api.Config.TOPOLOGY_COMPONENT_RAMMAP if exists, otherwise,
 * the default ram value for one instance.
 * <p>
 * 5. The cpu required for one instance is calculated as the default cpu value for one instance.
 * <p>
 * 6. The disk required for one instance is calculated as the default disk value for one instance.
 * <p>
 * 7. The ram required for a container is calculated as:
 * (ram for instances in container) + (paddingPercentage * ram for instances in container)
 * <p>
 * 8. The cpu required for a container is calculated as:
 * (cpu for instances in container) + (paddingPercentage * cpu for instances in container)
 * <p>
 * 9. The disk required for a container is calculated as:
 * (disk for instances in container) + ((paddingPercentage * disk for instances in container)
 * <p>
 * 10. The pack() return null if PackingPlan fails to pass the safe check, for instance,
 * the size of ram for an instance is less than the minimal required value.
 */
public class ResourceCompliantRRPacking implements IPacking, IRepacking {

  private static final long MIN_RAM_PER_INSTANCE = 192L * Constants.MB;
  private static final int DEFAULT_CONTAINER_PADDING_PERCENTAGE = 10;
  private static final int DEFAULT_NUMBER_INSTANCES_PER_CONTAINER = 4;

  private static final Logger LOG = Logger.getLogger(ResourceCompliantRRPacking.class.getName());
  private TopologyAPI.Topology topology;

  private Resource defaultInstanceResources;
  private Resource maxContainerResources;
  private int numContainers;
  private int numAdjustments;
  //ContainerId  to examine next. It is set to 1 when the
  //algorithm restarts with a new number of containers
  private int containerId;

  private int paddingPercentage;

  private void adjustNumContainers(int additionalContainers) {
    increaseNumContainers(additionalContainers);
    this.numAdjustments++;
  }

  private void increaseNumContainers(int additionalContainers) {
    this.numContainers += additionalContainers;
  }

  private void resetState() {
    this.containerId = 1;
    this.numAdjustments = 0;
  }

  @Override
  public void initialize(Config config, TopologyAPI.Topology inputTopology) {
    this.topology = inputTopology;
    this.numContainers = TopologyUtils.getNumContainers(topology);
    this.defaultInstanceResources = new Resource(
        Context.instanceCpu(config),
        Context.instanceRam(config),
        Context.instanceDisk(config));
    resetState();


    double defaultCpu = this.defaultInstanceResources.getCpu()
        * DEFAULT_NUMBER_INSTANCES_PER_CONTAINER;
    long defaultRam = this.defaultInstanceResources.getRam()
        * DEFAULT_NUMBER_INSTANCES_PER_CONTAINER;
    long defaultDisk = this.defaultInstanceResources.getDisk()
        * DEFAULT_NUMBER_INSTANCES_PER_CONTAINER;

    List<TopologyAPI.Config.KeyValue> topologyConfig = topology.getTopologyConfig().getKvsList();

    this.paddingPercentage = TopologyUtils.getConfigWithDefault(topologyConfig,
        TOPOLOGY_CONTAINER_PADDING_PERCENTAGE, DEFAULT_CONTAINER_PADDING_PERCENTAGE);

    this.maxContainerResources = new Resource(
        TopologyUtils.getConfigWithDefault(topologyConfig, TOPOLOGY_CONTAINER_CPU_REQUESTED,
            (double) Math.round(PackingUtils.increaseBy(defaultCpu, paddingPercentage))),
        TopologyUtils.getConfigWithDefault(topologyConfig, TOPOLOGY_CONTAINER_RAM_REQUESTED,
            PackingUtils.increaseBy(defaultRam, paddingPercentage)),
        TopologyUtils.getConfigWithDefault(topologyConfig, TOPOLOGY_CONTAINER_DISK_REQUESTED,
            PackingUtils.increaseBy(defaultDisk, paddingPercentage)));
  }

  @Override
  public PackingPlan pack() {
    int adjustments = this.numAdjustments;
    // Get the instances using a resource compliant round robin allocation
    Optional<Map<Integer, List<InstanceId>>> resourceCompliantRRAllocation =
        getResourceCompliantRRAllocation();

    while (!resourceCompliantRRAllocation.isPresent()) {
      //The number of containers has been updated
      if (this.numAdjustments > adjustments) {
        adjustments++;
        //Invoke again the resourceCompliantRRAllocation with the updated number of containers
        resourceCompliantRRAllocation = getResourceCompliantRRAllocation();
      } else {
        //The number of containers is the same. No valid allocation was found.
        return null;
      }
    }
    // Construct the PackingPlan
    Map<String, Long> ramMap = TopologyUtils.getComponentRamMapConfig(topology);
    Set<PackingPlan.ContainerPlan> containerPlans = PackingUtils.buildContainerPlans(
        resourceCompliantRRAllocation.get(), ramMap, this.defaultInstanceResources,
        paddingPercentage);
    return new PackingPlan(topology.getId(), containerPlans);
  }

  /**
   * Get a new packing plan given an existing packing plan and component-level changes.
   *
   * @return new packing plan
   */
  public PackingPlan repack(PackingPlan currentPackingPlan, Map<String, Integer> componentChanges) {
    int adjustments = 0;
    this.numContainers = currentPackingPlan.getContainers().size();
    resetState();

    int additionalContainers = computeNumAdditionalContainers(componentChanges, currentPackingPlan);
    increaseNumContainers(additionalContainers);
    LOG.info(String.format("Allocated "
            + "%s additional containers for repack bring the number of containers to %s.",
        additionalContainers, this.numContainers));
    // Get the instances using Resource Compliant Round Robin allocation
    Optional<Map<Integer, List<InstanceId>>> resourceCompliantRRAllocation =
        getResourceCompliantRRAllocation(currentPackingPlan, componentChanges);

    while (!resourceCompliantRRAllocation.isPresent()) {
      //The number of containers has been updated
      if (this.numAdjustments > adjustments) {
        adjustments++;
        //Invoke again the resourceCompliantRRAllocation with the updated number of containers
        resourceCompliantRRAllocation = getResourceCompliantRRAllocation(currentPackingPlan,
            componentChanges);
      } else {
        //The number of containers is the same. No valid allocation was found.
        return null;
      }
    }
    // Construct the PackingPlan
    Map<String, Long> ramMap = TopologyUtils.getComponentRamMapConfig(topology);
    Set<PackingPlan.ContainerPlan> containerPlans = PackingUtils.buildContainerPlans(
        resourceCompliantRRAllocation.get(), ramMap, defaultInstanceResources, paddingPercentage);
    return new PackingPlan(topology.getId(), containerPlans);
  }

  @Override
  public void close() {
  }

  /**
   * Get the RAM requirements of all the components
   *
   * @return The list of components and their RAM requirements. Returns null if one or more
   * instances do not have valid resource requirements.
   */
  protected ArrayList<RamRequirement> getRAMInstances(Map<String, Integer> parallelismMap) {
    ArrayList<RamRequirement> ramRequirements = new ArrayList<>();
    Map<String, Long> ramMap = TopologyUtils.getComponentRamMapConfig(topology);
    for (String component : parallelismMap.keySet()) {
      if (ramMap.containsKey(component)) {
        if (!PackingUtils.isValidInstance(
            this.defaultInstanceResources.cloneWithRam(ramMap.get(component)),
            MIN_RAM_PER_INSTANCE, this.maxContainerResources, this.paddingPercentage)) {
          throw new RuntimeException("The topology configuration does not have "
              + "valid resource requirements. Please make sure that the instance resource "
              + "requirements do not exceed the maximum per-container resources.");
        } else {
          ramRequirements.add(new RamRequirement(component, ramMap.get(component)));
        }
      } else {
        if (!PackingUtils.isValidInstance(this.defaultInstanceResources,
            MIN_RAM_PER_INSTANCE, this.maxContainerResources, this.paddingPercentage)) {
          throw new RuntimeException("The topology configuration does not have "
              + "valid resource requirements. Please make sure that the instance resource "
              + "requirements do not exceed the maximum per-container resources.");
        } else {
          ramRequirements.add(
              new RamRequirement(component, this.defaultInstanceResources.getRam()));
        }
      }
    }
    return ramRequirements;
  }

  /**
   * Computes the additional number of containers needed to accommodate a scale up/down operation
   *
   * @param componentChanges parallelism changes for scale up/down
   * @param packingPlan existing packing plan
   * @return additional number of containers needed
   */
  private int computeNumAdditionalContainers(Map<String, Integer> componentChanges,
                                             PackingPlan packingPlan) {
    Resource scaleDownResource = PackingUtils.computeTotalResourceChange(topology, componentChanges,
        defaultInstanceResources, PackingUtils.ScalingDirection.DOWN);
    Resource scaleUpResource = PackingUtils.computeTotalResourceChange(topology, componentChanges,
        defaultInstanceResources, PackingUtils.ScalingDirection.UP);
    Resource additionalResource = scaleUpResource.subtractAbsolute(scaleDownResource);
    return (int) additionalResource.divideBy(packingPlan.getMaxContainerResources());
  }

  /**
   * Get the instances' allocation based on the Resource Compliant Round Robin algorithm
   *
   * @return Map &lt; containerId, list of InstanceId belonging to this container &gt;
   */
  private Optional<Map<Integer, List<InstanceId>>> getResourceCompliantRRAllocation() {
    HashMap<Integer, List<InstanceId>> allocation = new HashMap<>();
    Map<String, Integer> parallelismMap = TopologyUtils.getComponentParallelism(topology);
    ArrayList<Container> containers = new ArrayList<>();

    int totalInstance = TopologyUtils.getTotalInstance(topology);

    if (numContainers > totalInstance) {
      throw new RuntimeException("More containers allocated than instances."
          + numContainers + " allocated to host " + totalInstance + " instances.");
    }
    for (int i = 1; i <= numContainers; ++i) {
      allocation.put(i, new ArrayList<InstanceId>());
    }
    for (int i = 0; i <= numContainers - 1; i++) {
      PackingUtils.allocateNewContainer(containers, maxContainerResources, this.paddingPercentage);
    }
    if (!assignInstancesToContainers(containers, allocation, parallelismMap, 1,
        PolicyType.STRICT)) {
      //Not enough containers. Adjust the number of containers.
      LOG.info(String.format("Increasing the number of containers to "
          + "%s and attempting packing again.", this.numContainers + 1));
      adjustNumContainers(1);
      return Optional.absent();
    }
    return Optional.of((Map<Integer, List<InstanceId>>) allocation);
  }

  /**
   * Get the instances' allocation based on the ResourceCompliantRR packing algorithm
   *
   * @return Map &lt; containerId, list of InstanceId belonging to this container &gt;
   */
  private Optional<Map<Integer, List<InstanceId>>> getResourceCompliantRRAllocation(
      PackingPlan currentPackingPlan, Map<String, Integer> componentChanges) {
    Map<String, Integer> componentsToScaleDown =
        PackingUtils.getComponentsToScale(componentChanges, PackingUtils.ScalingDirection.DOWN);
    Map<String, Integer> componentsToScaleUp =
        PackingUtils.getComponentsToScale(componentChanges, PackingUtils.ScalingDirection.UP);

    ArrayList<Container> containers = PackingUtils.getContainers(currentPackingPlan,
        this.paddingPercentage);
    Map<Integer, List<InstanceId>> allocation = PackingUtils.getAllocation(currentPackingPlan);

    //Allocate additional containers.
    for (int i = containers.size() + 1; i <= numContainers; ++i) {
      allocation.put(i, new ArrayList<InstanceId>());
    }
    for (int i = containers.size(); i <= numContainers - 1; i++) {
      PackingUtils.allocateNewContainer(containers, containers.get(0).getCapacity(),
          this.paddingPercentage);
    }
    int maxInstanceIndex = 0;
    for (PackingPlan.ContainerPlan containerPlan : currentPackingPlan.getContainers()) {
      for (PackingPlan.InstancePlan instancePlan : containerPlan.getInstances()) {
        maxInstanceIndex = Math.max(maxInstanceIndex, instancePlan.getTaskId());
      }
    }
    if (!componentsToScaleDown.isEmpty()) {
      this.containerId = 1;
      removeInstancesFromContainers(containers, allocation, componentsToScaleDown);
    }
    if (!componentsToScaleUp.isEmpty()) {
      this.containerId = 1;
      if (!assignInstancesToContainers(containers, allocation, componentsToScaleUp,
          maxInstanceIndex + 1, PolicyType.FLEXIBLE)) {
        //Not enough containers. Adjust the number of containers.
        LOG.info(String.format("Increasing the number of containers to "
            + "%s and attempting packing again.", this.numContainers + 1));
        adjustNumContainers(1);
        return Optional.absent();
      }
    }
    PackingUtils.removeEmptyContainers(allocation);
    return Optional.of(allocation);
  }

  /**
   * Assigns instances to containers.
   *
   * @param containers helper data structure that describes the containers' status
   * @param allocation existing packing plan
   * @param parallelismMap component parallelism
   * @param firstTaskIndex first taskId to use for the new instances
   * @return true if the instances fit in the existing set of containers, false otherwise.
   */
  private boolean assignInstancesToContainers(
      ArrayList<Container> containers, Map<Integer, List<InstanceId>> allocation,
      Map<String, Integer> parallelismMap, int firstTaskIndex, PolicyType policyType) {
    ArrayList<RamRequirement> ramRequirements = getRAMInstances(parallelismMap);
    int globalTaskIndex = firstTaskIndex;
    int componentIndex = 0;
    for (String component : parallelismMap.keySet()) {
      long ramRequirement = ramRequirements.get(componentIndex).getRamRequirement();
      int numInstance = parallelismMap.get(component);
      for (int i = 0; i < numInstance; ++i) {
        Resource instanceResource = this.defaultInstanceResources.cloneWithRam(ramRequirement);
        boolean sufficientNumContainers = true;
        InstanceId instanceId = new InstanceId(component, globalTaskIndex, i);
        sufficientNumContainers =
            policyType.invokePolicy(allocation, containers, instanceId, instanceResource, this);
        if (!sufficientNumContainers) {
          return false;
        }
        globalTaskIndex++;
      }
      componentIndex++;
    }
    return true;
  }

  /**
   * Performs a RR placement. If the placement cannot be performed on the existing number of containers
   * then it will request for an increase in the number of containers
   *
   * @param allocation existing packing plan
   * @param containers helper data structure that describes the containers' status
   * @param instanceId the instance that needs to be placed in the container
   * @param instanceResource the resources required for that instance
   * @return true if the existing number of containers is sufficient, false otherwise
   */
  private boolean strictRRpolicy(Map<Integer, List<InstanceId>> allocation,
                                 ArrayList<Container> containers, InstanceId instanceId,
                                 Resource instanceResource) {
    if (placeResourceCompliantRRInstance(containers, containerId,
        new PackingPlan.InstancePlan(instanceId, instanceResource))) {
      allocation.get(containerId).add(instanceId);
      containerId = (containerId == numContainers) ? 1 : containerId + 1;
      return true;
    } else {
      //Automatically adjust the number of containers
      containerId = 1;
      return false;
    }
  }

  /**
   * Performs a RR placement. If the placement cannot be performed on the existing number of containers
   * then it will request for an increase in the number of containers
   *
   * @param allocation existing packing plan
   * @param containers helper data structure that describes the containers' status
   * @param instanceId the instance that needs to be placed in the container
   * @param instanceResource the resources required for that instance
   * @return true if the existing number of containers is sufficient, false otherwise
   */
  private boolean flexibleRRpolicy(Map<Integer, List<InstanceId>> allocation,
                                   ArrayList<Container> containers, InstanceId instanceId,
                                   Resource instanceResource) {
    //Attempt to place on containerId
    if (placeResourceCompliantRRInstance(containers, containerId,
        new PackingPlan.InstancePlan(instanceId, instanceResource))) {
      allocation.get(containerId).add(instanceId);
      containerId = (containerId == numContainers) ? 1 : containerId + 1;
      return true;
    } else {
      //If there is not enough space on containerId look at other containers in a RR fashion
      // starting from containerId.
      boolean containersChecked = false;
      int currentContainer = (containerId == numContainers) ? 1 : containerId + 1;
      while (!containersChecked) {
        if (placeResourceCompliantRRInstance(containers, currentContainer,
            new PackingPlan.InstancePlan(instanceId, instanceResource))) {
          allocation.get(currentContainer).add(instanceId);
          containerId = (currentContainer == numContainers) ? 1 : currentContainer + 1;
          return true;
        }
        currentContainer = (currentContainer == numContainers) ? 1 : currentContainer + 1;
        if (currentContainer == containerId) {
          containersChecked = true;
        }
      }
    }
    //Not enough containers.
    containerId = 1;
    return false;
  }

  /**
   * Removes instances from containers during scaling down
   *
   * @param containers helper data structure that describes the containers' status
   * @param allocation existing packing plan
   * @param componentsToScaleDown scale down factor for the components.
   */
  private void removeInstancesFromContainers(
      ArrayList<Container> containers, Map<Integer, List<InstanceId>> allocation,
      Map<String, Integer> componentsToScaleDown) {
    ArrayList<RamRequirement> ramRequirements = getRAMInstances(componentsToScaleDown);
    for (RamRequirement ramRequirement : ramRequirements) {
      String component = ramRequirement.getComponentName();
      int numInstancesToRemove = -componentsToScaleDown.get(component);
      for (int j = 0; j < numInstancesToRemove; j++) {
        Pair<Integer, InstanceId> idPair = removeRRInstance(containers, component);
        List<InstanceId> instances = allocation.get(idPair.first);
        instances.remove(idPair.second);
        allocation.put(idPair.first, instances);
      }
    }
  }

  /**
   * Assign a particular instance to a container with a given containerId
   *
   * @return true if the container incorporated the instance, otherwise return false
   */
  private boolean placeResourceCompliantRRInstance(ArrayList<Container> containers, int container,
                                                   PackingPlan.InstancePlan instancePlan) {
    return containers.get(container - 1).add(instancePlan);
  }

  /**
   * Remove an instance of a particular component from the containers
   *
   * @return the pairId that captures the corresponding container and instance id.
   */
  private Pair<Integer, InstanceId> removeRRInstance(ArrayList<Container> containers,
                                                     String component) throws RuntimeException {
    int currentContainer = this.containerId - 1;
    Optional<PackingPlan.InstancePlan> instancePlan =
        containers.get(currentContainer).removeAnyInstanceOfComponent(component);
    if (instancePlan.isPresent()) {
      containerId = (containerId == numContainers) ? 1 : containerId + 1;
      PackingPlan.InstancePlan plan = instancePlan.get();
      return new Pair<Integer, InstanceId>(currentContainer + 1,
          new InstanceId(plan.getComponentName(), plan.getTaskId(), plan.getComponentIndex()));
    } else {
      boolean containersChecked = false;
      currentContainer = (containerId == numContainers) ? 0 : containerId;
      while (!containersChecked) {
        instancePlan = containers.get(currentContainer).removeAnyInstanceOfComponent(component);
        if (instancePlan.isPresent()) {
          containerId = (currentContainer == numContainers - 1) ? 1 : currentContainer + 2;
          PackingPlan.InstancePlan plan = instancePlan.get();
          return new Pair<Integer, InstanceId>(currentContainer + 1,
              new InstanceId(plan.getComponentName(),
                  plan.getTaskId(), plan.getComponentIndex()));
        }
        currentContainer = (currentContainer == numContainers - 1) ? 0 : currentContainer + 1;
        if (currentContainer == containerId - 1) {
          containersChecked = true;
        }
      }
      throw new RuntimeException("Cannot remove instance."
          + " No more instances of component " + component + " exist"
          + " in the containers.");
    }
  }

  private enum PolicyType {
    STRICT, FLEXIBLE;

    private boolean invokePolicy(Map<Integer, List<InstanceId>> allocation,
                                 ArrayList<Container> containers, InstanceId instanceId,
                                 Resource instanceResource, ResourceCompliantRRPacking packing)
        throws RuntimeException {
      boolean sufficientNumContainers = true;
      switch (this) {
        case STRICT:
          sufficientNumContainers = packing.strictRRpolicy(allocation, containers,
              instanceId, instanceResource);
          return sufficientNumContainers;
        case FLEXIBLE:
          sufficientNumContainers = packing.flexibleRRpolicy(allocation, containers,
              instanceId, instanceResource);
          return sufficientNumContainers;
        default:
          throw new RuntimeException("Not valid policy type");
      }
    }
  }
}


