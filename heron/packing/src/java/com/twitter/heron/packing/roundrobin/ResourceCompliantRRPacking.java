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

  private int paddingPercentage;

  private void increaseNumContainers() {
    this.numContainers++;
    this.numAdjustments++;
  }

  @Override
  public void initialize(Config config, TopologyAPI.Topology inputTopology) {
    this.topology = inputTopology;
    this.numContainers = TopologyUtils.getNumContainers(topology);
    this.defaultInstanceResources = new Resource(
        Context.instanceCpu(config),
        Context.instanceRam(config),
        Context.instanceDisk(config));
    this.numAdjustments = 0;


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
    Optional<Map<Integer, List<InstanceId>>> containerInstances =
        getResourceCompliantRRAllocation();

    while (!containerInstances.isPresent()) {
      if (this.numAdjustments > adjustments) {
        adjustments++;
        containerInstances = getResourceCompliantRRAllocation();
      } else {
        return null;
      }
    }
    // Construct the PackingPlan
    Map<String, Long> ramMap = TopologyUtils.getComponentRamMapConfig(topology);

    Set<PackingPlan.ContainerPlan> containerPlans = PackingUtils.buildContainerPlans(
        containerInstances.get(), ramMap, this.defaultInstanceResources, paddingPercentage);
    return new PackingPlan(topology.getId(), containerPlans);
  }

  /**
   * Get a new packing plan given an existing packing plan and component-level changes.
   * @return new packing plan
   */
  public PackingPlan repack(PackingPlan currentPackingPlan, Map<String, Integer> componentChanges) {
    // Get the instances using Resource Compliant Round Robin allocation
    Map<Integer, List<InstanceId>> ffdAllocation =
        getResourceCompliantRRAllocation(currentPackingPlan, componentChanges);

    // Construct the PackingPlan
    Map<String, Long> ramMap = TopologyUtils.getComponentRamMapConfig(topology);

    Set<PackingPlan.ContainerPlan> containerPlans = PackingUtils.buildContainerPlans(
        ffdAllocation, ramMap, defaultInstanceResources, paddingPercentage);
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
    int noContainers = assignInstancesToContainers(containers, allocation, parallelismMap, 1);
    if (noContainers > this.numContainers) {
      increaseNumContainers();
      return Optional.absent();
    }
    return Optional.of((Map<Integer, List<InstanceId>>) allocation);
  }


  /**
   * Get the instances' allocation based on the ResourceCompliantRR packing algorithm
   *
   * @return Map &lt; containerId, list of InstanceId belonging to this container &gt;
   */

  private Map<Integer, List<InstanceId>> getResourceCompliantRRAllocation(
      PackingPlan currentPackingPlan, Map<String, Integer> componentChanges) {
    Map<String, Integer> componentsToScaleDown =
        PackingUtils.getComponentsToScaleDown(componentChanges);
    Map<String, Integer> componentsToScaleUp =
        PackingUtils.getComponentsToScaleUp(componentChanges);

    ArrayList<Container> containers = PackingUtils.getContainers(currentPackingPlan,
        this.paddingPercentage);
    Map<Integer, List<InstanceId>> allocation = PackingUtils.getAllocation(currentPackingPlan);
    int maxInstanceIndex = 0;
    for (PackingPlan.ContainerPlan containerPlan : currentPackingPlan.getContainers()) {
      for (PackingPlan.InstancePlan instancePlan : containerPlan.getInstances()) {
        maxInstanceIndex = Math.max(maxInstanceIndex, instancePlan.getTaskId());
      }
    }
    if (!componentsToScaleDown.isEmpty()) {
      removeInstancesFromContainers(containers, allocation, componentsToScaleDown);
    }
    if (!componentsToScaleUp.isEmpty()) {
      assignInstancesToContainers(containers, allocation, componentsToScaleUp,
          maxInstanceIndex + 1);
    }
    PackingUtils.removeEmptyContainers(allocation);
    return allocation;
  }

  /**
   * Assigns instances to containers
   *
   * @param containers helper data structure that describes the containers' status
   * @param allocation existing packing plan
   * @param parallelismMap component parallelism
   * @param firstTaskIndex first taskId to use for the new instances
   */
  private int assignInstancesToContainers(
      ArrayList<Container> containers, Map<Integer, List<InstanceId>> allocation,
      Map<String, Integer> parallelismMap, int firstTaskIndex) {
    ArrayList<RamRequirement> ramRequirements = getRAMInstances(parallelismMap);
    int containerId = 1;
    int globalTaskIndex = 1;
    int componentIndex = 0;
    for (String component : parallelismMap.keySet()) {
      long ramRequirement = ramRequirements.get(componentIndex).getRamRequirement();
      int numInstance = parallelismMap.get(component);
      for (int i = 0; i < numInstance; ++i) {
        Resource instanceResource = this.defaultInstanceResources.cloneWithRam(ramRequirement);
        int noContainers = strictRRpolicy(allocation, containers, containerId,
            new InstanceId(component, globalTaskIndex, i), instanceResource);
        if (noContainers > this.numContainers) {
          return noContainers;
        }
        containerId = (containerId == numContainers) ? 1 : containerId + 1;
        globalTaskIndex++;
      }
      componentIndex++;
    }
    return this.numContainers;
  }

  /**
   * Performs a RR placement. If the placement cannot be performed on the existing number of containers
   * then it will request for an increase in the number of containers
   * @param allocation existing packing plan
   * @param containers helper data structure that describes the containers' status
   * @param containerId id of the container where the instance needs to be placed
   * @param instanceId the instance that needs to be placed in the container
   * @param instanceResource the resources required for that instance
   * @return number of containers needed for a valid RR allocation
   */
  private int strictRRpolicy(Map<Integer, List<InstanceId>> allocation,
                             ArrayList<Container> containers, int containerId,
                             InstanceId instanceId, Resource instanceResource) {
    int noContainers = this.numContainers;
    if (placeResourceCompliantRRInstance(containers, containerId,
        new PackingPlan.InstancePlan(instanceId, instanceResource))) {
      allocation.get(containerId).add(instanceId);
      return noContainers;
    } else {
      //Automatically adjust the number of containers
      noContainers++;
      LOG.info(String.format("Increasing the number of containers to "
          + "%s and attempting packing again.", this.numContainers));
      return noContainers;
    }
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

   /* ArrayList<RamRequirement> ramRequirements = getRAMInstances(componentsToScaleDown);
    for (RamRequirement ramRequirement : ramRequirements) {
      String component = ramRequirement.getComponentName();
      int numInstancesToRemove = -componentsToScaleDown.get(component);
      for (int j = 0; j < numInstancesToRemove; j++) {
        Pair<Integer, InstanceId> idPair = removeRRInstance(containers, component);
        List<InstanceId> instances = allocation.get(idPair.first);
        instances.remove(idPair.second);
        allocation.put(idPair.first, instances);
      }
    }*/
  }
  /**
   * Assign a particular instance to a container with a given containerId
   *
   * @return true if the container incorporated the instance, otherwise return false
   */
  private boolean placeResourceCompliantRRInstance(ArrayList<Container> containers, int containerId,
                                                   PackingPlan.InstancePlan instancePlan) {
    return containers.get(containerId - 1).add(instancePlan);
  }
}


