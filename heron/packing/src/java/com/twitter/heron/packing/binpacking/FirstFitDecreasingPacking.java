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

package com.twitter.heron.packing.binpacking;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
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

import static com.twitter.heron.api.Config.TOPOLOGY_CONTAINER_MAX_CPU_HINT;
import static com.twitter.heron.api.Config.TOPOLOGY_CONTAINER_MAX_DISK_HINT;
import static com.twitter.heron.api.Config.TOPOLOGY_CONTAINER_MAX_RAM_HINT;
import static com.twitter.heron.api.Config.TOPOLOGY_CONTAINER_PADDING_PERCENTAGE;

/**
 * FirstFitDecreasing packing algorithm
 * <p>
 * This IPacking implementation generates a PackingPlan based on the
 * First Fit Decreasing heuristic for the binpacking problem. The algorithm attempts to minimize
 * the amount of resources used.
 * <p>
 * Following semantics are guaranteed:
 * 1. Supports heterogeneous containers. The number of containers used is determined
 * by the algorithm and not by the config file.
 * The user provides a hint for the maximum container size and a padding percentage.
 * The padding percentage whose values range from [0, 100], determines the per container
 * resources allocated for system-related processes (e.g., the stream manager).
 * <p>
 * 2. The user provides a hint for the maximum CPU, RAM and Disk that can be used by each container through
 * the com.twitter.heron.api.Config.TOPOLOGY_CONTAINER_MAX_CPU_HINT,
 * com.twitter.heron.api.Config.TOPOLOGY_CONTAINER_MAX_RAM_HINT,
 * com.twitter.heron.api.Config.TOPOLOGY_CONTAINER_MAX_DISK_HINT parameters.
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
public class FirstFitDecreasingPacking implements IPacking, IRepacking {

  private static final long MIN_RAM_PER_INSTANCE = 192L * Constants.MB;
  private static final int DEFAULT_CONTAINER_PADDING_PERCENTAGE = 10;
  private static final int DEFAULT_NUMBER_INSTANCES_PER_CONTAINER = 4;

  private static final Logger LOG = Logger.getLogger(FirstFitDecreasingPacking.class.getName());
  private TopologyAPI.Topology topology;

  private Resource defaultInstanceResources;
  private Resource maxContainerResources;

  private int paddingPercentage;

  @Override
  public void initialize(Config config, TopologyAPI.Topology inputTopology) {
    this.topology = inputTopology;
    setPackingConfigs(config);
  }

  /**
   * Instatiate the packing algorithm parameters related to this topology.
   */
  private void setPackingConfigs(Config config) {
    List<TopologyAPI.Config.KeyValue> topologyConfig = topology.getTopologyConfig().getKvsList();

    this.defaultInstanceResources = new Resource(
        Context.instanceCpu(config),
        Context.instanceRam(config),
        Context.instanceDisk(config));

    this.paddingPercentage = TopologyUtils.getConfigWithDefault(topologyConfig,
        TOPOLOGY_CONTAINER_PADDING_PERCENTAGE, DEFAULT_CONTAINER_PADDING_PERCENTAGE);

    double defaultCpu = this.defaultInstanceResources.getCpu()
        * DEFAULT_NUMBER_INSTANCES_PER_CONTAINER;
    long defaultRam = this.defaultInstanceResources.getRam()
        * DEFAULT_NUMBER_INSTANCES_PER_CONTAINER;
    long defaultDisk = this.defaultInstanceResources.getDisk()
        * DEFAULT_NUMBER_INSTANCES_PER_CONTAINER;

    this.maxContainerResources = new Resource(
        TopologyUtils.getConfigWithDefault(topologyConfig, TOPOLOGY_CONTAINER_MAX_CPU_HINT,
            (double) Math.round(PackingUtils.increaseBy(defaultCpu, paddingPercentage))),
        TopologyUtils.getConfigWithDefault(topologyConfig, TOPOLOGY_CONTAINER_MAX_RAM_HINT,
            PackingUtils.increaseBy(defaultRam, paddingPercentage)),
        TopologyUtils.getConfigWithDefault(topologyConfig, TOPOLOGY_CONTAINER_MAX_DISK_HINT,
            PackingUtils.increaseBy(defaultDisk, paddingPercentage)));
  }

  /**
   * Get a packing plan using First Fit Decreasing
   *
   * @return packing plan
   */
  @Override
  public PackingPlan pack() {
    // Get the instances using FFD allocation
    Map<Integer, List<InstanceId>> ffdAllocation = getFFDAllocation();
    // Construct the PackingPlan
    Map<String, Long> ramMap = TopologyUtils.getComponentRamMapConfig(topology);

    Set<PackingPlan.ContainerPlan> containerPlans = PackingUtils.buildContainerPlans(
        ffdAllocation, ramMap, this.defaultInstanceResources, this.paddingPercentage);

    return new PackingPlan(topology.getId(), containerPlans);
  }

  /**
   * Get a new packing plan given an existing packing plan and component-level changes.
   * @return new packing plan
   */
  public PackingPlan repack(PackingPlan currentPackingPlan, Map<String, Integer> componentChanges) {
    // Get the instances using FFD allocation
    Map<Integer, List<InstanceId>> ffdAllocation =
        getFFDAllocation(currentPackingPlan, componentChanges);

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
   * Sort the components in decreasing order based on their RAM requirements
   *
   * @return The sorted list of components and their RAM requirements
   */
  private ArrayList<RamRequirement> getSortedRAMInstances(Map<String, Integer> parallelismMap) {
    ArrayList<RamRequirement> ramRequirements = new ArrayList<>();
    Map<String, Long> ramMap = TopologyUtils.getComponentRamMapConfig(topology);

    for (String component : parallelismMap.keySet()) {
      if (ramMap.containsKey(component)) {
        if (!PackingUtils.isValidInstance(
            this.defaultInstanceResources.cloneWithRam(ramMap.get(component)),
            MIN_RAM_PER_INSTANCE, maxContainerResources, this.paddingPercentage)) {
          throw new RuntimeException("The topology configuration does not have "
              + "valid resource requirements. Please make sure that the instance resource "
              + "requirements do not exceed the maximum per-container resources.");
        } else {
          ramRequirements.add(new RamRequirement(component, ramMap.get(component)));
        }
      } else {
        if (!PackingUtils.isValidInstance(this.defaultInstanceResources,
            MIN_RAM_PER_INSTANCE, maxContainerResources, this.paddingPercentage)) {
          throw new RuntimeException("The topology configuration does not have "
              + "valid resource requirements. Please make sure that the instance resource "
              + "requirements do not exceed the maximum per-container resources.");
        } else {
          ramRequirements.add(
              new RamRequirement(component, this.defaultInstanceResources.getRam()));
        }
      }
    }
    Collections.sort(ramRequirements, Collections.reverseOrder());

    return ramRequirements;
  }

  /**
   * Get the instances' allocation based on the First Fit Decreasing algorithm
   *
   * @return Map &lt; containerId, list of InstanceId belonging to this container &gt;
   */
  private Map<Integer, List<InstanceId>> getFFDAllocation() {
    Map<String, Integer> parallelismMap = TopologyUtils.getComponentParallelism(topology);
    HashMap<Integer, List<InstanceId>> allocation = new HashMap<>();
    assignInstancesToContainers(new ArrayList<Container>(), allocation, parallelismMap, 1,
        maxContainerResources);
    return allocation;
  }

  /**
   * Get the instances' allocation based on the First Fit Decreasing algorithm
   *
   * @return Map &lt; containerId, list of InstanceId belonging to this container &gt;
   */
  private Map<Integer, List<InstanceId>> getFFDAllocation(PackingPlan currentPackingPlan,
                                                          Map<String, Integer> componentChanges) {
    Map<String, Integer> componentsToScaleDown =
        PackingUtils.getComponentsToScale(componentChanges, PackingUtils.ScalingDirection.DOWN);
    Map<String, Integer> componentsToScaleUp =
        PackingUtils.getComponentsToScale(componentChanges, PackingUtils.ScalingDirection.UP);

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
          maxInstanceIndex + 1, containers.get(0).getCapacity());
    }
    removeEmptyContainers(allocation);
    return allocation;
  }

  /**
   * Removes containers from tha allocation that do not contain any instances
   */
  private void removeEmptyContainers(Map<Integer, List<InstanceId>> allocation) {
    Iterator<Integer> containerIds = allocation.keySet().iterator();
    while (containerIds.hasNext()) {
      Integer containerId = containerIds.next();
      if (allocation.get(containerId).isEmpty()) {
        containerIds.remove();
      }
    }
  }

  /**
   * Assigns instances to containers
   *
   * @param containers helper data structure that describes the containers' status
   * @param allocation existing packing plan
   * @param parallelismMap component parallelism
   * @param firstTaskIndex first taskId to use for the new instances
   */
  private void assignInstancesToContainers(
      ArrayList<Container> containers, Map<Integer, List<InstanceId>> allocation,
      Map<String, Integer> parallelismMap, int firstTaskIndex, Resource containerCapacity) {
    ArrayList<RamRequirement> ramRequirements = getSortedRAMInstances(parallelismMap);
    int globalTaskIndex = firstTaskIndex;
    for (RamRequirement ramRequirement : ramRequirements) {
      String component = ramRequirement.getComponentName();
      int numInstance = parallelismMap.get(component);
      for (int j = 0; j < numInstance; j++) {
        Resource instanceResource =
            this.defaultInstanceResources.cloneWithRam(ramRequirement.getRamRequirement());
        int containerId = placeFFDInstance(containers, new PackingPlan.InstancePlan(new InstanceId(
            component, globalTaskIndex, j), instanceResource), containerCapacity);
        List<InstanceId> instances = allocation.get(containerId);
        if (instances == null) {
          instances = new ArrayList<>();
        }
        instances.add(new InstanceId(component, globalTaskIndex++, j));
        allocation.put(containerId, instances);
      }
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

    ArrayList<RamRequirement> ramRequirements = getSortedRAMInstances(componentsToScaleDown);
    for (RamRequirement ramRequirement : ramRequirements) {
      String component = ramRequirement.getComponentName();
      int numInstancesToRemove = -componentsToScaleDown.get(component);
      for (int j = 0; j < numInstancesToRemove; j++) {
        Pair<Integer, InstanceId> idPair = removeFFDInstance(containers, component);
        List<InstanceId> instances = allocation.get(idPair.first);
        instances.remove(idPair.second);
        allocation.put(idPair.first, instances);
      }
    }
  }

  /**
   * Assign a particular instance to an existing container or to a new container
   *
   * @return the container Id that incorporated the instance
   */
  private int placeFFDInstance(ArrayList<Container> containers,
                               PackingPlan.InstancePlan instancePlan,
                               Resource containerCapacity) {
    boolean placed = false;
    int containerId = 0;
    for (int i = 0; i < containers.size() && !placed; i++) {
      if (containers.get(i).add(instancePlan)) {
        placed = true;
        containerId = i + 1;
      }
    }
    if (!placed) {
      containerId = PackingUtils.allocateNewContainer(containers, containerCapacity,
          this.paddingPercentage);
      containers.get(containerId - 1).add(instancePlan);
    }
    return containerId;
  }

  /**
   * Remove an instance of a particular component from the containers
   *
   * @return the pairId that captures the corresponding container and instance id.
   */
  private Pair<Integer, InstanceId> removeFFDInstance(ArrayList<Container> containers,
                                                      String component)
      throws RuntimeException {
    boolean removed = false;
    int containerId = 0;
    for (int i = 0; i < containers.size() && !removed; i++) {
      Optional<PackingPlan.InstancePlan> instancePlan =
          containers.get(i).removeAnyInstanceOfComponent(component);
      if (instancePlan.isPresent()) {
        removed = true;
        containerId = i + 1;
        PackingPlan.InstancePlan plan = instancePlan.get();
        return new Pair<Integer, InstanceId>(containerId, new InstanceId(plan.getComponentName(),
            plan.getTaskId(), plan.getComponentIndex()));
      }
    }
    throw new RuntimeException("Cannot remove instance."
        + " No more instances of component " + component + " exist"
        + " in the containers.");
  }
}
