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
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.packing.Container;
import com.twitter.heron.packing.RamRequirement;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Constants;
import com.twitter.heron.spi.common.Context;
import com.twitter.heron.spi.packing.IPacking;
import com.twitter.heron.spi.packing.IRepacking;
import com.twitter.heron.spi.packing.PackingPlan;
import com.twitter.heron.spi.packing.Resource;
import com.twitter.heron.spi.utils.TopologyUtils;

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
 * The padding percentage whose values range from [0, 100], determines the additional per container
 * resources allocated for system-related processes (e.g., the stream manager).
 * The algorithm might produce containers whose size is slightly higher than
 * the maximum container size provided by the user depending on the per-container instance allocation
 * and the padding configuration.
 * <p>
 * 2. The user provides a hint for the maximum CPU, RAM and Disk that can be used by each container through
 * the com.twitter.heron.api.Config.TOPOLOGY_CONTAINER_MAX_CPU_HINT,
 * com.twitter.heron.api.Config.TOPOLOGY_CONTAINER_MAX_RAM_HINT,
 * com.twitter.heron.api.Config.TOPOLOGY_CONTAINER_MAX_DISK_HINT parameters.
 * If the parameters are not specified then a default value is used for the maximum container
 * size.
 * <p>
 * 3. The user provides a percentage of each container size that will be added to the resources
 * allocated by the container through the com.twitter.heron.api.Config.TOPOLOGY_CONTAINER_PADDING_PERCENTAGE
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
 * 10. The size of resources required by the whole topology is equal to
 * ((# containers used by FFD)* size of each container
 * + size of a container that includes one instance with default resource requirements).
 * We add some resources to consider the Heron internal container (i.e. the one containing Scheduler
 * and TMaster).
 * <p>
 * 11. The pack() return null if PackingPlan fails to pass the safe check, for instance,
 * the size of ram for an instance is less than the minimal required value or .
 */

public class FirstFitDecreasingPacking implements IPacking, IRepacking {

  public static final long MIN_RAM_PER_INSTANCE = 192L * Constants.MB;
  public static final int DEFAULT_CONTAINER_PADDING_PERCENTAGE = 10;
  public static final int DEFAULT_NUMBER_INSTANCES_PER_CONTAINER = 4;

  private static final Logger LOG = Logger.getLogger(FirstFitDecreasingPacking.class.getName());
  protected TopologyAPI.Topology topology;

  protected long instanceRamDefault;
  protected double instanceCpuDefault;
  protected long instanceDiskDefault;

  protected long maxContainerRam;
  protected double maxContainerCpu;
  protected long maxContainerDisk;

  protected int paddingPercentage;

  public static String getContainerId(int index) {
    return Integer.toString(index);
  }

  public static String getInstanceId(
      int containerIdx, String componentName, int instanceIdx, int componentIdx) {
    return String.format("%d:%s:%d:%d", containerIdx, componentName, instanceIdx, componentIdx);
  }

  public static String getComponentName(String instanceId) {
    return instanceId.split(":")[1];
  }

  @Override
  public void initialize(Config config, TopologyAPI.Topology inputTopology) {
    this.topology = inputTopology;
    setPackingConfigs(config);
  }

  /**
   * Instatiate the packing algorithm parameters related to this topology.
   */
  private void setPackingConfigs(Config config) {
    this.instanceRamDefault = Context.instanceRam(config);
    this.instanceCpuDefault = Context.instanceCpu(config).doubleValue();
    this.instanceDiskDefault = Context.instanceDisk(config);

    List<TopologyAPI.Config.KeyValue> topologyConfig = topology.getTopologyConfig().getKvsList();
    this.maxContainerRam = Long.parseLong(TopologyUtils.getConfigWithDefault(
        topologyConfig, com.twitter.heron.api.Config.TOPOLOGY_CONTAINER_MAX_RAM_HINT,
        Long.toString(instanceRamDefault * DEFAULT_NUMBER_INSTANCES_PER_CONTAINER)));

    this.maxContainerCpu = Double.parseDouble(TopologyUtils.getConfigWithDefault(
        topologyConfig, com.twitter.heron.api.Config.TOPOLOGY_CONTAINER_MAX_CPU_HINT,
        Double.toString(instanceCpuDefault * DEFAULT_NUMBER_INSTANCES_PER_CONTAINER)));

    this.maxContainerDisk = Long.parseLong(TopologyUtils.getConfigWithDefault(
        topologyConfig, com.twitter.heron.api.Config.TOPOLOGY_CONTAINER_MAX_DISK_HINT,
        Long.toString(instanceDiskDefault * DEFAULT_NUMBER_INSTANCES_PER_CONTAINER)));

    this.paddingPercentage = Integer.parseInt(TopologyUtils.getConfigWithDefault(
        topologyConfig, com.twitter.heron.api.Config.TOPOLOGY_CONTAINER_PADDING_PERCENTAGE,
        Integer.toString(DEFAULT_CONTAINER_PADDING_PERCENTAGE)));
  }

  /**
   * Get a packing plan using First Fit Decreasing
   *
   * @return packing plan
   */
  @Override
  public PackingPlan pack() {
    // Get the instances using FFD allocation
    Map<String, List<String>> ffdAllocation = getFFDAllocation();
    // Construct the PackingPlan
    Map<String, PackingPlan.ContainerPlan> containerPlanMap = new HashMap<>();
    Map<String, Long> ramMap = TopologyUtils.getComponentRamMapConfig(topology);

    Resource resource = estimateResources(ffdAllocation, containerPlanMap, ramMap, false);

    PackingPlan plan = new PackingPlan(topology.getId(), containerPlanMap, resource);
    return plan;
  }

  /**
   * Get a new packing plan given an existing packing plan and component-level changes
   *
   * @return new packing plan
   */
  public PackingPlan repack(PackingPlan currentPackingPlan, Map<String, Integer> componentChanges) {
    // Get the instances using FFD allocation
    Map<String, List<String>> ffdAllocation = getFFDAllocation(currentPackingPlan,
        componentChanges);

    // Construct the PackingPlan
    Map<String, PackingPlan.ContainerPlan> containerPlanMap = new HashMap<>();
    Map<String, Long> ramMap = TopologyUtils.getComponentRamMapConfig(topology);

    Resource resource = estimateResources(ffdAllocation, containerPlanMap, ramMap, true);

    //merge the two plans
    Map<String, PackingPlan.ContainerPlan> totalContainerPlanMap = new HashMap<>();
    totalContainerPlanMap.putAll(containerPlanMap);
    totalContainerPlanMap.putAll(currentPackingPlan.getContainers());

    Resource total = new Resource(
        resource.getCpu() + currentPackingPlan.getResource().getCpu(),
        resource.getRam() + currentPackingPlan.getResource().getRam(),
        resource.getDisk() + currentPackingPlan.getResource().getDisk());

    PackingPlan plan = new PackingPlan(topology.getId(), totalContainerPlanMap, total);

    LOG.info("Created a packing plan with " + containerPlanMap.size() + " containers");
    for (String key : containerPlanMap.keySet()) {
      LOG.info("Container  " + key + " consists of "
          + containerPlanMap.get(key).instances.toString());
    }
    LOG.info("Topology Resources " + resource.toString());
    return plan;
  }

  @Override
  public void close() {

  }

  /**
   * Estimate the per instance and topology resources for the packing plan created using
   * FFD placement
   *
   * @return Resources required
   */
  private Resource estimateResources(Map<String, List<String>> ffdAllocation,
                                     Map<String, PackingPlan.ContainerPlan> containerPlanMap,
                                     Map<String, Long> ramMap,
                                     boolean scale) {
    long topologyRam = 0;
    long topologyDisk = 0;
    double topologyCpu = 0.0;

    for (Map.Entry<String, List<String>> entry : ffdAllocation.entrySet()) {

      String containerId = entry.getKey();
      List<String> instanceList = entry.getValue();

      long containerRam = 0;
      long containerDiskInBytes = 0;
      double containerCpu = 0;

      // Calculate the resource required for single instance
      Map<String, PackingPlan.InstancePlan> instancePlanMap = new HashMap<>();

      for (String instanceId : instanceList) {
        long instanceRam = 0;
        if (ramMap.containsKey(getComponentName(instanceId))) {
          instanceRam = ramMap.get(getComponentName(instanceId));
        } else {
          instanceRam = instanceRamDefault;
        }
        containerRam += instanceRam;

        // Currently not yet support disk or cpu config for different components,
        // so just use the default value.
        long instanceDisk = instanceDiskDefault;
        containerDiskInBytes += instanceDisk;

        double instanceCpu = instanceCpuDefault;
        containerCpu += instanceCpu;

        Resource resource =
            new Resource(instanceCpu, instanceRam, instanceDisk);
        PackingPlan.InstancePlan instancePlan =
            new PackingPlan.InstancePlan(
                instanceId,
                getComponentName(instanceId),
                resource);
        // Insert it into the map
        instancePlanMap.put(instanceId, instancePlan);
      }

      containerCpu += (paddingPercentage * containerCpu) / 100;
      containerRam += (paddingPercentage * containerRam) / 100;
      containerDiskInBytes += (paddingPercentage * containerDiskInBytes) / 100;

      Resource resource =
          new Resource(Math.round(containerCpu), containerRam, containerDiskInBytes);

      PackingPlan.ContainerPlan containerPlan =
          new PackingPlan.ContainerPlan(containerId, instancePlanMap, resource);

      containerPlanMap.put(containerId, containerPlan);
      topologyRam += containerRam;
      topologyCpu += Math.round(containerCpu);
      topologyDisk += containerDiskInBytes;
    }

    // Take the heron internal container into account and the application master for YARN
    // scheduler
    if (!scale) {
      topologyRam += instanceRamDefault;
      topologyDisk += instanceDiskDefault;
      topologyCpu += instanceCpuDefault;
    }

    return new Resource(topologyCpu, topologyRam, topologyDisk);
  }

  /**
   * Sort the components in decreasing order based on their RAM requirements
   *
   * @return The sorted list of components and their RAM requirements
   */
  protected ArrayList<RamRequirement> getSortedRAMInstances(Map<String, Integer> parallelismMap) {
    ArrayList<RamRequirement> ramRequirements = new ArrayList<>();
    Map<String, Long> ramMap = TopologyUtils.getComponentRamMapConfig(topology);

    for (String component : parallelismMap.keySet()) {
      if (ramMap.containsKey(component)) {
        if (!isValidInstance(new Resource(instanceCpuDefault,
            ramMap.get(component), instanceDiskDefault))) {
          throw new RuntimeException("The topology configuration does not have "
              + "valid resource requirements. Please make sure that the instance resource "
              + "requirements do not exceed the maximum per-container resources.");
        } else {
          ramRequirements.add(new RamRequirement(component, ramMap.get(component)));
        }
      } else {
        if (!isValidInstance(new Resource(instanceCpuDefault,
            instanceRamDefault, instanceDiskDefault))) {
          throw new RuntimeException("The topology configuration does not have "
              + "valid resource requirements. Please make sure that the instance resource "
              + "requirements do not exceed the maximum per-container resources.");
        } else {
          ramRequirements.add(new RamRequirement(component, instanceRamDefault));
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
  protected Map<String, List<String>> getFFDAllocation() {
    Map<String, Integer> parallelismMap = TopologyUtils.getComponentParallelism(topology);
    return placeInstances(parallelismMap, 0);
  }

  /**
   * Get the instances' allocation based on the First Fit Decreasing algorithm
   *
   * @return Map &lt; containerId, list of InstanceId belonging to this container &gt;
   */
  protected Map<String, List<String>> getFFDAllocation(PackingPlan currentPackingPlan,
                                                       Map<String, Integer> componentChanges) {
    int numContainers = currentPackingPlan.getContainers().size();
    Map<String, Integer> parallelismMap = componentChanges;
    return placeInstances(parallelismMap, numContainers);
  }

  /**
   * Place a set of instances into the containers using the FFD heuristic
   *
   * @return true if a placement was found, false otherwise
   */
  private Map<String, List<String>> placeInstances(Map<String, Integer> parallelismMap,
                                                   int numContainers) {
    Map<String, List<String>> allocation = new HashMap<>();
    ArrayList<Container> containers = new ArrayList<>();
    ArrayList<RamRequirement> ramRequirements = getSortedRAMInstances(parallelismMap);
    int globalTaskIndex = 1;

    for (int i = 0; i < ramRequirements.size(); i++) {
      String component = ramRequirements.get(i).getComponentName();
      int numInstance = parallelismMap.get(component);
      for (int j = 0; j < numInstance; j++) {
        int containerId = placeFFDInstance(containers,
            ramRequirements.get(i).getRamRequirement(),
            instanceCpuDefault, instanceDiskDefault);
        if (allocation.containsKey(getContainerId(containerId + numContainers))) {
          allocation.get(getContainerId(containerId + numContainers)).
              add(getInstanceId(containerId + numContainers, component, globalTaskIndex, j));
        } else {
          ArrayList<String> instance = new ArrayList<>();
          instance.add(getInstanceId(containerId + numContainers, component, globalTaskIndex, j));
          allocation.put(getContainerId(containerId + numContainers), instance);
        }
        globalTaskIndex++;
      }
    }
    return allocation;
  }

  /**
   * Assign a particular instance to an existing container or to a new container
   *
   * @return the container Id that incorporated the instance
   */
  public int placeFFDInstance(ArrayList<Container> containers, long ramRequirement,
                              double cpuRequirement, long diskRequirement) {
    boolean placed = false;
    int containerId = 0;
    for (int i = 0; i < containers.size() && !placed; i++) {
      if (containers.get(i).add(ramRequirement, cpuRequirement, diskRequirement)) {
        placed = true;
        containerId = i + 1;
      }
    }
    if (!placed) {
      containerId = allocateNewContainer(containers);
      containers.get(containerId - 1).add(ramRequirement, cpuRequirement, diskRequirement);
    }
    return containerId;
  }

  /**
   * Allocate a new container taking into account the maximum resource requirements specified
   * in the config
   *
   * @return the number of containers
   */
  private int allocateNewContainer(ArrayList<Container> containers) {
    containers.add(new Container(maxContainerRam, maxContainerCpu, maxContainerDisk));
    return containers.size();
  }

  /**
   * Check whether the Instance has enough RAM and whether it can fit within the container limits.
   *
   * @param instanceResources The resources allocated to the instance
   * @return true if the instance is valid, false otherwise
   */
  protected boolean isValidInstance(Resource instanceResources) {

    if (instanceResources.getRam() < MIN_RAM_PER_INSTANCE) {
      LOG.severe(String.format(
          "Require at least %d MB ram per instance",
          MIN_RAM_PER_INSTANCE / Constants.MB));
      return false;
    }

    if (instanceResources.getRam() > maxContainerRam) {
      LOG.severe(String.format(
          "This instance requires containers of at least %d MB ram. The current max container"
              + "size is %d MB",
          instanceResources.getRam(), maxContainerRam));
      return false;
    }

    if (instanceResources.getCpu() > maxContainerCpu) {
      LOG.severe(String.format(
          "This instance requires containers with at least %d cpu cores. The current max container"
              + "size is %d cores",
          instanceResources.getCpu(), maxContainerCpu));
      return false;
    }

    if (instanceResources.getDisk() > maxContainerDisk) {
      LOG.severe(String.format(
          "This instance requires containers of at least %d MB disk. The current max container"
              + "size is %d MB",
          instanceResources.getDisk(), maxContainerDisk));
      return false;
    }
    return true;
  }
}
