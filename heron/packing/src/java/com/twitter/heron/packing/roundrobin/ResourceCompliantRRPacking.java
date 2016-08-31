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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.packing.Container;
import com.twitter.heron.packing.RamRequirement;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Constants;
import com.twitter.heron.spi.common.Context;
import com.twitter.heron.spi.packing.IPacking;
import com.twitter.heron.spi.packing.PackingPlan;
import com.twitter.heron.spi.packing.Resource;
import com.twitter.heron.spi.utils.TopologyUtils;

/**
 * ResourceCompliantRoundRobin packing algorithm
 * <p>
 * This IPacking implementation generates a PackingPlan using a round robin algorithm.
 * <p>
 * Following semantics are guaranteed:
 * 1. Supports heterogeneous containers.
 * The user provides the number of containers to use as well as
 * the maximum container size and a padding percentage.
 * The padding percentage whose values range from [0, 100], determines the additional per container
 * resources allocated for system-related processes (e.g., the stream manager).
 * The algorithm might produce containers whose size is slightly higher than
 * the maximum container size provided by the user depending on the per-container instance allocation
 * and the padding configuration.
 * <p>
 * 2. The user provides the maximum CPU, RAM and Disk that can be used by each container through
 * the com.twitter.heron.api.Config.TOPOLOGY_CONTAINER_CPU_REQUESTED,
 * com.twitter.heron.api.Config.TOPOLOGY_CONTAINER_RAM_REQUESTED,
 * com.twitter.heron.api.Config.TOPOLOGY_CONTAINER_DISK_REQUESTED parameters.
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
 * ((# containers used by ResourceCompliantRR)* size of each container
 * + size of a container that includes one instance with default resource requirements).
 * We add some resources to consider the Heron internal container (i.e. the one containing Scheduler
 * and TMaster).
 * <p>
 * 11. The pack() return null if PackingPlan fails to pass the safe check, for instance,
 * the size of ram for an instance is less than the minimal required value or .
 */
public class ResourceCompliantRRPacking implements IPacking {

  public static final long MIN_RAM_PER_INSTANCE = 192L * Constants.MB;
  public static final int DEFAULT_CONTAINER_PADDING_PERCENTAGE = 10;
  public static final int DEFAULT_NUMBER_INSTANCES_PER_CONTAINER = 4;

  private static final Logger LOG = Logger.getLogger(ResourceCompliantRRPacking.class.getName());
  protected TopologyAPI.Topology topology;

  protected long instanceRamDefault;
  protected double instanceCpuDefault;
  protected long instanceDiskDefault;
  protected long maxContainerRam;
  protected double maxContainerCpu;
  protected long maxContainerDisk;
  protected int numContainers;
  protected int numAdjustments;

  public static String getContainerId(int index) {
    return "" + index;
  }

  public static String getInstanceId(
      int containerIdx, String componentName, int instanceIdx, int componentIdx) {
    return String.format("%d:%s:%d:%d", containerIdx, componentName, instanceIdx, componentIdx);
  }

  public static String getComponentName(String instanceId) {
    return instanceId.split(":")[1];
  }

  public void increasenumContainers() {
    this.numContainers++;
    this.numAdjustments++;
  }

  @Override
  public void initialize(Config config, TopologyAPI.Topology inputTopology) {
    this.topology = inputTopology;
    this.numContainers = TopologyUtils.getNumContainers(topology);
    this.instanceRamDefault = Context.instanceRam(config);
    this.instanceCpuDefault = Context.instanceCpu(config).doubleValue();
    this.instanceDiskDefault = Context.instanceDisk(config);
    this.numAdjustments = 0;

    List<TopologyAPI.Config.KeyValue> topologyConfig = topology.getTopologyConfig().getKvsList();
    this.maxContainerRam = Long.parseLong(TopologyUtils.getConfigWithDefault(
        topologyConfig, com.twitter.heron.api.Config.TOPOLOGY_CONTAINER_RAM_REQUESTED,
        Long.toString(instanceRamDefault * DEFAULT_NUMBER_INSTANCES_PER_CONTAINER)));

    this.maxContainerCpu = Double.parseDouble(TopologyUtils.getConfigWithDefault(
        topologyConfig, com.twitter.heron.api.Config.TOPOLOGY_CONTAINER_CPU_REQUESTED,
        Double.toString(instanceCpuDefault * DEFAULT_NUMBER_INSTANCES_PER_CONTAINER)));

    this.maxContainerDisk = Long.parseLong(TopologyUtils.getConfigWithDefault(
        topologyConfig, com.twitter.heron.api.Config.TOPOLOGY_CONTAINER_DISK_REQUESTED,
        Long.toString(instanceDiskDefault * DEFAULT_NUMBER_INSTANCES_PER_CONTAINER)));
  }

  @Override
  public PackingPlan pack() {
    int adjustments = this.numAdjustments;
    // Get the instances using a resource compliant round robin allocation
    Map<String, List<String>> resourceCompliantRRAllocation
        = getResourceCompliantRRAllocation();

    while (resourceCompliantRRAllocation == null) {
      if (this.numAdjustments > adjustments) {
        adjustments++;
        resourceCompliantRRAllocation = getResourceCompliantRRAllocation();
      } else {
        return null;
      }
    }
    // Construct the PackingPlan
    Set<PackingPlan.ContainerPlan> containerPlans = new HashSet<>();
    Map<String, Long> ramMap = TopologyUtils.getComponentRamMapConfig(topology);

    List<TopologyAPI.Config.KeyValue> topologyConfig = topology.getTopologyConfig().getKvsList();
    int paddingPercentage = Integer.parseInt(TopologyUtils.getConfigWithDefault(
        topologyConfig, com.twitter.heron.api.Config.TOPOLOGY_CONTAINER_PADDING_PERCENTAGE,
        Integer.toString(DEFAULT_CONTAINER_PADDING_PERCENTAGE)));

    long topologyRam = 0;
    long topologyDisk = 0;
    double topologyCpu = 0.0;

    for (Map.Entry<String, List<String>> entry : resourceCompliantRRAllocation.entrySet()) {

      String containerId = entry.getKey();
      List<String> instanceList = entry.getValue();

      long containerRam = 0;
      long containerDiskInBytes = 0;
      double containerCpu = 0;

      // Calculate the resource required for single instance
      Set<PackingPlan.InstancePlan> instancePlans = new HashSet<>();

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
        instancePlans.add(instancePlan);
      }

      containerCpu += (paddingPercentage * containerCpu) / 100;
      containerRam += (paddingPercentage * containerRam) / 100;
      containerDiskInBytes += (paddingPercentage * containerDiskInBytes) / 100;

      Resource resource =
          new Resource(Math.round(containerCpu), containerRam, containerDiskInBytes);

      PackingPlan.ContainerPlan containerPlan =
          new PackingPlan.ContainerPlan(containerId, instancePlans, resource);

      containerPlans.add(containerPlan);
      topologyRam += containerRam;
      topologyCpu += Math.round(containerCpu);
      topologyDisk += containerDiskInBytes;
    }

    // Take the heron internal container into account and the application master for YARN
    // scheduler
    topologyRam += instanceRamDefault;
    topologyDisk += instanceDiskDefault;
    topologyCpu += instanceCpuDefault;

    Resource resource = new Resource(topologyCpu, topologyRam, topologyDisk);

    return new PackingPlan(topology.getId(), containerPlans, resource);
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
  protected ArrayList<RamRequirement> getRAMInstances() {
    ArrayList<RamRequirement> ramRequirements = new ArrayList<>();
    Map<String, Integer> parallelismMap = TopologyUtils.getComponentParallelism(topology);
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
    return ramRequirements;
  }

  /**
   * Get the instances' allocation based on the Resource Compliant Round Robin algorithm
   *
   * @return Map &lt; containerId, list of InstanceId belonging to this container &gt;
   */
  protected Map<String, List<String>> getResourceCompliantRRAllocation() {

    Map<String, List<String>> allocation = new HashMap<>();
    ArrayList<Container> containers = new ArrayList<>();
    ArrayList<RamRequirement> ramRequirements = getRAMInstances();

    int totalInstance = TopologyUtils.getTotalInstance(topology);

    if (numContainers > totalInstance) {
      throw new RuntimeException("More containers allocated than instances."
          + numContainers + " allocated to host " + totalInstance + " instances.");
    }

    for (int i = 1; i <= numContainers; ++i) {
      allocation.put(getContainerId(i), new ArrayList<String>());
    }
    for (int i = 0; i <= numContainers - 1; i++) {
      allocateNewContainer(containers);
    }

    int containerId = 1;
    int globalTaskIndex = 1;
    Map<String, Integer> parallelismMap = TopologyUtils.getComponentParallelism(topology);
    int componentIndex = 0;
    for (String component : parallelismMap.keySet()) {
      long ramRequirement = ramRequirements.get(componentIndex).getRamRequirement();
      int numInstance = parallelismMap.get(component);
      for (int i = 0; i < numInstance; ++i) {
        if (placeResourceCompliantRRInstance(containers, containerId,
            ramRequirement, instanceCpuDefault, instanceDiskDefault)) {
          allocation.get(getContainerId(containerId))
              .add(getInstanceId(containerId, component, globalTaskIndex, i));
        } else {
          List<TopologyAPI.Config.KeyValue> topologyConfig
              = topology.getTopologyConfig().getKvsList();
          //Automatically adjust the number of containers
          increasenumContainers();
          LOG.info(String.format("Increasing the number of containers to "
              + this.numContainers + " and attempting packing again."));
          return null;
        }
        containerId = (containerId == numContainers) ? 1 : containerId + 1;
        globalTaskIndex++;
      }
      componentIndex++;
    }
    return allocation;
  }

  /**
   * Assign a particular instance to a container with a given containerId
   *
   * @return true if the container incorporated the instance, otherwise return false
   */
  public boolean placeResourceCompliantRRInstance(ArrayList<Container> containers, int containerId,
                                                  long ramRequirement, double cpuRequirement,
                                                  long diskRequirement) {
    if (containers.get(containerId - 1).add(ramRequirement, cpuRequirement, diskRequirement)) {
      return true;
    }
    return false;
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


