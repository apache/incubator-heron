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

import com.twitter.heron.api.HeronConfig;
import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.packing.utils.Container;
import com.twitter.heron.spi.common.Constants;
import com.twitter.heron.spi.common.Context;
import com.twitter.heron.spi.common.SpiCommonConfig;
import com.twitter.heron.spi.packing.IPacking;
import com.twitter.heron.spi.packing.PackingPlan;
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
 * the HeronConfig.TOPOLOGY_CONTAINER_MAX_CPU_HINT,
 * HeronConfig.TOPOLOGY_CONTAINER_MAX_RAM_HINT,
 * HeronConfig.TOPOLOGY_CONTAINER_MAX_DISK_HINT parameters.
 * If the parameters are not specified then a default value is used for the maximum container
 * size.
 * <p>
 * 3. The user provides a percentage of each container size that will be added to the resources
 * allocated by the container through the HeronConfig.TOPOLOGY_CONTAINER_PADDING_PERCENTAGE
 * If the parameter is not specified then a default value of 10 is used (10% of the container size)
 * <p>
 * 4. The ram required for one instance is calculated as:
 * value in HeronConfig.TOPOLOGY_COMPONENT_RAMMAP if exists, otherwise,
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

public class FirstFitDecreasingPacking implements IPacking {

  public static final long MIN_RAM_PER_INSTANCE = 192L * Constants.MB;
  public static final int DEFAULT_CONTAINER_PADDING_PERCENTAGE = 10;

  private static final Logger LOG = Logger.getLogger(FirstFitDecreasingPacking.class.getName());
  protected TopologyAPI.Topology topology;

  protected long instanceRamDefault;
  protected double instanceCpuDefault;
  protected long instanceDiskDefault;


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

  @Override
  public void initialize(SpiCommonConfig config, SpiCommonConfig runtime) {
    this.topology = com.twitter.heron.spi.utils.Runtime.topology(runtime);

    this.instanceRamDefault = Context.instanceRam(config);
    this.instanceCpuDefault = Context.instanceCpu(config).doubleValue();
    this.instanceDiskDefault = Context.instanceDisk(config);
  }

  @Override
  public PackingPlan pack() {
    // Get the instances using FFD allocation
    Map<String, List<String>> ffdAllocation = getFFDAllocation();

    if (ffdAllocation == null) {
      return null;
    }
    // Construct the PackingPlan
    Map<String, PackingPlan.ContainerPlan> containerPlanMap = new HashMap<>();
    Map<String, Long> ramMap = TopologyUtils.getComponentRamMapConfig(topology);

    List<TopologyAPI.Config.KeyValue> topologyConfig = topology.getTopologyConfig().getKvsList();
    int paddingPercentage = Integer.parseInt(TopologyUtils.getConfigWithDefault(
        topologyConfig, HeronConfig.TOPOLOGY_CONTAINER_PADDING_PERCENTAGE,
        Integer.toString(DEFAULT_CONTAINER_PADDING_PERCENTAGE)));

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

        PackingPlan.Resource resource =
            new PackingPlan.Resource(instanceCpu, instanceRam, instanceDisk);
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

      PackingPlan.Resource resource =
          new PackingPlan.Resource(Math.round(containerCpu), containerRam, containerDiskInBytes);

      PackingPlan.ContainerPlan containerPlan =
          new PackingPlan.ContainerPlan(containerId, instancePlanMap, resource);

      containerPlanMap.put(containerId, containerPlan);
      topologyRam += containerRam;
      topologyCpu += Math.round(containerCpu);
      topologyDisk += containerDiskInBytes;
    }

    // Take the heron internal container into account and the application master for YARN
    // scheduler
    topologyRam += instanceRamDefault;
    topologyDisk += instanceDiskDefault;
    topologyCpu += instanceCpuDefault;

    PackingPlan.Resource resource = new PackingPlan.Resource(
        topologyCpu, topologyRam, topologyDisk);

    PackingPlan plan = new PackingPlan(topology.getId(), containerPlanMap, resource);

    return plan;
  }

  @Override
  public void close() {

  }

  /**
   * Sort the components in decreasing order based on their RAM requirements
   *
   * @return The sorted list of components and their RAM requirements
   */
  protected ArrayList<RamRequirement> getSortedRAMInstances() {
    ArrayList<RamRequirement> ramRequirements = new ArrayList<>();
    Map<String, Integer> parallelismMap = TopologyUtils.getComponentParallelism(topology);
    Map<String, Long> ramMap = TopologyUtils.getComponentRamMapConfig(topology);

    for (String component : parallelismMap.keySet()) {
      if (ramMap.containsKey(component)) {
        if (!isValidInstance(ramMap.get(component))) {
          return null;
        } else {
          ramRequirements.add(new RamRequirement(component, ramMap.get(component)));
        }
      } else {
        ramRequirements.add(new RamRequirement(component, instanceRamDefault));
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
    Map<String, List<String>> allocation = new HashMap<>();

    ArrayList<Container> containers = new ArrayList<>();

    int globalTaskIndex = 1;
    Map<String, Integer> parallelismMap = TopologyUtils.getComponentParallelism(topology);
    ArrayList<RamRequirement> ramRequirements = getSortedRAMInstances();

    if (ramRequirements == null) {
      return null;
    }
    for (int i = 0; i < ramRequirements.size(); i++) {
      String component = ramRequirements.get(i).getComponentName();
      int numInstance = parallelismMap.get(component);
      for (int j = 0; j < numInstance; j++) {
        int containerId = placeFFDInstance(containers,
            ramRequirements.get(i).getRamRequirement(),
            instanceCpuDefault, instanceDiskDefault);
        if (allocation.containsKey(getContainerId(containerId))) {
          allocation.get(getContainerId(containerId)).
              add(getInstanceId(containerId, component, globalTaskIndex, j));
        } else {
          ArrayList<String> instance = new ArrayList<>();
          instance.add(getInstanceId(containerId, component, globalTaskIndex, j));
          allocation.put(getContainerId(containerId), instance);
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
    List<TopologyAPI.Config.KeyValue> topologyConfig = topology.getTopologyConfig().getKvsList();
    long maxContainerRam = Long.parseLong(TopologyUtils.getConfigWithDefault(
        topologyConfig, HeronConfig.TOPOLOGY_CONTAINER_MAX_RAM_HINT,
        Long.toString(instanceRamDefault * 4)));

    double maxContainerCpu = Double.parseDouble(TopologyUtils.getConfigWithDefault(
        topologyConfig, HeronConfig.TOPOLOGY_CONTAINER_MAX_CPU_HINT,
        Double.toString(instanceCpuDefault * 4)));

    long maxContainerDisk = Long.parseLong(TopologyUtils.getConfigWithDefault(
        topologyConfig, HeronConfig.TOPOLOGY_CONTAINER_MAX_DISK_HINT,
        Long.toString(instanceDiskDefault * 4)));

    containers.add(new Container(maxContainerRam, maxContainerCpu, maxContainerDisk));
    return containers.size();
  }

  /**
   * Check whether the Instance has enough RAM and whether it can fit within the container limits.
   *
   * @param instanceRAM The RAM allocated to the instance
   * @return true if the instance is valid, false otherwise
   */
  protected boolean isValidInstance(long instanceRAM) {

    if (instanceRAM < MIN_RAM_PER_INSTANCE) {
      LOG.severe(String.format(
          "Require at least %d MB ram per instance",
          MIN_RAM_PER_INSTANCE / Constants.MB));
      return false;
    }

    List<TopologyAPI.Config.KeyValue> topologyConfig = topology.getTopologyConfig().getKvsList();
    long maxContainerRam = Long.parseLong(TopologyUtils.getConfigWithDefault(
        topologyConfig, HeronConfig.TOPOLOGY_CONTAINER_MAX_RAM_HINT,
        Long.toString(instanceRamDefault * 4)));

    if (instanceRAM > maxContainerRam) {
      LOG.severe(String.format(
          "This instance requires containers of at least %d MB ram. The current max container"
              + "size is %d MB",
          instanceRAM, maxContainerRam));
      return false;
    }
    return true;
  }

  /**
   * Helper class that captures the RAM requirements of each component
   */
  protected class RamRequirement implements Comparable<RamRequirement> {
    private String componentName;
    private long ramRequirement;

    protected RamRequirement(String componentName, long ram) {
      this.componentName = componentName;
      this.ramRequirement = ram;
    }

    protected String getComponentName() {
      return componentName;
    }

    protected long getRamRequirement() {
      return ramRequirement;
    }

    @Override
    public int compareTo(RamRequirement other) {
      return Long.compare(this.ramRequirement, other.ramRequirement);
    }

    @Override
    public boolean equals(Object o) {

      if (o == this) {
        return true;
      }
      if (!(o instanceof RamRequirement)) {
        return false;
      }
      RamRequirement c = (RamRequirement) o;

      // Compare the ramRequirement values and return accordingly
      return Long.compare(ramRequirement, c.ramRequirement) == 0;
    }

    @Override
    public int hashCode() {
      int result = componentName.hashCode();
      result = 31 * result + (int) (ramRequirement ^ (ramRequirement >>> 32));
      return result;
    }
  }
}
