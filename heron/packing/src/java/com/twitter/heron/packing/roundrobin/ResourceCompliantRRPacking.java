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

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.packing.Container;
import com.twitter.heron.packing.PackingUtils;
import com.twitter.heron.packing.RamRequirement;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Constants;
import com.twitter.heron.spi.common.Context;
import com.twitter.heron.spi.packing.IPacking;
import com.twitter.heron.spi.packing.InstanceId;
import com.twitter.heron.spi.packing.PackingPlan;
import com.twitter.heron.spi.packing.Resource;
import com.twitter.heron.spi.utils.TopologyUtils;

import static com.twitter.heron.api.Config.TOPOLOGY_CONTAINER_CPU_REQUESTED;
import static com.twitter.heron.api.Config.TOPOLOGY_CONTAINER_DISK_REQUESTED;
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

  private static final long MIN_RAM_PER_INSTANCE = 192L * Constants.MB;
  private static final int DEFAULT_CONTAINER_PADDING_PERCENTAGE = 10;
  private static final int DEFAULT_NUMBER_INSTANCES_PER_CONTAINER = 4;

  private static final Logger LOG = Logger.getLogger(ResourceCompliantRRPacking.class.getName());
  private TopologyAPI.Topology topology;

  private Resource defaultInstanceResources;
  private Resource maxContainerResources;
  private int numContainers;
  private int numAdjustments;

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

    List<TopologyAPI.Config.KeyValue> topologyConfig = topology.getTopologyConfig().getKvsList();
    this.maxContainerResources = new Resource(
        TopologyUtils.getConfigWithDefault(topologyConfig, TOPOLOGY_CONTAINER_CPU_REQUESTED,
            this.defaultInstanceResources.getCpu() * DEFAULT_NUMBER_INSTANCES_PER_CONTAINER),
        TopologyUtils.getConfigWithDefault(topologyConfig, TOPOLOGY_CONTAINER_RAM_REQUESTED,
            this.defaultInstanceResources.getRam() * DEFAULT_NUMBER_INSTANCES_PER_CONTAINER),
        TopologyUtils.getConfigWithDefault(topologyConfig, TOPOLOGY_CONTAINER_DISK_REQUESTED,
            this.defaultInstanceResources.getDisk() * DEFAULT_NUMBER_INSTANCES_PER_CONTAINER));
  }

  @Override
  public PackingPlan pack() {
    int adjustments = this.numAdjustments;
    // Get the instances using a resource compliant round robin allocation
    Map<Integer, List<InstanceId>> containerInstances = getResourceCompliantRRAllocation();

    while (containerInstances == null) {
      if (this.numAdjustments > adjustments) {
        adjustments++;
        containerInstances = getResourceCompliantRRAllocation();
      } else {
        return null;
      }
    }
    // Construct the PackingPlan
    Map<String, Long> ramMap = TopologyUtils.getComponentRamMapConfig(topology);

    List<TopologyAPI.Config.KeyValue> topologyConfig = topology.getTopologyConfig().getKvsList();
    int paddingPercentage = TopologyUtils.getConfigWithDefault(
        topologyConfig, com.twitter.heron.api.Config.TOPOLOGY_CONTAINER_PADDING_PERCENTAGE,
        DEFAULT_CONTAINER_PADDING_PERCENTAGE);

    Set<PackingPlan.ContainerPlan> containerPlans = PackingUtils.buildContainerPlans(
        containerInstances, ramMap, this.defaultInstanceResources, paddingPercentage);

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
  protected ArrayList<RamRequirement> getRAMInstances() {
    ArrayList<RamRequirement> ramRequirements = new ArrayList<>();
    Map<String, Integer> parallelismMap = TopologyUtils.getComponentParallelism(topology);
    Map<String, Long> ramMap = TopologyUtils.getComponentRamMapConfig(topology);
    for (String component : parallelismMap.keySet()) {
      if (ramMap.containsKey(component)) {
        if (!PackingUtils.isValidInstance(
            this.defaultInstanceResources.cloneWithRam(ramMap.get(component)),
            MIN_RAM_PER_INSTANCE, this.maxContainerResources)) {
          throw new RuntimeException("The topology configuration does not have "
              + "valid resource requirements. Please make sure that the instance resource "
              + "requirements do not exceed the maximum per-container resources.");
        } else {
          ramRequirements.add(new RamRequirement(component, ramMap.get(component)));
        }
      } else {
        if (!PackingUtils.isValidInstance(this.defaultInstanceResources,
            MIN_RAM_PER_INSTANCE, this.maxContainerResources)) {
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
  private Map<Integer, List<InstanceId>> getResourceCompliantRRAllocation() {

    Map<Integer, List<InstanceId>> allocation = new HashMap<>();
    ArrayList<Container> containers = new ArrayList<>();
    ArrayList<RamRequirement> ramRequirements = getRAMInstances();

    int totalInstance = TopologyUtils.getTotalInstance(topology);

    if (numContainers > totalInstance) {
      throw new RuntimeException("More containers allocated than instances."
          + numContainers + " allocated to host " + totalInstance + " instances.");
    }

    for (int i = 1; i <= numContainers; ++i) {
      allocation.put(i, new ArrayList<InstanceId>());
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
        Resource instanceResource = this.defaultInstanceResources.cloneWithRam(ramRequirement);
        if (placeResourceCompliantRRInstance(containers, containerId,
            new PackingPlan.InstancePlan(new InstanceId(component, globalTaskIndex, i),
                instanceResource))) {
          allocation.get(containerId).add(new InstanceId(component, globalTaskIndex, i));
        } else {
          //Automatically adjust the number of containers
          increaseNumContainers();
          LOG.info(String.format("Increasing the number of containers to "
              + "%s and attempting packing again.", this.numContainers));
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
  private boolean placeResourceCompliantRRInstance(ArrayList<Container> containers, int containerId,
                                                   PackingPlan.InstancePlan instancePlan) {
    return containers.get(containerId - 1).add(instancePlan);
  }

  /**
   * Allocate a new container taking into account the maximum resource requirements specified
   * in the config
   *
   * @return the number of containers
   */
  private int allocateNewContainer(ArrayList<Container> containers) {
    containers.add(new Container(this.maxContainerResources));
    return containers.size();
  }

}


