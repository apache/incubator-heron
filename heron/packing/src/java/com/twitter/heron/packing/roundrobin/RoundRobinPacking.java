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
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Constants;
import com.twitter.heron.spi.common.Context;
import com.twitter.heron.spi.packing.IPacking;
import com.twitter.heron.spi.packing.PackingPlan;
import com.twitter.heron.spi.packing.Resource;
import com.twitter.heron.spi.utils.TopologyUtils;

/**
 * Round-robin packing algorithm
 * <p>
 * This IPacking implementation generates PackingPlan: instances of the component are assigned
 * to each container one by one in circular order, without any priority. Each container is expected
 * to take equal number of instances if # of instances is multiple of # of containers.
 * <p>
 * Following semantics are guaranteed:
 * 1. Every container requires same size of resource, i.e. same cpu, ram and disk.
 * Consider that instances in different containers can be different, the value of size
 * will be aligned to the max one.
 * <p>
 * 2. The size of resource required by the whole topology is equal to
 * ((# of container specified in config) + 1) * (size of resource required for a single container).
 * The extra 1 is considered for Heron internal container,
 * i.e. the one containing Scheduler and TMaster.
 * <p>
 * 3. The disk required for a container is calculated as:
 * value for com.twitter.heron.api.Config.TOPOLOGY_CONTAINER_DISK_REQUESTED if exists, otherwise,
 * (disk for instances in container) + (disk padding for heron internal process)
 * <p>
 * 4. The cpu required for a container is calculated as:
 * value for com.twitter.heron.api.Config.TOPOLOGY_CONTAINER_CPU_REQUESTED if exists, otherwise,
 * (cpu for instances in container) + (cpu padding for heron internal process)
 * <p>
 * 5. The ram required for a container is calculated as:
 * value for com.twitter.heron.api.Config.TOPOLOGY_CONTAINER_RAM_REQUESTED if exists, otherwise,
 * (ram for instances in container) + (ram padding for heron internal process)
 * <p>
 * 6. The ram required for one instance is calculated as:
 * value in com.twitter.heron.api.Config.TOPOLOGY_COMPONENT_RAMMAP if exists, otherwise,
 * - if com.twitter.heron.api.Config.TOPOLOGY_CONTAINER_RAM_REQUESTED not exists:
 * the default ram value for one instance
 * - if com.twitter.heron.api.Config.TOPOLOGY_CONTAINER_RAM_REQUESTED exists:
 * ((TOPOLOGY_CONTAINER_RAM_REQUESTED) - (ram padding for heron internal process)
 * - (ram used by instances within TOPOLOGY_COMPONENT_RAMMAP config))) /
 * (the # of instances in container not specified in TOPOLOGY_COMPONENT_RAMMAP config)
 * 7. The pack() return null if PackingPlan fails to pass the safe check, for instance,
 * the size of ram for an instance is less than the minimal required value.
 */
public class RoundRobinPacking implements IPacking {
  private static final Logger LOG = Logger.getLogger(RoundRobinPacking.class.getName());

  // TODO(mfu): Read these values from Config
  public static final long DEFAULT_DISK_PADDING_PER_CONTAINER = 12L * Constants.GB;
  public static final long DEFAULT_RAM_PADDING_PER_CONTAINER = 2L * Constants.GB;
  public static final double DEFAULT_CPU_PADDING_PER_CONTAINER = 1;
  public static final long MIN_RAM_PER_INSTANCE = 192L * Constants.MB;

  // Use as a stub as default number value when getting config value
  public static final String NOT_SPECIFIED_NUMBER_VALUE = "-1";

  protected TopologyAPI.Topology topology;

  protected long instanceRamDefault;
  protected double instanceCpuDefault;
  protected long instanceDiskDefault;

  @Override
  public void initialize(Config config, TopologyAPI.Topology inputTopology) {
    this.topology = inputTopology;
    this.instanceRamDefault = Context.instanceRam(config);
    this.instanceCpuDefault = Context.instanceCpu(config).doubleValue();
    this.instanceDiskDefault = Context.instanceDisk(config);
  }

  @Override
  public PackingPlan pack() {
    // Get the instances' round-robin allocation
    Map<String, List<String>> roundRobinAllocation = getRoundRobinAllocation();

    // Get the ram map for every instance
    Map<String, Map<String, Long>> instancesRamMap =
        getInstancesRamMapInContainer(roundRobinAllocation);

    long containerDiskInBytes = getContainerDiskHint(roundRobinAllocation);
    double containerCpu = getContainerCpuHint(roundRobinAllocation);

    // Construct the PackingPlan
    Set<PackingPlan.ContainerPlan> containerPlans = new HashSet<>();
    long topologyRam = 0;
    for (Map.Entry<String, List<String>> entry : roundRobinAllocation.entrySet()) {
      String containerId = entry.getKey();
      List<String> instanceList = entry.getValue();

      // Calculate the resource required for single instance
      Map<String, PackingPlan.InstancePlan> instancePlanMap = new HashMap<>();
      long containerRam = DEFAULT_RAM_PADDING_PER_CONTAINER;
      for (String instanceId : instanceList) {
        long instanceRam = instancesRamMap.get(containerId).get(instanceId);

        // Currently not yet support disk or cpu config for different components,
        // so just use the default value.
        long instanceDisk = instanceDiskDefault;
        double instanceCpu = instanceCpuDefault;

        Resource resource =
            new Resource(instanceCpu, instanceRam, instanceDisk);
        PackingPlan.InstancePlan instancePlan =
            new PackingPlan.InstancePlan(
                instanceId,
                getComponentName(instanceId),
                resource);
        // Insert it into the map
        instancePlanMap.put(instanceId, instancePlan);
        containerRam += instanceRam;
      }

      Resource resource =
          new Resource(containerCpu, containerRam, containerDiskInBytes);
      PackingPlan.ContainerPlan containerPlan =
          new PackingPlan.ContainerPlan(
              containerId, new HashSet<>(instancePlanMap.values()), resource);

      containerPlans.add(containerPlan);
      topologyRam += containerRam;
    }

    // Take the heron internal container into account
    int totalContainer = containerPlans.size() + 1;
    long topologyDisk = totalContainer * containerDiskInBytes;
    double topologyCpu = totalContainer * containerCpu;

    Resource resource = new Resource(
        topologyCpu, topologyRam, topologyDisk);

    PackingPlan plan = new PackingPlan(topology.getId(), containerPlans, resource);

    // Check whether it is a valid PackingPlan
    if (!isValidPackingPlan(plan)) {
      return null;
    }
    return plan;
  }

  @Override
  public void close() {

  }

  /**
   * Calculate the ram required by any instance in the container
   *
   * @param allocation the allocation of instances in different container
   * @return A map: (containerId -&gt; (instanceId -&gt; instanceRequiredRam))
   */
  protected Map<String, Map<String, Long>> getInstancesRamMapInContainer(
      Map<String, List<String>> allocation) {
    Map<String, Long> ramMap =
        TopologyUtils.getComponentRamMapConfig(topology);

    Map<String, Map<String, Long>> instancesRamMapInContainer = new HashMap<>();

    for (Map.Entry<String, List<String>> entry : allocation.entrySet()) {
      String containerId = entry.getKey();
      Map<String, Long> ramInsideContainer = new HashMap<>();
      instancesRamMapInContainer.put(containerId, ramInsideContainer);
      List<String> instancesToBeAccounted = new ArrayList<>();

      // Calculate the actual value
      long usedRam = 0;
      for (String instanceId : entry.getValue()) {
        String componentName = getComponentName(instanceId);
        if (ramMap.containsKey(componentName)) {
          long ram = ramMap.get(componentName);
          ramInsideContainer.put(instanceId, ram);
          usedRam += ram;
        } else {
          instancesToBeAccounted.add(instanceId);
        }
      }

      // Now we have calculated ram for instances specified in ComponentRamMap
      // Then to calculate ram for the rest instances
      long containerRamHint = getContainerRamHint(allocation);
      int instancesToAllocate = instancesToBeAccounted.size();

      if (instancesToAllocate != 0) {
        long individualInstanceRam = instanceRamDefault;

        // The ram map is partially set. We need to calculate ram for the rest

        // We have different strategy depending on whether container ram is specified
        // If container ram is specified
        if (containerRamHint != Long.parseLong(NOT_SPECIFIED_NUMBER_VALUE)) {
          // remove ram for heron internal process
          long remainingRam = containerRamHint - DEFAULT_RAM_PADDING_PER_CONTAINER - usedRam;

          // Split remaining ram evenly
          individualInstanceRam = remainingRam / instancesToAllocate;
        }

        // Put the results in instancesRam
        for (String instanceId : instancesToBeAccounted) {
          ramInsideContainer.put(instanceId, individualInstanceRam);
        }
      }
    }

    return instancesRamMapInContainer;
  }


  /**
   * Get the instances' allocation basing on round robin algorithm
   *
   * @return containerId -&gt; list of InstanceId belonging to this container
   */
  protected Map<String, List<String>> getRoundRobinAllocation() {
    Map<String, List<String>> allocation = new HashMap<>();
    int numContainer = TopologyUtils.getNumContainers(topology);
    int totalInstance = TopologyUtils.getTotalInstance(topology);
    if (numContainer > totalInstance) {
      throw new RuntimeException("More containers allocated than instance.");
    }

    for (int i = 1; i <= numContainer; ++i) {
      allocation.put(getContainerId(i), new ArrayList<String>());
    }

    int index = 1;
    int globalTaskIndex = 1;
    Map<String, Integer> parallelismMap = TopologyUtils.getComponentParallelism(topology);
    for (String component : parallelismMap.keySet()) {
      int numInstance = parallelismMap.get(component);
      for (int i = 0; i < numInstance; ++i) {
        allocation.
            get(getContainerId(index)).
            add(getInstanceId(index, component, globalTaskIndex, i));

        index = (index == numContainer) ? 1 : index + 1;
        globalTaskIndex++;
      }
    }
    return allocation;
  }

  /**
   * Get # of instances in the largest container
   *
   * @param allocation the instances' allocation
   * @return # of instances in the largest container
   */
  protected int getLargestContainerSize(Map<String, List<String>> allocation) {
    int max = 0;
    for (List<String> instances : allocation.values()) {
      if (instances.size() > max) {
        max = instances.size();
      }
    }
    return max;
  }

  /**
   * Provide cpu per container.
   *
   * @param allocation packing output.
   * @return cpu per container.
   */
  protected double getContainerCpuHint(Map<String, List<String>> allocation) {
    List<TopologyAPI.Config.KeyValue> topologyConfig = topology.getTopologyConfig().getKvsList();
    double defaultContainerCpu =
        DEFAULT_CPU_PADDING_PER_CONTAINER + getLargestContainerSize(allocation);

    String cpuHint = TopologyUtils.getConfigWithDefault(
        topologyConfig, com.twitter.heron.api.Config.TOPOLOGY_CONTAINER_CPU_REQUESTED,
        Double.toString(defaultContainerCpu));

    return Double.parseDouble(cpuHint);
  }

  /**
   * Provide disk per container.
   *
   * @param allocation packing output.
   * @return disk per container.
   */
  protected long getContainerDiskHint(Map<String, List<String>> allocation) {
    long defaultContainerDisk =
        instanceDiskDefault * getLargestContainerSize(allocation)
            + DEFAULT_DISK_PADDING_PER_CONTAINER;

    List<TopologyAPI.Config.KeyValue> topologyConfig = topology.getTopologyConfig().getKvsList();

    String diskHint = TopologyUtils.getConfigWithDefault(
        topologyConfig, com.twitter.heron.api.Config.TOPOLOGY_CONTAINER_DISK_REQUESTED,
        Long.toString(defaultContainerDisk));

    return Long.parseLong(diskHint);
  }

  /**
   * Provide ram per container.
   *
   * @param allocation packing
   * @return Container ram requirement
   */
  public long getContainerRamHint(Map<String, List<String>> allocation) {
    List<TopologyAPI.Config.KeyValue> topologyConfig = topology.getTopologyConfig().getKvsList();

    String ramHint = TopologyUtils.getConfigWithDefault(
        topologyConfig, com.twitter.heron.api.Config.TOPOLOGY_CONTAINER_RAM_REQUESTED,
        NOT_SPECIFIED_NUMBER_VALUE);

    return Long.parseLong(ramHint);
  }

  /**
   * Check whether the PackingPlan generated is valid
   *
   * @param plan The PackingPlan to check
   * @return true if it is valid. Otherwise return false
   */
  protected boolean isValidPackingPlan(PackingPlan plan) {
    for (PackingPlan.ContainerPlan containerPlan : plan.getContainers()) {
      for (PackingPlan.InstancePlan instancePlan : containerPlan.getInstances()) {
        // Safe check
        if (instancePlan.getResource().getRam() < MIN_RAM_PER_INSTANCE) {
          LOG.severe(String.format(
              "Require at least %dMB ram. Given on %d MB",
              MIN_RAM_PER_INSTANCE / Constants.MB,
              instancePlan.getResource().getRam() / Constants.MB));

          return false;
        }
      }
    }

    return true;
  }


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
}
