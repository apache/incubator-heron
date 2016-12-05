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

import com.google.common.annotations.VisibleForTesting;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.common.basics.ByteAmount;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Context;
import com.twitter.heron.spi.packing.IPacking;
import com.twitter.heron.spi.packing.InstanceId;
import com.twitter.heron.spi.packing.PackingException;
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
  private static final ByteAmount DEFAULT_DISK_PADDING_PER_CONTAINER = ByteAmount.fromGigabytes(12);
  private static final double DEFAULT_CPU_PADDING_PER_CONTAINER = 1;
  private static final ByteAmount MIN_RAM_PER_INSTANCE = ByteAmount.fromMegabytes(192);
  @VisibleForTesting
  static final ByteAmount DEFAULT_RAM_PADDING_PER_CONTAINER = ByteAmount.fromGigabytes(2);

  // Use as a stub as default number value when getting config value
  private static final ByteAmount NOT_SPECIFIED_NUMBER_VALUE = ByteAmount.fromBytes(-1);

  private TopologyAPI.Topology topology;

  private ByteAmount instanceRamDefault;
  private double instanceCpuDefault;
  private ByteAmount instanceDiskDefault;

  @Override
  public void initialize(Config config, TopologyAPI.Topology inputTopology) {
    this.topology = inputTopology;
    this.instanceRamDefault = Context.instanceRam(config);
    this.instanceCpuDefault = Context.instanceCpu(config);
    this.instanceDiskDefault = Context.instanceDisk(config);
  }

  @Override
  public PackingPlan pack() {
    // Get the instances' round-robin allocation
    Map<Integer, List<InstanceId>> roundRobinAllocation = getRoundRobinAllocation();

    // Get the ram map for every instance
    Map<Integer, Map<InstanceId, ByteAmount>> instancesRamMap =
        getInstancesRamMapInContainer(roundRobinAllocation);

    ByteAmount containerDiskInBytes = getContainerDiskHint(roundRobinAllocation);
    double containerCpu = getContainerCpuHint(roundRobinAllocation);

    // Construct the PackingPlan
    Set<PackingPlan.ContainerPlan> containerPlans = new HashSet<>();
    for (int containerId : roundRobinAllocation.keySet()) {
      List<InstanceId> instanceList = roundRobinAllocation.get(containerId);

      // Calculate the resource required for single instance
      Map<InstanceId, PackingPlan.InstancePlan> instancePlanMap = new HashMap<>();
      ByteAmount containerRam = DEFAULT_RAM_PADDING_PER_CONTAINER;
      for (InstanceId instanceId : instanceList) {
        ByteAmount instanceRam = instancesRamMap.get(containerId).get(instanceId);

        // Currently not yet support disk or cpu config for different components,
        // so just use the default value.
        ByteAmount instanceDisk = instanceDiskDefault;
        double instanceCpu = instanceCpuDefault;

        Resource resource = new Resource(instanceCpu, instanceRam, instanceDisk);

        // Insert it into the map
        instancePlanMap.put(instanceId, new PackingPlan.InstancePlan(instanceId, resource));
        containerRam = containerRam.plus(instanceRam);
      }

      Resource resource = new Resource(containerCpu, containerRam, containerDiskInBytes);
      PackingPlan.ContainerPlan containerPlan = new PackingPlan.ContainerPlan(
          containerId, new HashSet<>(instancePlanMap.values()), resource);

      containerPlans.add(containerPlan);
    }

    PackingPlan plan = new PackingPlan(topology.getId(), containerPlans);

    // Check whether it is a valid PackingPlan
    validatePackingPlan(plan);
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
  private Map<Integer, Map<InstanceId, ByteAmount>> getInstancesRamMapInContainer(
      Map<Integer, List<InstanceId>> allocation) {
    Map<String, ByteAmount> ramMap = TopologyUtils.getComponentRamMapConfig(topology);

    Map<Integer, Map<InstanceId, ByteAmount>> instancesRamMapInContainer = new HashMap<>();

    for (int containerId : allocation.keySet()) {
      List<InstanceId> instanceIds = allocation.get(containerId);
      Map<InstanceId, ByteAmount> ramInsideContainer = new HashMap<>();
      instancesRamMapInContainer.put(containerId, ramInsideContainer);
      List<InstanceId> instancesToBeAccounted = new ArrayList<>();

      // Calculate the actual value
      ByteAmount usedRam = ByteAmount.ZERO;
      for (InstanceId instanceId : instanceIds) {
        String componentName = instanceId.getComponentName();
        if (ramMap.containsKey(componentName)) {
          ByteAmount ram = ramMap.get(componentName);
          ramInsideContainer.put(instanceId, ram);
          usedRam = usedRam.plus(ram);
        } else {
          instancesToBeAccounted.add(instanceId);
        }
      }

      // Now we have calculated ram for instances specified in ComponentRamMap
      // Then to calculate ram for the rest instances
      ByteAmount containerRamHint = getContainerRamHint(allocation);
      int instancesToAllocate = instancesToBeAccounted.size();

      if (instancesToAllocate != 0) {
        ByteAmount individualInstanceRam = instanceRamDefault;

        // The ram map is partially set. We need to calculate ram for the rest

        // We have different strategy depending on whether container ram is specified
        // If container ram is specified
        if (!containerRamHint.equals(NOT_SPECIFIED_NUMBER_VALUE)) {
          // remove ram for heron internal process
          ByteAmount remainingRam = containerRamHint
              .minus(DEFAULT_RAM_PADDING_PER_CONTAINER).minus(usedRam);

          // Split remaining ram evenly
          individualInstanceRam = remainingRam.divide(instancesToAllocate);
        }

        // Put the results in instancesRam
        for (InstanceId instanceId : instancesToBeAccounted) {
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
  private Map<Integer, List<InstanceId>> getRoundRobinAllocation() {
    Map<Integer, List<InstanceId>> allocation = new HashMap<>();
    int numContainer = TopologyUtils.getNumContainers(topology);
    int totalInstance = TopologyUtils.getTotalInstance(topology);
    if (numContainer > totalInstance) {
      throw new RuntimeException("More containers allocated than instance.");
    }

    for (int i = 1; i <= numContainer; ++i) {
      allocation.put(i, new ArrayList<InstanceId>());
    }

    int index = 1;
    int globalTaskIndex = 1;
    Map<String, Integer> parallelismMap = TopologyUtils.getComponentParallelism(topology);
    for (String component : parallelismMap.keySet()) {
      int numInstance = parallelismMap.get(component);
      for (int i = 0; i < numInstance; ++i) {
        allocation.get(index).add(new InstanceId(component, globalTaskIndex, i));
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
  private int getLargestContainerSize(Map<Integer, List<InstanceId>> allocation) {
    int max = 0;
    for (List<InstanceId> instances : allocation.values()) {
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
  private double getContainerCpuHint(Map<Integer, List<InstanceId>> allocation) {
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
  private ByteAmount getContainerDiskHint(Map<Integer, List<InstanceId>> allocation) {
    ByteAmount defaultContainerDisk = instanceDiskDefault
        .multiply(getLargestContainerSize(allocation))
        .plus(DEFAULT_DISK_PADDING_PER_CONTAINER);

    List<TopologyAPI.Config.KeyValue> topologyConfig = topology.getTopologyConfig().getKvsList();

    return TopologyUtils.getConfigWithDefault(topologyConfig,
        com.twitter.heron.api.Config.TOPOLOGY_CONTAINER_DISK_REQUESTED,
        defaultContainerDisk);
  }

  /**
   * Provide ram per container.
   *
   * @param allocation packing
   * @return Container ram requirement
   */
  private ByteAmount getContainerRamHint(Map<Integer, List<InstanceId>> allocation) {
    List<TopologyAPI.Config.KeyValue> topologyConfig = topology.getTopologyConfig().getKvsList();

    return TopologyUtils.getConfigWithDefault(
        topologyConfig, com.twitter.heron.api.Config.TOPOLOGY_CONTAINER_RAM_REQUESTED,
        NOT_SPECIFIED_NUMBER_VALUE);
  }

  /**
   * Check whether the PackingPlan generated is valid
   *
   * @param plan The PackingPlan to check
   * @throws PackingException if it's not a valid plan
   */
  private void validatePackingPlan(PackingPlan plan) throws PackingException {
    for (PackingPlan.ContainerPlan containerPlan : plan.getContainers()) {
      for (PackingPlan.InstancePlan instancePlan : containerPlan.getInstances()) {
        // Safe check
        if (instancePlan.getResource().getRam().lessThan(MIN_RAM_PER_INSTANCE)) {
          throw new PackingException(String.format("Invalid packing plan generated. A minimum of "
                  + "%s ram is required, but InstancePlan for component '%s' has %s",
              MIN_RAM_PER_INSTANCE, instancePlan.getComponentName(),
              instancePlan.getResource().getRam()));
        }
      }
    }
  }
}
