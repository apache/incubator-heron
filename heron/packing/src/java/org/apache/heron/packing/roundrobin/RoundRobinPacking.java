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

package org.apache.heron.packing.roundrobin;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import com.google.common.annotations.VisibleForTesting;

import org.apache.heron.api.generated.TopologyAPI;
import org.apache.heron.api.utils.TopologyUtils;
import org.apache.heron.common.basics.ByteAmount;
import org.apache.heron.common.basics.CPUShare;
import org.apache.heron.common.basics.ResourceMeasure;
import org.apache.heron.spi.common.Config;
import org.apache.heron.spi.common.Context;
import org.apache.heron.spi.packing.IPacking;
import org.apache.heron.spi.packing.IRepacking;
import org.apache.heron.spi.packing.InstanceId;
import org.apache.heron.spi.packing.PackingException;
import org.apache.heron.spi.packing.PackingPlan;
import org.apache.heron.spi.packing.Resource;

/**
 * Round-robin packing algorithm
 * <p>
 * This IPacking implementation generates PackingPlan: instances of the component are assigned
 * to each container one by one in circular order, without any priority. Each container is expected
 * to take equal number of instances if # of instances is multiple of # of containers.
 * <p>
 * Following semantics are guaranteed:
 * 1. Every container requires same size of resource, i.e. same CPU, RAM and disk.
 * Consider that instances in different containers can be different, the value of size
 * will be aligned to the max one.
 * <p>
 * 2. The size of resource required by the whole topology is equal to
 * ((# of container specified in config) + 1) * (size of resource required for a single container).
 * The extra 1 is considered for Heron internal container,
 * i.e. the one containing Scheduler and TMaster.
 * <p>
 * 3. The disk required for a container is calculated as:
 * value for org.apache.heron.api.Config.TOPOLOGY_CONTAINER_DISK_REQUESTED if exists, otherwise,
 * (disk for instances in container) + (disk padding for heron internal process)
 * <p>
 * 4. The CPU required for a container is calculated as:
 * value for org.apache.heron.api.Config.TOPOLOGY_CONTAINER_CPU_REQUESTED if exists, otherwise,
 * (CPU for instances in container) + (CPU padding for heron internal process)
 * <p>
 * 5. The RAM required for a container is calculated as:
 * value for org.apache.heron.api.Config.TOPOLOGY_CONTAINER_RAM_REQUESTED if exists, otherwise,
 * (RAM for instances in container) + (RAM padding for heron internal process)
 * <p>
 * 6. The RAM required for one instance is calculated as:
 * value in org.apache.heron.api.Config.TOPOLOGY_COMPONENT_RAMMAP if exists, otherwise,
 * - if org.apache.heron.api.Config.TOPOLOGY_CONTAINER_RAM_REQUESTED not exists:
 * the default RAM value for one instance
 * - if org.apache.heron.api.Config.TOPOLOGY_CONTAINER_RAM_REQUESTED exists:
 * ((TOPOLOGY_CONTAINER_RAM_REQUESTED) - (RAM padding for heron internal process)
 * - (RAM used by instances within TOPOLOGY_COMPONENT_RAMMAP config))) /
 * (the # of instances in container not specified in TOPOLOGY_COMPONENT_RAMMAP config)
 * 7. The pack() return null if PackingPlan fails to pass the safe check, for instance,
 * the size of RAM for an instance is less than the minimal required value.
 */
public class RoundRobinPacking implements IPacking, IRepacking {
  private static final Logger LOG = Logger.getLogger(RoundRobinPacking.class.getName());

  @VisibleForTesting
  static final ByteAmount DEFAULT_RAM_PADDING_PER_CONTAINER = ByteAmount.fromGigabytes(2);
  @VisibleForTesting
  static final double DEFAULT_CPU_PADDING_PER_CONTAINER = 1.0;
  private static final ByteAmount DEFAULT_DISK_PADDING_PER_CONTAINER = ByteAmount.fromGigabytes(12);

  @VisibleForTesting
  static final ByteAmount DEFAULT_DAEMON_PROCESS_RAM_PADDING = ByteAmount.fromGigabytes(1);
  private static final ByteAmount MIN_RAM_PER_INSTANCE = ByteAmount.fromMegabytes(192);

  // Use as a stub as default number value when getting config value
  private static final ByteAmount NOT_SPECIFIED_BYTE_AMOUNT = ByteAmount.fromBytes(-1);
  private static final double NOT_SPECIFIED_CPU_SHARE = -1.0;

  private static final String RAM = "RAM";
  private static final String CPU = "CPU";
  private static final String DISK = "DISK";

  private TopologyAPI.Topology topology;

  private ByteAmount instanceRamDefault;
  private double instanceCpuDefault;
  private ByteAmount instanceDiskDefault;
  private ByteAmount containerRamPadding = DEFAULT_RAM_PADDING_PER_CONTAINER;
  private double containerCpuPadding = DEFAULT_CPU_PADDING_PER_CONTAINER;

  @Override
  public void initialize(Config config, TopologyAPI.Topology inputTopology) {
    this.topology = inputTopology;
    this.instanceCpuDefault = Context.instanceCpu(config);
    this.instanceRamDefault = Context.instanceRam(config);
    this.instanceDiskDefault = Context.instanceDisk(config);
    this.containerRamPadding = getContainerRamPadding(topology.getTopologyConfig().getKvsList());
    LOG.info(String.format("Initalizing RoundRobinPacking. "
        + "CPU default: %f, RAM default: %s, DISK default: %s, RAM padding: %s.",
        this.instanceCpuDefault,
        this.instanceRamDefault.toString(),
        this.instanceDiskDefault.toString(),
        this.containerRamPadding.toString()));
  }

  @Override
  public PackingPlan pack() {
    int numContainer = TopologyUtils.getNumContainers(topology);
    Map<String, Integer> parallelismMap = TopologyUtils.getComponentParallelism(topology);

    return packInternal(numContainer, parallelismMap);
  }

  private PackingPlan packInternal(int numContainer, Map<String, Integer> parallelismMap) {
    // Get the instances' round-robin allocation
    Map<Integer, List<InstanceId>> roundRobinAllocation =
        getRoundRobinAllocation(numContainer, parallelismMap);

    // Get the RAM map for every instance
    Map<Integer, Map<InstanceId, ByteAmount>> instancesRamMap =
        calculateInstancesResourceMapInContainer(
        roundRobinAllocation,
        TopologyUtils.getComponentRamMapConfig(topology),
        getContainerRamHint(roundRobinAllocation),
        instanceRamDefault,
        containerRamPadding,
        ByteAmount.ZERO,
        NOT_SPECIFIED_BYTE_AMOUNT,
        RAM);

    // Get the CPU map for every instance
    Map<Integer, Map<InstanceId, CPUShare>> instancesCpuMap =
        calculateInstancesResourceMapInContainer(
        roundRobinAllocation,
        CPUShare.convertDoubleMapToCpuShareMap(TopologyUtils.getComponentCpuMapConfig(topology)),
        CPUShare.fromDouble(getContainerCpuHint(roundRobinAllocation)),
        CPUShare.fromDouble(instanceCpuDefault),
        CPUShare.fromDouble(containerCpuPadding),
        CPUShare.fromDouble(0.0),
        CPUShare.fromDouble(NOT_SPECIFIED_CPU_SHARE),
        CPU);

    ByteAmount containerDiskInBytes = getContainerDiskHint(roundRobinAllocation);
    double containerCpuHint = getContainerCpuHint(roundRobinAllocation);
    ByteAmount containerRamHint = getContainerRamHint(roundRobinAllocation);

    LOG.info(String.format("Pack internal: container CPU hint: %.3f, RAM hint: %s, disk hint: %s.",
        containerCpuHint,
        containerRamHint.toString(),
        containerDiskInBytes.toString()));

    // Construct the PackingPlan
    Set<PackingPlan.ContainerPlan> containerPlans = new HashSet<>();
    for (int containerId : roundRobinAllocation.keySet()) {
      List<InstanceId> instanceList = roundRobinAllocation.get(containerId);

      // Calculate the resource required for single instance
      Map<InstanceId, PackingPlan.InstancePlan> instancePlanMap = new HashMap<>();
      ByteAmount containerRam = containerRamPadding;
      double containerCpu = containerCpuPadding;

      for (InstanceId instanceId : instanceList) {
        ByteAmount instanceRam = instancesRamMap.get(containerId).get(instanceId);
        Double instanceCpu = instancesCpuMap.get(containerId).get(instanceId).getValue();

        // Currently not yet support disk config for different components, just use the default.
        ByteAmount instanceDisk = instanceDiskDefault;

        Resource resource = new Resource(instanceCpu, instanceRam, instanceDisk);

        // Insert it into the map
        instancePlanMap.put(instanceId, new PackingPlan.InstancePlan(instanceId, resource));
        containerRam = containerRam.plus(instanceRam);
        containerCpu += instanceCpu;
      }

      Resource resource = new Resource(Math.max(containerCpu, containerCpuHint),
          containerRam, containerDiskInBytes);
      PackingPlan.ContainerPlan containerPlan = new PackingPlan.ContainerPlan(
          containerId, new HashSet<>(instancePlanMap.values()), resource);

      containerPlans.add(containerPlan);

      LOG.info(String.format("Pack internal finalized: container#%d CPU: %f, RAM: %s, disk: %s.",
          containerId,
          resource.getCpu(),
          resource.getRam().toString(),
          resource.getDisk().toString()));
    }

    PackingPlan plan = new PackingPlan(topology.getId(), containerPlans);

    validatePackingPlan(plan);
    return plan;
  }

  @Override
  public void close() {

  }

  private ByteAmount getContainerRamPadding(List<TopologyAPI.Config.KeyValue> topologyConfig) {
    ByteAmount stmgrRam = TopologyUtils.getConfigWithDefault(topologyConfig,
        org.apache.heron.api.Config.TOPOLOGY_STMGR_RAM,
        DEFAULT_DAEMON_PROCESS_RAM_PADDING);
    ByteAmount metricsmgrRam = TopologyUtils.getConfigWithDefault(topologyConfig,
        org.apache.heron.api.Config.TOPOLOGY_METRICSMGR_RAM,
        DEFAULT_DAEMON_PROCESS_RAM_PADDING);
    String reliabilityMode = TopologyUtils.getConfigWithDefault(topologyConfig,
        org.apache.heron.api.Config.TOPOLOGY_RELIABILITY_MODE,
        org.apache.heron.api.Config.TopologyReliabilityMode.ATMOST_ONCE.name());
    boolean isStateful =
        org.apache.heron.api.Config.TopologyReliabilityMode
            .EFFECTIVELY_ONCE.name().equals(reliabilityMode);
    ByteAmount ckptmgrRam = TopologyUtils.getConfigWithDefault(topologyConfig,
        org.apache.heron.api.Config.TOPOLOGY_STATEFUL_CKPTMGR_RAM,
        isStateful ? DEFAULT_DAEMON_PROCESS_RAM_PADDING : ByteAmount.ZERO);

    ByteAmount daemonProcessPadding = stmgrRam.plus(metricsmgrRam).plus(ckptmgrRam);

    // return the container padding if it's set, otherwise return the total daemon request ram
    return TopologyUtils.getConfigWithDefault(topologyConfig,
        org.apache.heron.api.Config.TOPOLOGY_CONTAINER_RAM_PADDING,
        daemonProcessPadding);
  }

  @SuppressWarnings("unchecked")
  private <T extends ResourceMeasure> Map<Integer, Map<InstanceId, T>>
            calculateInstancesResourceMapInContainer(
                Map<Integer, List<InstanceId>> allocation,
                Map<String, T> resMap,
                T containerResHint,
                T instanceResDefault,
                T containerResPadding,
                T zero,
                T notSpecified,
                String resourceType) {
    Map<Integer, Map<InstanceId, T>> instancesResMapInContainer = new HashMap<>();

    for (int containerId : allocation.keySet()) {
      List<InstanceId> instanceIds = allocation.get(containerId);
      Map<InstanceId, T> resInsideContainer = new HashMap<>();
      instancesResMapInContainer.put(containerId, resInsideContainer);
      List<InstanceId> unspecifiedInstances = new ArrayList<>();

      // Register the instance resource allocation and calculate the used resource so far
      T usedRes = zero;
      for (InstanceId instanceId : instanceIds) {
        String componentName = instanceId.getComponentName();
        if (resMap.containsKey(componentName)) {
          T res = resMap.get(componentName);
          resInsideContainer.put(instanceId, res);
          usedRes = (T) usedRes.plus(res);
        } else {
          unspecifiedInstances.add(instanceId);
        }
      }

      // Validate instance resources specified so far don't violate container-level constraint
      if (!containerResHint.equals(notSpecified)
          && usedRes.greaterThan(containerResHint.minus(containerResPadding))) {
        throw new PackingException(String.format("Invalid packing plan generated. "
                + "Total instance %s in a container (%s) have exceeded "
                + "the container-level constraint of %s.",
            resourceType, usedRes.toString(), containerResHint));
      }

      // calculate resource for the remaining unspecified instances if any
      if (!unspecifiedInstances.isEmpty()) {
        T individualInstanceRes = instanceResDefault;

        // If container resource is specified
        if (!containerResHint.equals(notSpecified)) {
          // discount resource for heron internal process (padding) and used (usedRes)
          T remainingRes = (T) containerResHint.minus(containerResPadding).minus(usedRes);

          if (remainingRes.lessOrEqual(zero)) {
            throw new PackingException(String.format("Invalid packing plan generated. "
                + "No enough %s to allocate for unspecified instances", resourceType));
          }

          // Split remaining resource evenly
          individualInstanceRes = (T) remainingRes.divide(unspecifiedInstances.size());
        }

        // Put the results in resInsideContainer
        for (InstanceId instanceId : unspecifiedInstances) {
          resInsideContainer.put(instanceId, individualInstanceRes);
        }
      }
    }

    return instancesResMapInContainer;
  }

  /**
   * Get the instances' allocation basing on round robin algorithm
   *
   * @return containerId -&gt; list of InstanceId belonging to this container
   */
  private Map<Integer, List<InstanceId>> getRoundRobinAllocation(
      int numContainer, Map<String, Integer> parallelismMap) {
    Map<Integer, List<InstanceId>> allocation = new HashMap<>();
    int totalInstance = TopologyUtils.getTotalInstance(parallelismMap);
    if (numContainer > totalInstance) {
      throw new RuntimeException("More containers allocated than instance.");
    }

    for (int i = 1; i <= numContainer; ++i) {
      allocation.put(i, new ArrayList<>());
    }

    int index = 1;
    int globalTaskIndex = 1;
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
   * Provide CPU per container.
   *
   * @param allocation packing output.
   * @return CPU per container.
   */
  private double getContainerCpuHint(Map<Integer, List<InstanceId>> allocation) {
    List<TopologyAPI.Config.KeyValue> topologyConfig = topology.getTopologyConfig().getKvsList();
    double defaultContainerCpu =
        DEFAULT_CPU_PADDING_PER_CONTAINER + getLargestContainerSize(allocation);

    String cpuHint = TopologyUtils.getConfigWithDefault(
        topologyConfig, org.apache.heron.api.Config.TOPOLOGY_CONTAINER_CPU_REQUESTED,
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
        org.apache.heron.api.Config.TOPOLOGY_CONTAINER_DISK_REQUESTED,
        defaultContainerDisk);
  }

  /**
   * Provide RAM per container.
   *
   * @param allocation packing
   * @return Container RAM requirement
   */
  private ByteAmount getContainerRamHint(Map<Integer, List<InstanceId>> allocation) {
    List<TopologyAPI.Config.KeyValue> topologyConfig = topology.getTopologyConfig().getKvsList();

    return TopologyUtils.getConfigWithDefault(
        topologyConfig, org.apache.heron.api.Config.TOPOLOGY_CONTAINER_RAM_REQUESTED,
        NOT_SPECIFIED_BYTE_AMOUNT);
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
                  + "%s RAM is required, but InstancePlan for component '%s' has %s",
              MIN_RAM_PER_INSTANCE, instancePlan.getComponentName(),
              instancePlan.getResource().getRam()));
        }
      }
    }
  }

  /*
   * read the current packing plan with update parallelism to calculate a new packing plan
   * the packing algorithm packInternal() is shared with pack()
   * delegate to packInternal() with the new container count and component parallelism
   */
  @Override
  public PackingPlan repack(PackingPlan currentPackingPlan, Map<String, Integer> componentChanges)
      throws PackingException {
    int initialNumContainer = TopologyUtils.getNumContainers(topology);
    int initialNumInstance = TopologyUtils.getTotalInstance(topology);
    double initialNumInstancePerContainer = (double) initialNumInstance / initialNumContainer;

    Map<String, Integer> currentComponentParallelism = currentPackingPlan.getComponentCounts();
    Map<String, Integer> newComponentParallelism =
        getNewComponentParallelism(currentPackingPlan, componentChanges);

    int newNumInstance = TopologyUtils.getTotalInstance(currentComponentParallelism);
    int newNumContainer = (int) Math.ceil(newNumInstance / initialNumInstancePerContainer);
    return packInternal(newNumContainer, newComponentParallelism);
  }

  public Map<String, Integer> getNewComponentParallelism(PackingPlan currentPackingPlan,
                                                         Map<String, Integer> componentChanges) {
    Map<String, Integer> currentComponentParallelism = currentPackingPlan.getComponentCounts();
    for (Map.Entry<String, Integer> e : componentChanges.entrySet()) {
      Integer newParallelism = currentComponentParallelism.get(e.getKey()) + e.getValue();
      currentComponentParallelism.put(e.getKey(), newParallelism);
    }
    return currentComponentParallelism;
  }

  @Override
  public PackingPlan repack(PackingPlan currentPackingPlan, int containers, Map<String, Integer>
      componentChanges) throws PackingException {
    if (containers == currentPackingPlan.getContainers().size()) {
      return repack(currentPackingPlan, componentChanges);
    }
    Map<String, Integer> newComponentParallelism = getNewComponentParallelism(currentPackingPlan,
        componentChanges);
    return packInternal(containers, newComponentParallelism);
  }
}
