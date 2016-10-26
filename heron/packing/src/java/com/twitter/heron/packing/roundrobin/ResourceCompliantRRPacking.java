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

import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.packing.PackingPlanBuilder;
import com.twitter.heron.packing.PackingUtils;
import com.twitter.heron.packing.ResourceExceededException;
import com.twitter.heron.spi.common.Config;
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
    PackingPlanBuilder planBuilder =
        new PackingPlanBuilder(topology.getId())
            .setMaxContainerResource(maxContainerResources)
            .setDefaultInstanceResource(defaultInstanceResources)
            .setRequestedContainerPadding(paddingPercentage)
            .setRequestedComponentRam(TopologyUtils.getComponentRamMapConfig(topology));

    int adjustments = this.numAdjustments;
    while (adjustments <= this.numAdjustments) {
      try {
        planBuilder.resetNumContainers(numContainers);
        planBuilder = getResourceCompliantRRAllocation(planBuilder);
        return planBuilder.build();
      } catch (ResourceExceededException e) {
        //Not enough containers. Adjust the number of containers.
        LOG.info(String.format("Increasing the number of containers to "
            + "%s and attempting packing again.", this.numContainers + 1));
        adjustNumContainers(1);
        adjustments++;
      }
    }
    return null; // TODO: should throw instead
  }

  /**
   * Get a new packing plan given an existing packing plan and component-level changes.
   *
   * @return new packing plan
   */
  public PackingPlan repack(PackingPlan currentPackingPlan, Map<String, Integer> componentChanges) {
    this.numContainers = currentPackingPlan.getContainers().size();
    resetState();

    int additionalContainers = computeNumAdditionalContainers(componentChanges, currentPackingPlan);
    increaseNumContainers(additionalContainers);
    LOG.info(String.format(
        "Allocated %s additional containers for repack bring the number of containers to %s.",
        additionalContainers, this.numContainers));

    PackingPlanBuilder planBuilder =
        new PackingPlanBuilder(topology.getId(), currentPackingPlan)
            .setMaxContainerResource(maxContainerResources)
            .setDefaultInstanceResource(defaultInstanceResources)
            .setRequestedContainerPadding(paddingPercentage)
            .setRequestedComponentRam(TopologyUtils.getComponentRamMapConfig(topology));

    int adjustments = 0;
    while (adjustments <= this.numAdjustments) {
      try {
        planBuilder.resetNumContainers(numContainers);
        planBuilder = getResourceCompliantRRAllocation(
            planBuilder, currentPackingPlan, componentChanges);
        return planBuilder.build();
      } catch (ResourceExceededException e) {
        //Not enough containers. Adjust the number of containers.
        LOG.info(String.format("Increasing the number of containers to "
            + "%s and attempting packing again.", this.numContainers + 1));
        adjustNumContainers(1);
        adjustments++;
      }
    }
    return null; // TODO: should throw instead
  }

  @Override
  public void close() {
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

  private PackingPlanBuilder getResourceCompliantRRAllocation(
      PackingPlanBuilder planBuilder) throws ResourceExceededException {

    Map<String, Integer> parallelismMap = TopologyUtils.getComponentParallelism(topology);
    int totalInstance = TopologyUtils.getTotalInstance(topology);

    if (numContainers > totalInstance) {
      throw new RuntimeException("More containers allocated than instances."
          + numContainers + " allocated to host " + totalInstance + " instances.");
    }

    assignInstancesToContainers(planBuilder, parallelismMap, 1, PolicyType.STRICT);
    return planBuilder;
  }

  /**
   * Get the instances' allocation based on the ResourceCompliantRR packing algorithm
   *
   * @return Map &lt; containerId, list of InstanceId belonging to this container &gt;
   */
  private PackingPlanBuilder getResourceCompliantRRAllocation(
      PackingPlanBuilder planBuilder, PackingPlan currentPackingPlan,
      Map<String, Integer> componentChanges) throws ResourceExceededException {
    Map<String, Integer> componentsToScaleDown =
        PackingUtils.getComponentsToScale(componentChanges, PackingUtils.ScalingDirection.DOWN);
    Map<String, Integer> componentsToScaleUp =
        PackingUtils.getComponentsToScale(componentChanges, PackingUtils.ScalingDirection.UP);

    if (!componentsToScaleDown.isEmpty()) {
      this.containerId = 1;
      removeInstancesFromContainers(planBuilder, componentsToScaleDown);
    }

    if (!componentsToScaleUp.isEmpty()) {
      this.containerId = 1;
      int maxInstanceIndex = 0;
      for (PackingPlan.ContainerPlan containerPlan : currentPackingPlan.getContainers()) {
        for (PackingPlan.InstancePlan instancePlan : containerPlan.getInstances()) {
          maxInstanceIndex = Math.max(maxInstanceIndex, instancePlan.getTaskId());
        }
      }
      assignInstancesToContainers(
          planBuilder, componentsToScaleUp, maxInstanceIndex + 1, PolicyType.FLEXIBLE);
    }
    return planBuilder;
  }

  /**
   * Assigns instances to containers.
   *
   * @param planBuilder packing plan builder
   * @param parallelismMap component parallelism
   * @param firstTaskIndex first taskId to use for the new instances
   */
  private void assignInstancesToContainers(PackingPlanBuilder planBuilder,
                                           Map<String, Integer> parallelismMap,
                                           int firstTaskIndex,
                                           PolicyType policyType) throws ResourceExceededException {
    int globalTaskIndex = firstTaskIndex;
    for (String componentName : parallelismMap.keySet()) {
      int numInstance = parallelismMap.get(componentName);
      for (int i = 0; i < numInstance; ++i) {
        InstanceId instanceId = new InstanceId(componentName, globalTaskIndex, i);
        policyType.assignInstance(planBuilder, instanceId, this);
        globalTaskIndex++;
      }
    }
  }

  /**
   * Performs a RR placement. If the placement cannot be performed on the existing number of containers
   * then it will request for an increase in the number of containers
   *
   * @param planBuilder packing plan builder
   * @param instanceId the instance that needs to be placed in the container
   * @return true if the existing number of containers is sufficient, false otherwise
   */
  private boolean strictRRpolicy(PackingPlanBuilder planBuilder,
                                 InstanceId instanceId) throws ResourceExceededException {
    try {
      planBuilder.addInstance(containerId, instanceId);
      containerId = nextContainerId(containerId);
      return true;
    } catch (ResourceExceededException e) {
      //Automatically adjust the number of containers
      containerId = 1;
      throw e;
    }
  }

  private int nextContainerId(int afterId) {
    return (afterId == numContainers) ? 1 : afterId + 1;
  }

  /**
   * Performs a RR placement. If the placement cannot be performed on the existing number of containers
   * then it will request for an increase in the number of containers
   *
   * @param planBuilder packing plan builder
   * @param instanceId the instance that needs to be placed in the container
   */
  private void flexibleRRpolicy(PackingPlanBuilder planBuilder,
                                InstanceId instanceId) throws ResourceExceededException {
    //If there is not enough space on containerId look at other containers in a RR fashion
    // starting from containerId.
    boolean containersChecked = false;
    int currentContainer = containerId;
    while (!containersChecked) {
      try {
        planBuilder.addInstance(currentContainer, instanceId);
        containerId = nextContainerId(currentContainer);
        return;
      } catch (ResourceExceededException e) {
        currentContainer = nextContainerId(currentContainer);
        if (currentContainer == containerId) {
          containersChecked = true;
        }
      }
    }

    //Not enough containers.
    containerId = 1; // TODO: necessary?
    throw new ResourceExceededException(); //TODO
  }

  /**
   * Removes instances from containers during scaling down
   *
   * @param packingPlanBuilder packing plan builder
   * @param componentsToScaleDown scale down factor for the components.
   */
  private void removeInstancesFromContainers(PackingPlanBuilder packingPlanBuilder,
      Map<String, Integer> componentsToScaleDown) {
    for (String component : componentsToScaleDown.keySet()) {
      int numInstancesToRemove = -componentsToScaleDown.get(component);
      for (int j = 0; j < numInstancesToRemove; j++) {
        removeRRInstance(packingPlanBuilder, component);
      }
    }
  }

  /**
   * Remove an instance of a particular component from the containers
   *
   */
  private void removeRRInstance(PackingPlanBuilder packingPlanBuilder,
                                String component) throws RuntimeException {
    boolean containersChecked = false;
    int currentContainer = this.containerId;
    while (!containersChecked) {
      if (packingPlanBuilder.removeInstance(currentContainer, component)) {
        containerId = nextContainerId(currentContainer);
        return;
      }
      currentContainer = nextContainerId(currentContainer);
      if (currentContainer == containerId) {
        containersChecked = true;
      }
    }
    throw new RuntimeException("Cannot remove instance."
        + " No more instances of component " + component + " exist"
        + " in the containers.");
  }

  private enum PolicyType {
    STRICT, FLEXIBLE;

    private void assignInstance(PackingPlanBuilder planBuilder,
                                InstanceId instanceId,
                                ResourceCompliantRRPacking packing)
        throws ResourceExceededException, RuntimeException {
      switch (this) {
        case STRICT:
          packing.strictRRpolicy(planBuilder, instanceId);
          break;
        case FLEXIBLE:
          packing.flexibleRRpolicy(planBuilder, instanceId);
          break;
        default:
          throw new RuntimeException("Not valid policy type");
      }
    }
  }
}


