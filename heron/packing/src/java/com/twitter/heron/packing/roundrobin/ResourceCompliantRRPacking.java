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
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.common.basics.ByteAmount;
import com.twitter.heron.packing.ResourceExceededException;
import com.twitter.heron.packing.builder.Container;
import com.twitter.heron.packing.builder.ContainerIdScorer;
import com.twitter.heron.packing.builder.HomogeneityScorer;
import com.twitter.heron.packing.builder.InstanceCountScorer;
import com.twitter.heron.packing.builder.PackingPlanBuilder;
import com.twitter.heron.packing.builder.Scorer;
import com.twitter.heron.packing.utils.PackingUtils;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Context;
import com.twitter.heron.spi.packing.IPacking;
import com.twitter.heron.spi.packing.IRepacking;
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

  static final int DEFAULT_CONTAINER_PADDING_PERCENTAGE = 10;
  private static final int DEFAULT_NUMBER_INSTANCES_PER_CONTAINER = 4;

  private static final Logger LOG = Logger.getLogger(ResourceCompliantRRPacking.class.getName());

  private TopologyAPI.Topology topology;
  private Resource defaultInstanceResources;

  private int numContainers;
  //ContainerId to examine next. It is set to 1 when the
  //algorithm restarts with a new number of containers
  private int containerId;

  private void increaseNumContainers(int additionalContainers) {
    this.numContainers += additionalContainers;
  }

  private void resetToFirstContainer() {
    this.containerId = 1;
  }

  private int nextContainerId(int afterId) {
    return (afterId == numContainers) ? 1 : afterId + 1;
  }

  @Override
  public void initialize(Config config, TopologyAPI.Topology inputTopology) {
    this.topology = inputTopology;
    this.numContainers = TopologyUtils.getNumContainers(topology);
    this.defaultInstanceResources = new Resource(
        Context.instanceCpu(config),
        Context.instanceRam(config),
        Context.instanceDisk(config));
    resetToFirstContainer();
  }

  private PackingPlanBuilder newPackingPlanBuilder(PackingPlan existingPackingPlan) {
    List<TopologyAPI.Config.KeyValue> topologyConfig = topology.getTopologyConfig().getKvsList();

    double defaultCpu = this.defaultInstanceResources.getCpu()
        * DEFAULT_NUMBER_INSTANCES_PER_CONTAINER;
    ByteAmount defaultRam = this.defaultInstanceResources.getRam()
        .multiply(DEFAULT_NUMBER_INSTANCES_PER_CONTAINER);
    ByteAmount defaultDisk = this.defaultInstanceResources.getDisk()
        .multiply(DEFAULT_NUMBER_INSTANCES_PER_CONTAINER);
    int paddingPercentage = TopologyUtils.getConfigWithDefault(topologyConfig,
        TOPOLOGY_CONTAINER_PADDING_PERCENTAGE, DEFAULT_CONTAINER_PADDING_PERCENTAGE);

    Resource maxContainerResources = new Resource(
        TopologyUtils.getConfigWithDefault(topologyConfig, TOPOLOGY_CONTAINER_CPU_REQUESTED,
            (double) Math.round(PackingUtils.increaseBy(defaultCpu, paddingPercentage))),
        TopologyUtils.getConfigWithDefault(topologyConfig, TOPOLOGY_CONTAINER_RAM_REQUESTED,
            defaultRam.increaseBy(paddingPercentage)),
        TopologyUtils.getConfigWithDefault(topologyConfig, TOPOLOGY_CONTAINER_DISK_REQUESTED,
            defaultDisk.increaseBy(paddingPercentage)));

    return new PackingPlanBuilder(topology.getId(), existingPackingPlan)
        .setMaxContainerResource(maxContainerResources)
        .setDefaultInstanceResource(defaultInstanceResources)
        .setRequestedContainerPadding(paddingPercentage)
        .setRequestedComponentRam(TopologyUtils.getComponentRamMapConfig(topology));
  }

  @Override
  public PackingPlan pack() {

    while (true) {
      try {
        PackingPlanBuilder planBuilder = newPackingPlanBuilder(null);
        planBuilder.updateNumContainers(numContainers);
        planBuilder = getResourceCompliantRRAllocation(planBuilder);

        return planBuilder.build();

      } catch (ResourceExceededException e) {
        //Not enough containers. Adjust the number of containers.
        LOG.info(String.format(
            "%s Increasing the number of containers to %s and attempting to place again.",
            e.getMessage(), this.numContainers + 1));
        increaseNumContainers(1);
        resetToFirstContainer();
      }
    }
  }

  /**
   * Get a new packing plan given an existing packing plan and component-level changes.
   *
   * @return new packing plan
   */
  @Override
  public PackingPlan repack(PackingPlan currentPackingPlan, Map<String, Integer> componentChanges) {
    this.numContainers = currentPackingPlan.getContainers().size();
    resetToFirstContainer();

    int additionalContainers = computeNumAdditionalContainers(componentChanges, currentPackingPlan);
    if (additionalContainers > 0) {
      increaseNumContainers(additionalContainers);
      LOG.info(String.format(
          "Allocated %s additional containers for repack bring the number of containers to %s.",
          additionalContainers, this.numContainers));
    }

    while (true) {
      try {
        PackingPlanBuilder planBuilder = newPackingPlanBuilder(currentPackingPlan);
        planBuilder.updateNumContainers(numContainers);
        planBuilder = getResourceCompliantRRAllocation(planBuilder, componentChanges);

        return planBuilder.build();

      } catch (ResourceExceededException e) {
        //Not enough containers. Adjust the number of containers.
        increaseNumContainers(1);
        resetToFirstContainer();
        LOG.info(String.format(
            "%s Increasing the number of containers to %s and attempting packing again.",
            e.getMessage(), this.numContainers));
      }
    }
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
    int totalInstances = TopologyUtils.getTotalInstance(topology);

    if (numContainers > totalInstances) {
      LOG.warning(String.format(
          "More containers requested (%s) than total instances (%s). Reducing containers to %s",
          numContainers, totalInstances, totalInstances));
      numContainers = totalInstances;
      planBuilder.updateNumContainers(numContainers);
    }

    assignInstancesToContainers(planBuilder, parallelismMap, PolicyType.STRICT);
    return planBuilder;
  }

  /**
   * Get the instances' allocation based on the ResourceCompliantRR packing algorithm
   *
   * @return Map &lt; containerId, list of InstanceId belonging to this container &gt;
   */
  private PackingPlanBuilder getResourceCompliantRRAllocation(
      PackingPlanBuilder planBuilder, Map<String, Integer> componentChanges)
      throws ResourceExceededException {

    Map<String, Integer> componentsToScaleDown =
        PackingUtils.getComponentsToScale(componentChanges, PackingUtils.ScalingDirection.DOWN);
    Map<String, Integer> componentsToScaleUp =
        PackingUtils.getComponentsToScale(componentChanges, PackingUtils.ScalingDirection.UP);

    if (!componentsToScaleDown.isEmpty()) {
      resetToFirstContainer();
      removeInstancesFromContainers(planBuilder, componentsToScaleDown);
    }

    if (!componentsToScaleUp.isEmpty()) {
      resetToFirstContainer();
      assignInstancesToContainers(planBuilder, componentsToScaleUp, PolicyType.FLEXIBLE);
    }
    return planBuilder;
  }

  /**
   * Assigns instances to containers.
   *
   * @param planBuilder packing plan builder
   * @param parallelismMap component parallelism
   */
  private void assignInstancesToContainers(PackingPlanBuilder planBuilder,
                                           Map<String, Integer> parallelismMap,
                                           PolicyType policyType) throws ResourceExceededException {
    for (String componentName : parallelismMap.keySet()) {
      int numInstance = parallelismMap.get(componentName);
      for (int i = 0; i < numInstance; ++i) {
        policyType.assignInstance(planBuilder, componentName, this);
      }
    }
  }

  /**
   * Attempts to place the instance the current containerId.
   *
   * @param planBuilder packing plan builder
   * @param componentName the componet name of the instance that needs to be placed in the container
   * @throws ResourceExceededException if there is no room on the current container for the instance
   */
  private void strictRRpolicy(PackingPlanBuilder planBuilder,
                              String componentName) throws ResourceExceededException {
    planBuilder.addInstance(this.containerId, componentName);
    this.containerId = nextContainerId(this.containerId);
  }

  /**
   * Performs a RR placement. Tries to place the instance on any container with space, starting at
   * containerId and cycling through the container set until it can be placed.
   *
   * @param planBuilder packing plan builder
   * @param componentName the component name of the instance that needs to be placed in the container
   * @throws ResourceExceededException if there is no room on any container to place the instance
   */
  private void flexibleRRpolicy(PackingPlanBuilder planBuilder,
                                String componentName) throws ResourceExceededException {
    // If there is not enough space on containerId look at other containers in a RR fashion
    // starting from containerId.
    ContainerIdScorer scorer = new ContainerIdScorer(this.containerId, this.numContainers);
    this.containerId = nextContainerId(planBuilder.addInstance(scorer, componentName));
  }

  /**
   * Removes instances from containers during scaling down
   *
   * @param packingPlanBuilder packing plan builder
   * @param componentsToScaleDown scale down factor for the components.
   */
  private void removeInstancesFromContainers(PackingPlanBuilder packingPlanBuilder,
      Map<String, Integer> componentsToScaleDown) {
    for (String componentName : componentsToScaleDown.keySet()) {
      int numInstancesToRemove = -componentsToScaleDown.get(componentName);
      for (int j = 0; j < numInstancesToRemove; j++) {
        removeRRInstance(packingPlanBuilder, componentName);
      }
    }
  }

  /**
   * Remove an instance of a particular component from the containers
   */
  private void removeRRInstance(PackingPlanBuilder packingPlanBuilder,
                                String componentName) throws RuntimeException {
    List<Scorer<Container>> scorers = new ArrayList<>();
    scorers.add(new HomogeneityScorer(componentName, true));  // all-same-component containers first
    scorers.add(new InstanceCountScorer());                   // then fewest instances
    scorers.add(new HomogeneityScorer(componentName, false)); // then most homogeneous
    scorers.add(new ContainerIdScorer(false));                // then highest container id

    this.containerId = nextContainerId(packingPlanBuilder.removeInstance(scorers, componentName));
  }

  private enum PolicyType {
    STRICT, FLEXIBLE;

    private void assignInstance(PackingPlanBuilder planBuilder,
                                String componentName,
                                ResourceCompliantRRPacking packing)
        throws ResourceExceededException, RuntimeException {
      switch (this) {
        case STRICT:
          packing.strictRRpolicy(planBuilder, componentName);
          break;
        case FLEXIBLE:
          packing.flexibleRRpolicy(planBuilder, componentName);
          break;
        default:
          throw new RuntimeException("Not valid policy type");
      }
    }
  }
}


