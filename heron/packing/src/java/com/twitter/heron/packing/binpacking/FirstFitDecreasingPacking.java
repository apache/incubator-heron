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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.common.basics.ByteAmount;
import com.twitter.heron.packing.RamRequirement;
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
import com.twitter.heron.spi.packing.PackingException;
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

  private static final int DEFAULT_CONTAINER_PADDING_PERCENTAGE = 10;
  private static final int DEFAULT_NUMBER_INSTANCES_PER_CONTAINER = 4;

  private static final Logger LOG = Logger.getLogger(FirstFitDecreasingPacking.class.getName());

  private TopologyAPI.Topology topology;
  private Resource defaultInstanceResources;
  private Resource maxContainerResources;
  private int paddingPercentage;

  private int numContainers = 0;

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
    ByteAmount defaultRam = this.defaultInstanceResources.getRam()
        .multiply(DEFAULT_NUMBER_INSTANCES_PER_CONTAINER);
    ByteAmount defaultDisk = this.defaultInstanceResources.getDisk()
        .multiply(DEFAULT_NUMBER_INSTANCES_PER_CONTAINER);

    this.maxContainerResources = new Resource(
        TopologyUtils.getConfigWithDefault(topologyConfig, TOPOLOGY_CONTAINER_MAX_CPU_HINT,
            (double) Math.round(PackingUtils.increaseBy(defaultCpu, paddingPercentage))),
        TopologyUtils.getConfigWithDefault(topologyConfig, TOPOLOGY_CONTAINER_MAX_RAM_HINT,
            defaultRam.increaseBy(paddingPercentage)),
        TopologyUtils.getConfigWithDefault(topologyConfig, TOPOLOGY_CONTAINER_MAX_DISK_HINT,
            defaultDisk.increaseBy(paddingPercentage)));
  }

  private PackingPlanBuilder newPackingPlanBuilder(PackingPlan existingPackingPlan) {
    return new PackingPlanBuilder(topology.getId(), existingPackingPlan)
        .setMaxContainerResource(maxContainerResources)
        .setDefaultInstanceResource(defaultInstanceResources)
        .setRequestedContainerPadding(paddingPercentage)
        .setRequestedComponentRam(TopologyUtils.getComponentRamMapConfig(topology));
  }

  /**
   * Get a packing plan using First Fit Decreasing
   *
   * @return packing plan
   */
  @Override
  public PackingPlan pack() {
    PackingPlanBuilder planBuilder = newPackingPlanBuilder(null);

    // Get the instances using FFD allocation
    try {
      planBuilder = getFFDAllocation(planBuilder);
    } catch (ResourceExceededException e) {
      throw new PackingException("Could not allocate all instances to packing plan", e);
    }

    return planBuilder.build();
  }

  /**
   * Get a new packing plan given an existing packing plan and component-level changes.
   * @return new packing plan
   */
  public PackingPlan repack(PackingPlan currentPackingPlan, Map<String, Integer> componentChanges) {
    PackingPlanBuilder planBuilder = newPackingPlanBuilder(currentPackingPlan);

    // Get the instances using FFD allocation
    try {
      planBuilder = getFFDAllocation(planBuilder, currentPackingPlan, componentChanges);
    } catch (ResourceExceededException e) {
      throw new PackingException("Could not repack instances into existing packing plan", e);
    }

    return planBuilder.build();
  }

  @Override
  public void close() {

  }

  /**
   * Sort the components in decreasing order based on their RAM requirements
   *
   * @return The sorted list of components and their RAM requirements
   */
  private ArrayList<RamRequirement> getSortedRAMInstances(Set<String> componentNames) {
    ArrayList<RamRequirement> ramRequirements = new ArrayList<>();
    Map<String, ByteAmount> ramMap = TopologyUtils.getComponentRamMapConfig(topology);

    for (String componentName : componentNames) {
      Resource requiredResource = PackingUtils.getResourceRequirement(
          componentName, ramMap, this.defaultInstanceResources,
          this.maxContainerResources, this.paddingPercentage);
      ramRequirements.add(new RamRequirement(componentName, requiredResource.getRam()));
    }
    Collections.sort(ramRequirements, Collections.reverseOrder());

    return ramRequirements;
  }

  /**
   * Get the instances' allocation based on the First Fit Decreasing algorithm
   *
   * @return Map &lt; containerId, list of InstanceId belonging to this container &gt;
   */
  private PackingPlanBuilder getFFDAllocation(PackingPlanBuilder planBuilder)
      throws ResourceExceededException {
    Map<String, Integer> parallelismMap = TopologyUtils.getComponentParallelism(topology);
    assignInstancesToContainers(planBuilder, parallelismMap);
    return planBuilder;
  }

  /**
   * Get the instances' allocation based on the First Fit Decreasing algorithm
   *
   * @return Map &lt; containerId, list of InstanceId belonging to this container &gt;
   */
  private PackingPlanBuilder getFFDAllocation(PackingPlanBuilder packingPlanBuilder,
                                              PackingPlan currentPackingPlan,
                                              Map<String, Integer> componentChanges)
      throws ResourceExceededException {
    this.numContainers = currentPackingPlan.getContainers().size();

    Map<String, Integer> componentsToScaleDown =
        PackingUtils.getComponentsToScale(componentChanges, PackingUtils.ScalingDirection.DOWN);
    Map<String, Integer> componentsToScaleUp =
        PackingUtils.getComponentsToScale(componentChanges, PackingUtils.ScalingDirection.UP);

    if (!componentsToScaleDown.isEmpty()) {
      removeInstancesFromContainers(packingPlanBuilder, componentsToScaleDown);
    }

    if (!componentsToScaleUp.isEmpty()) {
      assignInstancesToContainers(packingPlanBuilder, componentsToScaleUp);
    }

    return packingPlanBuilder;
  }

  /**
   * Assigns instances to containers
   *
   * @param planBuilder existing packing plan
   * @param parallelismMap component parallelism
   */
  private void assignInstancesToContainers(PackingPlanBuilder planBuilder,
      Map<String, Integer> parallelismMap) throws ResourceExceededException {
    ArrayList<RamRequirement> ramRequirements = getSortedRAMInstances(parallelismMap.keySet());
    for (RamRequirement ramRequirement : ramRequirements) {
      String componentName = ramRequirement.getComponentName();
      int numInstance = parallelismMap.get(componentName);
      for (int j = 0; j < numInstance; j++) {
        placeFFDInstance(planBuilder, componentName);
      }
    }
  }

  /**
   * Removes instances from containers during scaling down
   *
   * @param packingPlanBuilder existing packing plan
   * @param componentsToScaleDown scale down factor for the components.
   */
  private void removeInstancesFromContainers(PackingPlanBuilder packingPlanBuilder,
                                             Map<String, Integer> componentsToScaleDown) {

    ArrayList<RamRequirement> ramRequirements =
        getSortedRAMInstances(componentsToScaleDown.keySet());

    InstanceCountScorer instanceCountScorer = new InstanceCountScorer();
    ContainerIdScorer containerIdScorer = new ContainerIdScorer(false);

    for (RamRequirement ramRequirement : ramRequirements) {
      String componentName = ramRequirement.getComponentName();
      int numInstancesToRemove = -componentsToScaleDown.get(componentName);
      List<Scorer<Container>> scorers = new ArrayList<>();

      scorers.add(new HomogeneityScorer(componentName, true));  // all-same-component containers
      scorers.add(instanceCountScorer);                         // then fewest instances
      scorers.add(new HomogeneityScorer(componentName, false)); // then most homogeneous
      scorers.add(containerIdScorer);                           // then highest container id

      for (int j = 0; j < numInstancesToRemove; j++) {
        packingPlanBuilder.removeInstance(scorers, componentName);
      }
    }
  }

  /**
   * Assign a particular instance to an existing container or to a new container
   *
   */
  private void placeFFDInstance(PackingPlanBuilder planBuilder,
                                String componentName) throws ResourceExceededException {
    if (this.numContainers == 0) {
      planBuilder.updateNumContainers(++numContainers);
    }

    try {
      planBuilder.addInstance(new ContainerIdScorer(), componentName);
    } catch (ResourceExceededException e) {
      planBuilder.updateNumContainers(++numContainers);
      planBuilder.addInstance(numContainers, componentName);
    }
  }
}
