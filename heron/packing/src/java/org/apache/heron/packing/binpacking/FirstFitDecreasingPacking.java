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

package org.apache.heron.packing.binpacking;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import org.apache.heron.api.generated.TopologyAPI;
import org.apache.heron.api.utils.TopologyUtils;
import org.apache.heron.packing.AbstractPacking;
import org.apache.heron.packing.builder.Container;
import org.apache.heron.packing.builder.ContainerIdScorer;
import org.apache.heron.packing.builder.HomogeneityScorer;
import org.apache.heron.packing.builder.InstanceCountScorer;
import org.apache.heron.packing.builder.PackingPlanBuilder;
import org.apache.heron.packing.builder.ResourceRequirement;
import org.apache.heron.packing.builder.Scorer;
import org.apache.heron.packing.builder.SortingStrategy;
import org.apache.heron.packing.constraints.InstanceDensityConstraint;
import org.apache.heron.packing.constraints.MinCpuConstraint;
import org.apache.heron.packing.constraints.MinRamConstraint;
import org.apache.heron.packing.constraints.ResourceConstraint;
import org.apache.heron.packing.exceptions.ConstraintViolationException;
import org.apache.heron.packing.exceptions.ResourceExceededException;
import org.apache.heron.packing.utils.PackingUtils;
import org.apache.heron.spi.common.Config;
import org.apache.heron.spi.packing.PackingException;
import org.apache.heron.spi.packing.PackingPlan;
import org.apache.heron.spi.packing.Resource;

import static org.apache.heron.api.Config.TOPOLOGY_PACKING_FFD_SORTING_STRATEGY;

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
 * the org.apache.heron.api.Config.TOPOLOGY_CONTAINER_MAX_CPU_HINT,
 * org.apache.heron.api.Config.TOPOLOGY_CONTAINER_MAX_RAM_HINT,
 * org.apache.heron.api.Config.TOPOLOGY_CONTAINER_MAX_DISK_HINT parameters.
 * If the parameters are not specified then a default value is used for the maximum container
 * size.
 * <p>
 * 3. The user provides a percentage of each container size that will be used for padding
 * through the org.apache.heron.api.Config.TOPOLOGY_CONTAINER_PADDING_PERCENTAGE
 * If the parameter is not specified then a default value of 10 is used (10% of the container size)
 * <p>
 * 4. The RAM required for one instance is calculated as:
 * value in org.apache.heron.api.Config.TOPOLOGY_COMPONENT_RAMMAP if exists, otherwise,
 * the default RAM value for one instance.
 * <p>
 * 5. The CPU required for one instance is calculated as the default CPU value for one instance.
 * <p>
 * 6. The disk required for one instance is calculated as the default disk value for one instance.
 * <p>
 * 7. The RAM required for a container is calculated as:
 * (RAM for instances in container) + (paddingPercentage * RAM for instances in container)
 * <p>
 * 8. The CPU required for a container is calculated as:
 * (CPU for instances in container) + (paddingPercentage * CPU for instances in container)
 * <p>
 * 9. The disk required for a container is calculated as:
 * (disk for instances in container) + ((paddingPercentage * disk for instances in container)
 * <p>
 * 10. The pack() return null if PackingPlan fails to pass the safe check, for instance,
 * the size of RAM for an instance is less than the minimal required value.
 */
public class FirstFitDecreasingPacking extends AbstractPacking {

  private static final Logger LOG = Logger.getLogger(FirstFitDecreasingPacking.class.getName());

  private static final SortingStrategy DEFAULT_SORTING_STRATEGY = SortingStrategy.RAM_FIRST;

  private int numContainers = 0;
  private SortingStrategy sortingStrategy = DEFAULT_SORTING_STRATEGY;

  @Override
  public void initialize(Config config, TopologyAPI.Topology inputTopology) {
    super.initialize(config, inputTopology);
    List<TopologyAPI.Config.KeyValue> topologyConfig = topology.getTopologyConfig().getKvsList();
    this.sortingStrategy = SortingStrategy.valueOf(
        TopologyUtils.getConfigWithDefault(topologyConfig,
            TOPOLOGY_PACKING_FFD_SORTING_STRATEGY, DEFAULT_SORTING_STRATEGY.toString())
            .toUpperCase());
  }

  private PackingPlanBuilder newPackingPlanBuilder(PackingPlan existingPackingPlan) {
    return new PackingPlanBuilder(topology.getId(), existingPackingPlan)
        .setDefaultInstanceResource(defaultInstanceResources)
        .setMaxContainerResource(maxContainerResources)
        .setRequestedContainerPadding(padding)
        .setRequestedComponentResource(componentResourceMap)
        .setInstanceConstraints(Arrays.asList(new MinRamConstraint(), new MinCpuConstraint()))
        .setPackingConstraints(Arrays.asList(new ResourceConstraint(),
            new InstanceDensityConstraint(maxNumInstancesPerContainer)));
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
    } catch (ConstraintViolationException e) {
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
    } catch (ConstraintViolationException e) {
      throw new PackingException("Could not repack instances into existing packing plan", e);
    }

    return planBuilder.build();
  }

  @Override
  public PackingPlan repack(PackingPlan currentPackingPlan, int containers,
                            Map<String, Integer> componentChanges)
      throws PackingException, UnsupportedOperationException {
    throw new UnsupportedOperationException("FirstFitDecreasingPacking does not currently support"
        + " creating a new packing plan with a new number of containers.");
  }

  /**
   * Sort the components in decreasing order based on their RAM requirements
   *
   * @return The sorted list of components and their RAM requirements
   */
  private List<ResourceRequirement> getSortedInstances(Set<String> componentNames) {
    List<ResourceRequirement> resourceRequirements = new ArrayList<>();
    for (String componentName : componentNames) {
      Resource requiredResource = this.componentResourceMap.getOrDefault(componentName,
          defaultInstanceResources);
      resourceRequirements.add(new ResourceRequirement(componentName,
          requiredResource.getRam(), requiredResource.getCpu()));
    }
    Collections.sort(resourceRequirements, sortingStrategy.reversed());

    return resourceRequirements;
  }

  /**
   * Get the instances' allocation based on the First Fit Decreasing algorithm
   *
   * @return Map &lt; containerId, list of InstanceId belonging to this container &gt;
   */
  private PackingPlanBuilder getFFDAllocation(PackingPlanBuilder planBuilder)
      throws ConstraintViolationException {
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
      throws ConstraintViolationException {
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
                                           Map<String, Integer> parallelismMap)
      throws ConstraintViolationException {
    List<ResourceRequirement> resourceRequirements
        = getSortedInstances(parallelismMap.keySet());
    for (ResourceRequirement resourceRequirement : resourceRequirements) {
      String componentName = resourceRequirement.getComponentName();
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

    List<ResourceRequirement> resourceRequirements =
        getSortedInstances(componentsToScaleDown.keySet());

    InstanceCountScorer instanceCountScorer = new InstanceCountScorer();
    ContainerIdScorer containerIdScorer = new ContainerIdScorer(false);

    for (ResourceRequirement resourceRequirement : resourceRequirements) {
      String componentName = resourceRequirement.getComponentName();
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
  private void placeFFDInstance(PackingPlanBuilder planBuilder, String componentName)
      throws ConstraintViolationException {
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
