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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import org.apache.heron.api.generated.TopologyAPI;
import org.apache.heron.api.utils.TopologyUtils;
import org.apache.heron.packing.AbstractPacking;
import org.apache.heron.packing.builder.Container;
import org.apache.heron.packing.builder.ContainerIdScorer;
import org.apache.heron.packing.builder.HomogeneityScorer;
import org.apache.heron.packing.builder.InstanceCountScorer;
import org.apache.heron.packing.builder.PackingPlanBuilder;
import org.apache.heron.packing.builder.Scorer;
import org.apache.heron.packing.constraints.MinRamConstraint;
import org.apache.heron.packing.constraints.ResourceConstraint;
import org.apache.heron.packing.exceptions.ConstraintViolationException;
import org.apache.heron.packing.exceptions.ResourceExceededException;
import org.apache.heron.packing.utils.PackingUtils;
import org.apache.heron.spi.common.Config;
import org.apache.heron.spi.packing.PackingException;
import org.apache.heron.spi.packing.PackingPlan;
import org.apache.heron.spi.packing.Resource;

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
 * the org.apache.heron.api.Config.TOPOLOGY_CONTAINER_CPU_REQUESTED,
 * org.apache.heron.api.Config.TOPOLOGY_CONTAINER_RAM_REQUESTED,
 * org.apache.heron.api.Config.TOPOLOGY_CONTAINER_DISK_REQUESTED parameters.
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
public class ResourceCompliantRRPacking extends AbstractPacking {
  private static final Logger LOG = Logger.getLogger(ResourceCompliantRRPacking.class.getName());

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
    super.initialize(config, inputTopology);
    this.numContainers = TopologyUtils.getNumContainers(topology);
    resetToFirstContainer();
  }

  private PackingPlanBuilder newPackingPlanBuilder(PackingPlan existingPackingPlan) {
    return new PackingPlanBuilder(topology.getId(), existingPackingPlan)
        .setDefaultInstanceResource(defaultInstanceResources)
        .setMaxContainerResource(maxContainerResources)
        .setRequestedContainerPadding(padding)
        .setRequestedComponentResource(componentResourceMap)
        .setInstanceConstraints(Collections.singletonList(new MinRamConstraint()))
        .setPackingConstraints(Collections.singletonList(new ResourceConstraint()));
  }

  @Override
  public PackingPlan pack() {

    while (true) {
      try {
        PackingPlanBuilder planBuilder = newPackingPlanBuilder(null);
        planBuilder.updateNumContainers(numContainers);
        planBuilder = getResourceCompliantRRAllocation(planBuilder);

        return planBuilder.build();

      } catch (ConstraintViolationException e) {
        //Not enough containers. Adjust the number of containers.
        LOG.finest(String.format(
            "%s Increasing the number of containers to %s and attempting to place again.",
            e.getMessage(), this.numContainers + 1));
        retryWithAdditionalContainer();
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

      } catch (ConstraintViolationException e) {
        //Not enough containers. Adjust the number of containers.
        LOG.info(String.format(
            "%s Increasing the number of containers to %s and attempting to repack again.",
            e.getMessage(), this.numContainers + 1));
        retryWithAdditionalContainer();
      }
    }
  }

  private void retryWithAdditionalContainer() {
    increaseNumContainers(1);
    resetToFirstContainer();

    int totalInstances = TopologyUtils.getTotalInstance(topology);
    if (numContainers > totalInstances) {
      throw new PackingException("Cannot add to that container");
    }
  }

  @Override
  public PackingPlan repack(PackingPlan currentPackingPlan, int containers,
                            Map<String, Integer> componentChanges)
      throws PackingException, UnsupportedOperationException {
    throw new UnsupportedOperationException("ResourceCompliantRRPacking does not "
        + "currently support creating a new packing plan with a new number of containers.");
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
      PackingPlanBuilder planBuilder) throws ConstraintViolationException {

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
      throws ConstraintViolationException {

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
                                           PolicyType policyType)
      throws ConstraintViolationException {
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
   * @param componentName the component name of the instance that needs to be placed in the container
   * @throws ResourceExceededException if there is no room on the current container for the instance
   */
  private void strictRRpolicy(PackingPlanBuilder planBuilder,
                              String componentName) throws ConstraintViolationException {
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
        throws ConstraintViolationException, RuntimeException {
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


