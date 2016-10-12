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
package com.twitter.heron.packing;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.spi.common.Constants;
import com.twitter.heron.spi.packing.InstanceId;
import com.twitter.heron.spi.packing.PackingPlan;
import com.twitter.heron.spi.packing.Resource;
import com.twitter.heron.spi.utils.TopologyUtils;

/**
 * Shared utilities for packing algorithms
 */
public final class PackingUtils {
  private static final Logger LOG = Logger.getLogger(PackingUtils.class.getName());

  private PackingUtils() {
  }

  /**
   * Check whether the Instance has enough RAM and whether it can fit within the container limits.
   *
   * @param instanceResources The resources allocated to the instance
   * @return true if the instance is valid, false otherwise
   */
  public static boolean isValidInstance(Resource instanceResources,
                                        long minInstanceRam,
                                        Resource maxContainerResources,
                                        int paddingPercentage) {

    if (instanceResources.getRam() < minInstanceRam) {
      LOG.severe(String.format(
          "Instance requires %d MB ram which is less than the minimum %d MB ram per instance",
          instanceResources.getRam(), minInstanceRam / Constants.MB));
      return false;
    }

    long instanceRam = PackingUtils.increaseBy(instanceResources.getRam(), paddingPercentage);
    if (instanceRam > maxContainerResources.getRam()) {
      LOG.severe(String.format(
          "This instance requires containers of at least %d MB ram. The current max container"
              + "size is %d MB",
          instanceRam, maxContainerResources.getRam()));
      return false;
    }

    double instanceCpu = Math.round(PackingUtils.increaseBy(
        instanceResources.getCpu(), paddingPercentage));
    if (instanceCpu > maxContainerResources.getCpu()) {
      LOG.severe(String.format(
          "This instance requires containers with at least %s cpu cores. The current max container"
              + "size is %s cores",
          instanceCpu > maxContainerResources.getCpu(), maxContainerResources.getCpu()));
      return false;
    }

    long instanceDisk = PackingUtils.increaseBy(instanceResources.getDisk(), paddingPercentage);
    if (instanceDisk > maxContainerResources.getDisk()) {
      LOG.severe(String.format(
          "This instance requires containers of at least %d MB disk. The current max container"
              + "size is %d MB",
          instanceDisk, maxContainerResources.getDisk()));
      return false;
    }
    return true;
  }

  /**
   * Estimate the per instance and topology resources for the packing plan based on the ramMap,
   * instance defaults and paddingPercentage.
   *
   * @return container plans
   */
  public static Set<PackingPlan.ContainerPlan> buildContainerPlans(
      Map<Integer, List<InstanceId>> containerInstances,
      Map<String, Long> ramMap,
      Resource instanceDefaults,
      double paddingPercentage) {
    Set<PackingPlan.ContainerPlan> containerPlans = new HashSet<>();

    for (Integer containerId : containerInstances.keySet()) {
      List<InstanceId> instanceList = containerInstances.get(containerId);

      long containerRam = 0;
      long containerDiskInBytes = 0;
      double containerCpu = 0;

      // Calculate the resource required for single instance
      Set<PackingPlan.InstancePlan> instancePlans = new HashSet<>();

      for (InstanceId instanceId : instanceList) {
        long instanceRam = 0;
        if (ramMap.containsKey(instanceId.getComponentName())) {
          instanceRam = ramMap.get(instanceId.getComponentName());
        } else {
          instanceRam = instanceDefaults.getRam();
        }
        containerRam += instanceRam;

        // Currently not yet support disk or cpu config for different components,
        // so just use the default value.
        long instanceDisk = instanceDefaults.getDisk();
        containerDiskInBytes += instanceDisk;

        double instanceCpu = instanceDefaults.getCpu();
        containerCpu += instanceCpu;

        // Insert it into the map
        instancePlans.add(new PackingPlan.InstancePlan(instanceId,
            new Resource(instanceCpu, instanceRam, instanceDisk)));
      }

      containerCpu += (paddingPercentage * containerCpu) / 100;
      containerRam += (paddingPercentage * containerRam) / 100;
      containerDiskInBytes += (paddingPercentage * containerDiskInBytes) / 100;

      Resource resource =
          new Resource(Math.round(containerCpu), containerRam, containerDiskInBytes);

      PackingPlan.ContainerPlan containerPlan =
          new PackingPlan.ContainerPlan(containerId, instancePlans, resource);

      containerPlans.add(containerPlan);
    }

    return containerPlans;
  }

  /**
   * Sort the container plans based on the container Ids
   *
   * @return sorted array of container plans
   */
  public static PackingPlan.ContainerPlan[] sortOnContainerId(
      Set<PackingPlan.ContainerPlan> containers) {
    ArrayList<Integer> containerIds = new ArrayList<>();
    PackingPlan.ContainerPlan[] currentContainers =
        new PackingPlan.ContainerPlan[containers.size()];
    for (PackingPlan.ContainerPlan container : containers) {
      containerIds.add(container.getId());
    }
    Collections.sort(containerIds);
    for (PackingPlan.ContainerPlan container : containers) {
      int position = containerIds.indexOf(container.getId());
      currentContainers[position] = container;
    }
    return currentContainers;
  }

  public static long increaseBy(long value, int paddingPercentage) {
    return value + (paddingPercentage * value) / 100;
  }

  public static double increaseBy(double value, int paddingPercentage) {
    return value + (paddingPercentage * value) / 100;
  }

  /**
   * Allocate a new container of a given capacity
   *
   * @return the number of containers
   */
  public static int allocateNewContainer(ArrayList<Container> containers, Resource capacity,
                                         int paddingPercentage) {
    containers.add(new Container(capacity, paddingPercentage));
    return containers.size();
  }

  /**
   * Identifies which components need to be scaled given specific scaling direction
   *
   * @return Map &lt; component name, scale factor &gt;
   */
  public static Map<String, Integer> getComponentsToScale(Map<String,
      Integer> componentChanges, ScalingDirection scalingDirection) {
    Map<String, Integer> componentsToScale = new HashMap<String, Integer>();
    for (String component : componentChanges.keySet()) {
      int parallelismChange = componentChanges.get(component);
      if (scalingDirection.includes(parallelismChange)) {
        componentsToScale.put(component, parallelismChange);
      }
    }
    return componentsToScale;
  }

  /**
   * Identifies the resources reclaimed by the components that will be scaled down
   *
   * @return Total resources reclaimed
   */
  public static Resource computeTotalResourceChange(TopologyAPI.Topology topology,
                                                    Map<String, Integer> componentChanges,
                                                    Resource defaultInstanceResources,
                                                    ScalingDirection scalingDirection) {
    double cpu = 0;
    long ram = 0;
    long disk = 0;
    Map<String, Long> ramMap = TopologyUtils.getComponentRamMapConfig(topology);
    Map<String, Integer> componentsToScale = PackingUtils.getComponentsToScale(
        componentChanges, scalingDirection);
    for (String component : componentsToScale.keySet()) {
      int parallelismChange = Math.abs(componentChanges.get(component));
      cpu += parallelismChange * defaultInstanceResources.getCpu();
      disk += parallelismChange * defaultInstanceResources.getDisk();
      if (ramMap.containsKey(component)) {
        ram += parallelismChange * ramMap.get(component);
      } else {
        ram += parallelismChange * defaultInstanceResources.getRam();
      }
    }
    return new Resource(cpu, ram, disk);
  }

  /**
   * Removes containers from tha allocation that do not contain any instances
   */
  public static void removeEmptyContainers(Map<Integer, List<InstanceId>> allocation) {
    Iterator<Integer> containerIds = allocation.keySet().iterator();
    while (containerIds.hasNext()) {
      Integer containerId = containerIds.next();
      if (allocation.get(containerId).isEmpty()) {
        containerIds.remove();
      }
    }
  }

  /**
   * Generates the containers that correspond to the current packing plan
   * along with their associated instances.
   *
   * @return List of containers for the current packing plan
   */
  public static ArrayList<Container> getContainers(PackingPlan currentPackingPlan,
                                                   int paddingPercentage) {
    ArrayList<Container> containers = new ArrayList<>();

    //sort containers based on containerIds;
    PackingPlan.ContainerPlan[] currentContainers =
        PackingUtils.sortOnContainerId(currentPackingPlan.getContainers());

    Resource capacity = currentPackingPlan.getMaxContainerResources();
    for (int i = 0; i < currentContainers.length; i++) {
      int containerId = PackingUtils.allocateNewContainer(
          containers, capacity, paddingPercentage);
      for (PackingPlan.InstancePlan instancePlan
          : currentContainers[i].getInstances()) {
        containers.get(containerId - 1).add(instancePlan);
      }
    }
    return containers;
  }


  /**
   * Generates an instance allocation for the current packing plan
   *
   * @return Map &lt; containerId, list of InstanceId belonging to this container &gt;
   */
  public static Map<Integer, List<InstanceId>> getAllocation(PackingPlan currentPackingPlan) {
    Map<Integer, List<InstanceId>> allocation = new HashMap<Integer, List<InstanceId>>();
    for (PackingPlan.ContainerPlan containerPlan : currentPackingPlan.getContainers()) {
      ArrayList<InstanceId> instances = new ArrayList<InstanceId>();
      for (PackingPlan.InstancePlan instance : containerPlan.getInstances()) {
        instances.add(new InstanceId(instance.getComponentName(), instance.getTaskId(),
            instance.getComponentIndex()));
      }
      allocation.put(containerPlan.getId(), instances);
    }
    return allocation;
  }

  public enum ScalingDirection {
    UP,
    DOWN;

    boolean includes(int parallelismChange) {
      switch (this) {
        case UP:
          return parallelismChange > 0;
        case DOWN:
          return parallelismChange < 0;
        default:
          throw new IllegalArgumentException(String.format("Not valid parallelism change: %d",
              parallelismChange));
      }
    }
  }
}
