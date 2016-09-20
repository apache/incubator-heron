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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import com.twitter.heron.spi.common.Constants;
import com.twitter.heron.spi.packing.InstanceId;
import com.twitter.heron.spi.packing.PackingPlan;
import com.twitter.heron.spi.packing.Resource;

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
                                        Resource maxContainerResources) {

    if (instanceResources.getRam() < minInstanceRam) {
      LOG.severe(String.format(
          "Instance requires %d MB ram which is less than the minimum %d MB ram per instance",
          instanceResources.getRam(), minInstanceRam / Constants.MB));
      return false;
    }

    if (instanceResources.getRam() > maxContainerResources.getRam()) {
      LOG.severe(String.format(
          "This instance requires containers of at least %d MB ram. The current max container"
              + "size is %d MB",
          instanceResources.getRam(), maxContainerResources.getRam()));
      return false;
    }

    if (instanceResources.getCpu() > maxContainerResources.getCpu()) {
      LOG.severe(String.format(
          "This instance requires containers with at least %s cpu cores. The current max container"
              + "size is %s cores",
          instanceResources.getCpu(), maxContainerResources.getCpu()));
      return false;
    }

    if (instanceResources.getDisk() > maxContainerResources.getDisk()) {
      LOG.severe(String.format(
          "This instance requires containers of at least %d MB disk. The current max container"
              + "size is %d MB",
          instanceResources.getDisk(), maxContainerResources.getDisk()));
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
   * @param containers
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

}
