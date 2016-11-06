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

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.spi.common.Constants;
import com.twitter.heron.spi.packing.Resource;
import com.twitter.heron.spi.utils.TopologyUtils;

/**
 * Shared utilities for packing algorithms
 */
public final class PackingUtils {
  private static final Logger LOG = Logger.getLogger(PackingUtils.class.getName());
  private static final long MIN_RAM_PER_INSTANCE = 192L * Constants.MB;

  private PackingUtils() {
  }

  /**
   * Check whether the Instance has enough RAM and whether it can fit within the container limits.
   *
   * @param instanceResources The resources allocated to the instance
   * @return true if the instance is valid, false otherwise
   */
  private static boolean isValidInstance(Resource instanceResources,
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

  public static Resource getResourceRequirement(String component,
                                                Map<String, Long> componentRamMap,
                                                Resource defaultInstanceResource,
                                                Resource maxContainerResource,
                                                int paddingPercentage) {
    long instanceRam = defaultInstanceResource.getRam();
    if (componentRamMap.containsKey(component)) {
      instanceRam = componentRamMap.get(component);
    }
    if (!isValidInstance(defaultInstanceResource.cloneWithRam(instanceRam),
        MIN_RAM_PER_INSTANCE, maxContainerResource, paddingPercentage)) {
      throw new RuntimeException("The topology configuration does not have "
          + "valid resource requirements. Please make sure that the instance resource "
          + "requirements do not exceed the maximum per-container resources.");
    }
    return defaultInstanceResource.cloneWithRam(instanceRam);
  }

  public static long increaseBy(long value, int paddingPercentage) {
    return value + (paddingPercentage * value) / 100;
  }

  public static double increaseBy(double value, int paddingPercentage) {
    return value + (paddingPercentage * value) / 100;
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
