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
package com.twitter.heron.packing.utils;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.common.basics.ByteAmount;
import com.twitter.heron.spi.packing.PackingException;
import com.twitter.heron.spi.packing.Resource;
import com.twitter.heron.spi.utils.TopologyUtils;

/**
 * Shared utilities for packing algorithms
 */
public final class PackingUtils {
  private static final Logger LOG = Logger.getLogger(PackingUtils.class.getName());
  private static final ByteAmount MIN_RAM_PER_INSTANCE = ByteAmount.fromMegabytes(192);

  private PackingUtils() {
  }

  /**
   * Verifies the Instance has enough RAM and that it can fit within the container limits.
   *
   * @param instanceResources The resources allocated to the instance
   * @throws PackingException if the instance is invalid
   */
  private static void assertIsValidInstance(Resource instanceResources,
                                            ByteAmount minInstanceRam,
                                            Resource maxContainerResources,
                                            int paddingPercentage) throws PackingException {

    if (instanceResources.getRam().lessThan(minInstanceRam)) {
      throw new PackingException(String.format(
          "Instance requires ram %s which is less than the minimum ram per instance of %s",
          instanceResources.getRam(), minInstanceRam));
    }

    ByteAmount instanceRam = instanceResources.getRam().increaseBy(paddingPercentage);
    if (instanceRam.greaterThan(maxContainerResources.getRam())) {
      throw new PackingException(String.format(
          "This instance requires containers of at least %s ram. The current max container "
              + "size is %s",
          instanceRam, maxContainerResources.getRam()));
    }

    double instanceCpu = Math.round(PackingUtils.increaseBy(
        instanceResources.getCpu(), paddingPercentage));
    if (instanceCpu > maxContainerResources.getCpu()) {
      throw new PackingException(String.format(
          "This instance requires containers with at least %s cpu cores. The current max container"
              + "size is %s cores",
          instanceCpu > maxContainerResources.getCpu(), maxContainerResources.getCpu()));
    }

    ByteAmount instanceDisk = instanceResources.getDisk().increaseBy(paddingPercentage);
    if (instanceDisk.greaterThan(maxContainerResources.getDisk())) {
      throw new PackingException(String.format(
          "This instance requires containers of at least %s disk. The current max container"
              + "size is %s",
          instanceDisk, maxContainerResources.getDisk()));
    }
  }

  public static Resource getResourceRequirement(String component,
                                                Map<String, ByteAmount> componentRamMap,
                                                Resource defaultInstanceResource,
                                                Resource maxContainerResource,
                                                int paddingPercentage) {
    ByteAmount instanceRam = defaultInstanceResource.getRam();
    if (componentRamMap.containsKey(component)) {
      instanceRam = componentRamMap.get(component);
    }
    assertIsValidInstance(defaultInstanceResource.cloneWithRam(instanceRam),
        MIN_RAM_PER_INSTANCE, maxContainerResource, paddingPercentage);
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
    ByteAmount ram = ByteAmount.ZERO;
    ByteAmount disk = ByteAmount.ZERO;
    Map<String, ByteAmount> ramMap = TopologyUtils.getComponentRamMapConfig(topology);
    Map<String, Integer> componentsToScale = PackingUtils.getComponentsToScale(
        componentChanges, scalingDirection);
    for (String component : componentsToScale.keySet()) {
      int parallelismChange = Math.abs(componentChanges.get(component));
      cpu += parallelismChange * defaultInstanceResources.getCpu();
      disk = disk.plus(defaultInstanceResources.getDisk().multiply(parallelismChange));
      if (ramMap.containsKey(component)) {
        ram = ram.plus(ramMap.get(component).multiply(parallelismChange));
      } else {
        ram = ram.plus(defaultInstanceResources.getRam().multiply(parallelismChange));
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
