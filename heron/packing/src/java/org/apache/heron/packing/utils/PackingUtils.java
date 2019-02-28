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

package org.apache.heron.packing.utils;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import org.apache.heron.api.generated.TopologyAPI;
import org.apache.heron.api.utils.TopologyUtils;
import org.apache.heron.common.basics.ByteAmount;
import org.apache.heron.spi.packing.Resource;

/**
 * Shared utilities for packing algorithms
 */
public final class PackingUtils {
  private static final Logger LOG = Logger.getLogger(PackingUtils.class.getName());

  // default
  public static final int DEFAULT_CONTAINER_PADDING_PERCENTAGE = 10;
  public static final ByteAmount DEFAULT_CONTAINER_RAM_PADDING = ByteAmount.fromGigabytes(1);
  public static final ByteAmount DEFAULT_CONTAINER_DISK_PADDING = ByteAmount.fromGigabytes(1);
  public static final double DEFAULT_CONTAINER_CPU_PADDING = 1.0;
  public static final int DEFAULT_MAX_NUM_INSTANCES_PER_CONTAINER = 10;

  private PackingUtils() {
  }

  /**
   * Compose the component resource map by reading from user configs or default
   *
   * @param components component names
   * @param componentRamMap user configured component ram map
   * @param componentCpuMap user configured component cpu map
   * @param componentDiskMap user configured component disk map
   * @param defaultInstanceResource default instance resources
   * @return component resource map
   */
  public static Map<String, Resource> getComponentResourceMap(
      Set<String> components,
      Map<String, ByteAmount> componentRamMap,
      Map<String, Double> componentCpuMap,
      Map<String, ByteAmount> componentDiskMap,
      Resource defaultInstanceResource) {
    Map<String, Resource> componentResourceMap = new HashMap<>();
    for (String component : components) {
      ByteAmount instanceRam = componentRamMap.getOrDefault(component,
          defaultInstanceResource.getRam());
      double instanceCpu = componentCpuMap.getOrDefault(component,
          defaultInstanceResource.getCpu());
      ByteAmount instanceDisk = componentDiskMap.getOrDefault(component,
          defaultInstanceResource.getDisk());
      componentResourceMap.put(component, new Resource(instanceCpu, instanceRam, instanceDisk));
    }

    return componentResourceMap;
  }

  public static long increaseBy(long value, int paddingPercentage) {
    return value + (paddingPercentage * value) / 100;
  }

  public static double increaseBy(double value, int paddingPercentage) {
    return value + (paddingPercentage * value) / 100;
  }

  /**
   * Finalize padding by taking Math.max(containerResource * paddingPercent, paddingValue)
   *
   * @param containerResource max container resource
   * @param padding padding value
   * @param paddingPercentage padding percent
   * @return finalized padding amount
   */
  public static Resource finalizePadding(
      Resource containerResource, Resource padding, int paddingPercentage) {
    double cpuPadding = Math.max(padding.getCpu(),
        containerResource.getCpu() * paddingPercentage / 100);
    ByteAmount ramPadding = ByteAmount.fromBytes(Math.max(padding.getRam().asBytes(),
        containerResource.getRam().asBytes() * paddingPercentage / 100));
    ByteAmount diskPadding = ByteAmount.fromBytes(Math.max(padding.getDisk().asBytes(),
        containerResource.getDisk().asBytes() * paddingPercentage / 100));

    return new Resource(cpuPadding, ramPadding, diskPadding);
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
