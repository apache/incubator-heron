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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import com.twitter.heron.api.generated.TopologyAPI;

import com.twitter.heron.spi.common.Constants;
import com.twitter.heron.spi.common.Keys;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Context;
import com.twitter.heron.spi.common.PackingPlan;
import com.twitter.heron.spi.utils.Runtime;
import com.twitter.heron.spi.utils.TopologyUtils;
import com.twitter.heron.spi.packing.IPacking;

/**
 * Round-robin packing algorithm
 */
public class RoundRobinPacking implements IPacking {
  private static final Logger LOG = Logger.getLogger(RoundRobinPacking.class.getName());
  private static final long DEFAULT_DISK_PADDING = 12 * Constants.GB;

  protected TopologyAPI.Topology topology;
  protected long stmgrRamDefault ;
  protected long instanceRamDefault;
  protected double instanceCpuDefault; 
  protected long instanceDiskDefault; 

  @Override
  public void initialize(Config config, Config runtime) {
    this.stmgrRamDefault = Context.stmgrRam(config);
    this.instanceRamDefault = Context.instanceRam(config);
    this.instanceCpuDefault = Context.instanceCpu(config).doubleValue(); 
    this.instanceDiskDefault = Context.instanceDisk(config);
    this.topology = Runtime.topology(runtime);
  }

  @Override
  public PackingPlan pack() {
    Map<String, List<String>> containerInstancesMap = getBasePacking();
    return fillInResource(containerInstancesMap);
  }

  @Override
  public void close() {
  }

  private int getLargestContainerSize(Map<String, List<String>> packing) {
    int max = 0;
    for (List<String> instances : packing.values()) {
      if (instances.size() > max) {
        max = instances.size();
      }
    }
    return max;
  }

  /**
   * Provide cpu per container.
   *
   * @param packing packing output.
   * @return cpu per container.
   */
  public double getContainerCpuHint(Map<String, List<String>> packing) {
    List<TopologyAPI.Config.KeyValue> topologyConfig = topology.getTopologyConfig().getKvsList();
    double totalInstanceCpu = instanceCpuDefault * TopologyUtils.getTotalInstance(topology);
    // TODO(nbhagat): Add 1 more cpu for metrics manager also.
    // TODO(nbhagat): Use max cpu here. To get max use packing information.
    double defaultContainerCpu =
        (float) (1 + totalInstanceCpu / TopologyUtils.getNumContainers(topology));

    String cpuHint = TopologyUtils.getConfigWithDefault(
        topologyConfig, com.twitter.heron.api.Config.TOPOLOGY_CONTAINER_CPU_REQUESTED, Double.toString(defaultContainerCpu));

    return Double.parseDouble(cpuHint);
  }

  /**
   * Provide disk per container.
   *
   * @param packing packing output.
   * @return disk per container.
   */
  public long getContainerDiskHint(Map<String, List<String>> packing) {
    long defaultContainerDisk =
        instanceDiskDefault * getLargestContainerSize(packing) + DEFAULT_DISK_PADDING;

    List<TopologyAPI.Config.KeyValue> topologyConfig = topology.getTopologyConfig().getKvsList();

    String diskHint = TopologyUtils.getConfigWithDefault(
        topologyConfig, com.twitter.heron.api.Config.TOPOLOGY_CONTAINER_DISK_REQUESTED, Long.toString(defaultContainerDisk));

    return Long.parseLong(diskHint);
  }

  /**
   * Provide default ram for instances of component whose Ram requirement is not specified.
   *
   * @return default ram in bytes.
   */
  public long getDefaultInstanceRam(Map<String, List<String>> packing) {
    List<TopologyAPI.Config.KeyValue> topologyConfig = topology.getTopologyConfig().getKvsList();

    String containerRam = TopologyUtils.getConfigWithDefault(
        topologyConfig, com.twitter.heron.api.Config.TOPOLOGY_CONTAINER_RAM_REQUESTED, "-1");

    long containerRamRequested = Long.parseLong(containerRam);
    return getDefaultInstanceRam(
        packing, topology, instanceRamDefault, stmgrRamDefault, containerRamRequested);
  }

  /**
   * Check if user has provided ram for some component. Use user provided ram mapping to override
   * default ram assignment from config. Default ram assignment depends on if user has provided
   * by uniformly sharing remaining container's requested ram between all instance. If container
   * ram is not provided than default is set from config.
   *
   * @param packing packing
   * @return Container ram requirement
   */
  public long getContainerRamHint(Map<String, List<String>> packing) {
    List<TopologyAPI.Config.KeyValue> topologyConfig = topology.getTopologyConfig().getKvsList();

    // Get RAM mapping with default instance ram value.
    Map<String, Long> ramMap = TopologyUtils.getComponentRamMap(
        topology, getDefaultInstanceRam(packing));
    // Find container with maximum ram.
    long maxRamRequired = 0;
    for (List<String> instances : packing.values()) {
      long ramRequired = 0;
      for (String instanceId : instances) {
        String componentName = getComponentName(instanceId);
        ramRequired += ramMap.get(componentName);
      }
      if (ramRequired > maxRamRequired) {
        maxRamRequired = ramRequired;
      }
    }
    long defaultRequest = maxRamRequired + stmgrRamDefault;
    long containerRamRequested = Long.parseLong(TopologyUtils.getConfigWithDefault(
        topologyConfig, com.twitter.heron.api.Config.TOPOLOGY_CONTAINER_RAM_REQUESTED, "" + defaultRequest));
    if (defaultRequest > containerRamRequested) {
      LOG.severe("Container is set to value lower than computed defaults. This could be due" +
          "to incorrect RAM map provided for components.");
    }
    return containerRamRequested;
  }


  public int getNumContainers() {
    List<TopologyAPI.Config.KeyValue> topologyConfig = topology.getTopologyConfig().getKvsList();
    return Integer.parseInt(TopologyUtils.getConfigWithDefault(
        topologyConfig, com.twitter.heron.api.Config.TOPOLOGY_STMGRS, "1").trim());
  }

  public String getContainerId(int index) {
    return "" + index;
  }

  public String getInstanceId(
      int containerIdx, String componentName, int instanceIdx, int componentIdx) {
    return String.format("%d:%s:%d:%d", containerIdx, componentName, instanceIdx, componentIdx);
  }

  public PackingPlan fillInResource(Map<String, List<String>> basePacking) {
    Map<String, Long> ramMap = TopologyUtils.getComponentRamMap(
        topology, getDefaultInstanceRam(basePacking));

    Map<String, PackingPlan.ContainerPlan> containerPlanMap = new HashMap<>();

    long containerRam = getContainerRamHint(basePacking);
    long containerDisk = getContainerDiskHint(basePacking);
    double containerCpu = getContainerCpuHint(basePacking);

    for (Map.Entry<String, List<String>> entry : basePacking.entrySet()) {
      String containerId = entry.getKey();
      List<String> instanceList = entry.getValue();

      Map<String, PackingPlan.InstancePlan> instancePlanMap = new HashMap<>();

      for (String instanceId : instanceList) {
        long instanceRam = ramMap.get(getComponentName(instanceId));
        long instanceDisk = 1 * Constants.GB;  // Not used in aurora.

        PackingPlan.Resource resource = new PackingPlan.Resource(instanceCpuDefault, instanceRam, instanceDisk);
        PackingPlan.InstancePlan instancePlan =
            new PackingPlan.InstancePlan(instanceId, getComponentName(instanceId), resource);
        instancePlanMap.put(instanceId, instancePlan);
      }

      PackingPlan.Resource resource = new PackingPlan.Resource(containerCpu, containerRam, containerDisk);
      PackingPlan.ContainerPlan containerPlan =
          new PackingPlan.ContainerPlan(containerId, instancePlanMap, resource);

      containerPlanMap.put(containerId, containerPlan);
    }

    // We also need to take TMASTER container into account
    // TODO(mfu): Figure out a better definition
    int totalContainer = containerPlanMap.size() + 1;
    long topologyRam = totalContainer * containerRam;
    long topologyDisk = totalContainer * containerDisk;
    double topologyCpu = totalContainer * containerCpu;

    PackingPlan.Resource resource = new PackingPlan.Resource(topologyCpu, topologyRam, topologyDisk);
    return new PackingPlan(topology.getId(), containerPlanMap, resource);
  }

  public String getComponentName(String instanceId) {
    return instanceId.split(":")[1];
  }

  // Return: containerId -> list of InstanceId belonging to this container
  private Map<String, List<String>> getBasePacking() {
    Map<String, List<String>> packing = new HashMap<>();
    int numContainer = getNumContainers();
    int totalInstance = TopologyUtils.getTotalInstance(topology);
    if (numContainer > totalInstance) {
      throw new RuntimeException("More containers allocated than instance. Bailing out.");
    }

    for (int i = 1; i <= numContainer; ++i) {
      packing.put(getContainerId(i), new ArrayList<String>());
    }
    int index = 1;
    int globalTaskIndex = 1;
    Map<String, Integer> parallelismMap = TopologyUtils.getComponentParallelism(topology);
    for (String component : parallelismMap.keySet()) {
      int numInstance = parallelismMap.get(component);
      for (int i = 0; i < numInstance; ++i) {
        packing.get(getContainerId(index)).add(getInstanceId(index, component, globalTaskIndex, i));
        index = (index == numContainer) ? 1 : index + 1;
        globalTaskIndex++;
      }
    }
    return packing;
  }

  /**
   * Provide default ram for instances of component whose Ram requirement is not specified.
   *
   * @param instanceRamDefaultValue Default value of instance ram. If no information is specified,
   * This value will be used.
   * @param stmgrRam Ram reserved for stream manager
   * @param containerRamRequested If Container notion is valid then pass that, -1 otherwise.
   * @return default ram in bytes.
   */
  public long getDefaultInstanceRam(Map<String, List<String>> packing,
                                    TopologyAPI.Topology topology,
                                    long instanceRamDefaultValue,
                                    long stmgrRam,
                                    long containerRamRequested) {
    long defaultInstanceRam = instanceRamDefaultValue;

    if (containerRamRequested != -1) {
      Map<String, Long> ramMap = TopologyUtils.getComponentRamMap(topology, -1);
      // Find the minimum possible ram that can be fit in this packing.
      long minInstanceRam = Long.MAX_VALUE;
      for (List<String> instances : packing.values()) {
        Long ramRemaining = containerRamRequested - stmgrRam;  // Remove stmgr.
        int defaultInstance = 0;
        for (String id : instances) {
          if (-1 != ramMap.get(getComponentName(id))) {
            ramRemaining -= ramMap.get(getComponentName(id));  // Remove known ram
          } else {
            defaultInstance++;
          }
        }
        // Evenly distribute remaining ram.
        if (defaultInstance != 0 && minInstanceRam > (ramRemaining / defaultInstance)) {
          minInstanceRam = (ramRemaining / defaultInstance);
        }
      }
      if (minInstanceRam != Integer.MAX_VALUE) {
        defaultInstanceRam = minInstanceRam;
      }
    }
    return defaultInstanceRam;
  }
}
