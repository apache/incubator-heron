package com.twitter.heron.scheduler.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import com.twitter.heron.api.Config;
import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.scheduler.api.Constants;
import com.twitter.heron.scheduler.api.IPackingAlgorithm;
import com.twitter.heron.scheduler.api.PackingPlan;
import com.twitter.heron.scheduler.api.context.LaunchContext;

/**
 * Round-robin packing algorithm
 */
public class RoundRobinPacking implements IPackingAlgorithm {
  private static final Logger LOG = Logger.getLogger(RoundRobinPacking.class.getName());
  private static final long DEFAULT_DISK_PADDING = 12 * Constants.GB;
  public static final String STMGR_RAM_DEFAULT = "stmgr.ram.default";
  public static final String INSTANCE_RAM_DEFAULT = "instance.ram.default";
  public static final String INSTANCE_CPU_DEFAULT = "instance.cpu.default";
  public static final String INSTANCE_DISK_DEFAULT = "instance.disk.default";
  public TopologyAPI.Topology topology;
  public LaunchContext context;

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
   * Provide cpu per aurora container.
   *
   * @param packing packing output.
   * @return cpu per aurora container.
   */
  public double getContainerCpuHint(Map<String, List<String>> packing) {
    double defaultInstanceCpu = Double.parseDouble(
        context.getProperty(INSTANCE_CPU_DEFAULT, "1.0"));
    List<TopologyAPI.Config.KeyValue> topologyConfig = topology.getTopologyConfig().getKvsList();
    double totalInstanceCpu = defaultInstanceCpu * TopologyUtility.getTotalInstance(topology);
    // TODO(nbhagat): Add 1 more cpu for metrics manager also.
    // TODO(nbhagat): Use max cpu here. To get max use packing information.
    float defaultContainerCpu =
        (float) (1 + totalInstanceCpu / TopologyUtility.getNumContainer(topology));
    return Double.parseDouble(TopologyUtility.getConfigWithDefault(
        topologyConfig, Config.TOPOLOGY_CONTAINER_CPU_REQUESTED, defaultContainerCpu + ""));
  }

  /**
   * Provide disk per aurora container.
   *
   * @param packing packing output.
   * @return disk per aurora container.
   */
  public long getContainerDiskHint(Map<String, List<String>> packing) {
    long defaultInstanceDisk = Long.parseLong(context.getProperty(
        INSTANCE_DISK_DEFAULT, Long.toString(1 * Constants.GB)));
    long defaultContainerDisk =
        defaultInstanceDisk * getLargestContainerSize(packing) + DEFAULT_DISK_PADDING;

    List<TopologyAPI.Config.KeyValue> topologyConfig = topology.getTopologyConfig().getKvsList();

    return Long.parseLong(TopologyUtility.getConfigWithDefault(
        topologyConfig, Config.TOPOLOGY_CONTAINER_DISK_REQUESTED, defaultContainerDisk + ""));
  }

  /**
   * Provide default ram for instances of component whose Ram requirement is not specified.
   *
   * @return default ram in bytes.
   */
  public long getDefaultInstanceRam(Map<String, List<String>> packing) {
    List<TopologyAPI.Config.KeyValue> topologyConfig = topology.getTopologyConfig().getKvsList();
    long defaultInstanceRam = Long.parseLong(context.getProperty(
        INSTANCE_RAM_DEFAULT, Long.toString(1 * Constants.GB)));
    long stmgrRam = Long.parseLong(context.getProperty(STMGR_RAM_DEFAULT, Long.toString(1 * Constants.GB)));

    long containerRamRequested = Long.parseLong(TopologyUtility.getConfigWithDefault(
        topologyConfig, Config.TOPOLOGY_CONTAINER_RAM_REQUESTED, "-1"));
    return getDefaultInstanceRam(
        packing, topology, defaultInstanceRam, stmgrRam, containerRamRequested);
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
    // Get ram mapping with default instance ram value.
    Map<String, Long> ramMap = TopologyUtility.getComponentRamMap(
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
    long stmgrRam = Long.parseLong(context.getProperty(
        STMGR_RAM_DEFAULT, Long.toString(1 * Constants.GB)));
    long defaultRequest = maxRamRequired + stmgrRam;
    long containerRamRequested = Long.parseLong(TopologyUtility.getConfigWithDefault(
        topologyConfig, Config.TOPOLOGY_CONTAINER_RAM_REQUESTED, "" + defaultRequest));
    if (defaultRequest > containerRamRequested) {
      LOG.severe("Container is set to value lower than computed defaults. This could be due" +
          "to incorrect RAM map provided for components.");
    }
    return containerRamRequested;
  }


  public int getNumContainer() {
    List<TopologyAPI.Config.KeyValue> topologyConfig = topology.getTopologyConfig().getKvsList();
    return Integer.parseInt(TopologyUtility.getConfigWithDefault(
        topologyConfig, Config.TOPOLOGY_STMGRS, "1").trim());
  }

  public String getContainerId(int index) {
    return "" + index;
  }

  public String getInstanceId(
      int containerIdx, String componentName, int instanceIdx, int componentIdx) {
    return String.format("%d:%s:%d:%d", containerIdx, componentName, instanceIdx, componentIdx);
  }

  public PackingPlan fillInResource(Map<String, List<String>> basePacking) {
    Map<String, Long> ramMap = TopologyUtility.getComponentRamMap(
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
        double instanceCpu = Double.parseDouble(
            context.getProperty(INSTANCE_CPU_DEFAULT, "1.0"));
        long instanceRam = ramMap.get(getComponentName(instanceId));
        long instanceDisk = 1 * Constants.GB;  // Not used in aurora.

        PackingPlan.Resource resource = new PackingPlan.Resource(instanceCpu, instanceRam, instanceDisk);
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
  public Map<String, List<String>> getBasePacking() {
    Map<String, List<String>> packing = new HashMap<>();
    int numContainer = getNumContainer();
    int totalInstance = TopologyUtility.getTotalInstance(topology);
    if (numContainer > totalInstance) {
      throw new RuntimeException("More containers allocated than instance. Bailing out.");
    }

    for (int i = 1; i <= numContainer; ++i) {
      packing.put(getContainerId(i), new ArrayList<String>());
    }
    int index = 1;
    int globalTaskIndex = 1;
    Map<String, Integer> parallelismMap = TopologyUtility.getComponentParallelism(topology);
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
      Map<String, Long> ramMap = TopologyUtility.getComponentRamMap(topology, -1);
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

  @Override
  public PackingPlan pack(LaunchContext context) {
    this.topology = context.getTopology();
    this.context = context;

    Map<String, List<String>> containerInstancesMap = getBasePacking();
    return fillInResource(containerInstancesMap);
  }
}
