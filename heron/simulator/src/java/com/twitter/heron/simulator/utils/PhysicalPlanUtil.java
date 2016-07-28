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

package com.twitter.heron.simulator.utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.twitter.heron.api.HeronConfig;
import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.proto.system.PhysicalPlans;

public final class PhysicalPlanUtil {

  private PhysicalPlanUtil() {
  }

  /**
   * Want get a PhysicalPlan basing the topology given.
   * It would contain one fake stream mgr/container info. And all instances would be belong to
   * this container.
   *
   * @param topology The topology protobuf given
   * @return Physical Plan containing this topology
   */
  public static PhysicalPlans.PhysicalPlan getPhysicalPlan(TopologyAPI.Topology topology) {
    PhysicalPlans.PhysicalPlan.Builder pPlanBuilder = PhysicalPlans.PhysicalPlan.newBuilder();

    // Add the topology
    pPlanBuilder.setTopology(topology);

    // Add fake stream mgr
    PhysicalPlans.StMgr stMgr = PhysicalPlans.StMgr.newBuilder().
        setId("").setHostName("").setDataPort(-1).setLocalEndpoint("").setCwd("").build();
    pPlanBuilder.addStmgrs(stMgr);

    // Add instances
    int globalTaskIndex = 1;
    for (Map.Entry<String, Integer> componentParallelism
        : getComponentParallelism(topology).entrySet()) {
      String componentName = componentParallelism.getKey();
      int parallelism = componentParallelism.getValue();

      int componentIndex = 1;
      for (int i = 0; i < parallelism; i++) {
        PhysicalPlans.InstanceInfo instanceInfo =
            PhysicalPlans.InstanceInfo.newBuilder().
                setComponentName(componentName).
                setTaskId(globalTaskIndex).
                setComponentIndex(componentIndex).
                build();

        PhysicalPlans.Instance instance =
            PhysicalPlans.Instance.newBuilder().
                setStmgrId("").
                setInstanceId(String.format("%s_%s", componentName, componentIndex)).
                setInfo(instanceInfo).build();

        pPlanBuilder.addInstances(instance);

        componentIndex++;
        globalTaskIndex++;
      }
    }

    return pPlanBuilder.build();
  }

  /**
   * Get the map &lt;componentId -&gt; taskIds&gt; from the Physical Plan given
   *
   * @param physicalPlan the given Physical Plan
   * @return the map from componentId to its task ids
   */
  public static Map<String, List<Integer>> getComponentToTaskIds(
      PhysicalPlans.PhysicalPlan physicalPlan) {
    Map<String, List<Integer>> componentToTaskIds =
        new HashMap<>();

    // Iterate over all instances and insert necessary info into the map
    for (PhysicalPlans.Instance instance : physicalPlan.getInstancesList()) {
      int taskId = instance.getInfo().getTaskId();
      String componentName = instance.getInfo().getComponentName();

      if (!componentToTaskIds.containsKey(componentName)) {
        componentToTaskIds.put(componentName, new ArrayList<Integer>());
      }

      componentToTaskIds.get(componentName).add(taskId);
    }

    return componentToTaskIds;
  }


  /**
   * Extract the config value "topology.message.timeout.secs" for given topology protobuf
   *
   * @param topology The given topology protobuf
   * @return the config value of "topology.message.timeout.secs"
   */
  public static int extractTopologyTimeout(TopologyAPI.Topology topology) {
    for (TopologyAPI.Config.KeyValue keyValue : topology.getTopologyConfig().getKvsList()) {
      if (keyValue.getKey().equals("topology.message.timeout.secs")) {
        return Integer.parseInt(keyValue.getValue());
      }
    }

    throw new IllegalArgumentException("topology.message.timeout.secs does not exist");
  }

  // TODO(mfu): put it into api package, since it is used by also scheduler package
  public static Map<String, Integer> getComponentParallelism(TopologyAPI.Topology topology) {
    Map<String, Integer> parallelismMap = new HashMap<>();
    for (TopologyAPI.Spout spout : topology.getSpoutsList()) {
      String componentName = spout.getComp().getName();
      String parallelism = getConfigWithException(
          spout.getComp().getConfig().getKvsList(), HeronConfig.TOPOLOGY_COMPONENT_PARALLELISM).trim();
      parallelismMap.put(componentName, Integer.parseInt(parallelism));
    }
    for (TopologyAPI.Bolt bolt : topology.getBoltsList()) {
      String componentName = bolt.getComp().getName();
      String parallelism = getConfigWithException(
          bolt.getComp().getConfig().getKvsList(), HeronConfig.TOPOLOGY_COMPONENT_PARALLELISM).trim();
      parallelismMap.put(componentName, Integer.parseInt(parallelism));
    }
    return parallelismMap;
  }

  // TODO(mfu): put it into api package, since it is used by also scheduler package

  public static String getConfigWithException(
      List<TopologyAPI.Config.KeyValue> config, String key) {
    for (TopologyAPI.Config.KeyValue kv : config) {
      if (kv.getKey().equals(key)) {
        return kv.getValue();
      }
    }
    throw new RuntimeException("Missing config for required key " + key);
  }
}
