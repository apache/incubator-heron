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

package org.apache.heron.simulator.utils;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.heron.api.Config;
import org.apache.heron.api.generated.TopologyAPI;
import org.apache.heron.common.basics.TypeUtils;
import org.apache.heron.proto.system.HeronTuples;
import org.apache.heron.proto.system.PhysicalPlans;
import org.apache.heron.simulator.grouping.Grouping;

public class TopologyManager {
  private TopologyAPI.Topology topology;
  private Map<String, List<Integer>> componentToTaskIds;
  private PhysicalPlans.PhysicalPlan physicalPlan;
  private HashMap<TopologyAPI.StreamId, List<Grouping>> streamConsumers;
  private ArrayList<Integer> spoutTasks;

  public TopologyManager(TopologyAPI.Topology topology) {
    this.topology = topology;
  }

  public TopologyAPI.Topology getTopology() {
    return topology;
  }

  /**
   * Want get a PhysicalPlan basing the topology given.
   * It would contain one fake stream mgr/container info. And all instances would be belong to
   * this container.
   *
   * @return Physical Plan containing this topology
   */
  public PhysicalPlans.PhysicalPlan getPhysicalPlan() {
    if (this.physicalPlan == null) {
      PhysicalPlans.PhysicalPlan.Builder pPlanBuilder = PhysicalPlans.PhysicalPlan.newBuilder();

      // Add the topology
      pPlanBuilder.setTopology(this.getTopology());

      // Add fake stream mgr
      PhysicalPlans.StMgr stMgr = PhysicalPlans.StMgr.newBuilder().
          setId("").setHostName("").setDataPort(-1).setLocalEndpoint("").setCwd("").build();
      pPlanBuilder.addStmgrs(stMgr);

      // Add instances
      int globalTaskIndex = 1;
      for (Map.Entry<String, Integer> componentParallelism
          : this.getComponentParallelism().entrySet()) {
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

      this.physicalPlan = pPlanBuilder.build();
    }

    return this.physicalPlan;
  }

  /**
   * Get the map &lt;componentId -&gt; taskIds&gt; from the Physical Plan
   *
   * @return the map from componentId to its task ids
   */
  public Map<String, List<Integer>> getComponentToTaskIds() {
    if (this.componentToTaskIds == null) {
      this.componentToTaskIds = new HashMap<>();

      // Iterate over all instances and insert necessary info into the map
      for (PhysicalPlans.Instance instance : this.getPhysicalPlan().getInstancesList()) {
        int taskId = instance.getInfo().getTaskId();
        String componentName = instance.getInfo().getComponentName();

        if (!this.componentToTaskIds.containsKey(componentName)) {
          this.componentToTaskIds.put(componentName, new ArrayList<Integer>());
        }

        this.componentToTaskIds.get(componentName).add(taskId);
      }
    }

    return this.componentToTaskIds;
  }

  /**
   * Extract the config value "topology.message.timeout.secs" for given topology protobuf
   *
   * @return the config value of "topology.message.timeout.secs"
   */
  public Duration extractTopologyTimeout() {
    for (TopologyAPI.Config.KeyValue keyValue
         : this.getTopology().getTopologyConfig().getKvsList()) {
      if (keyValue.getKey().equals("topology.message.timeout.secs")) {
        return TypeUtils.getDuration(keyValue.getValue(), ChronoUnit.SECONDS);
      }
    }

    throw new IllegalArgumentException("topology.message.timeout.secs does not exist");
  }

  private Map<String, Integer> getComponentParallelism() {
    Map<String, Integer> parallelismMap = new HashMap<>();
    for (TopologyAPI.Spout spout : this.getTopology().getSpoutsList()) {
      String componentName = spout.getComp().getName();
      String parallelism = getConfigWithException(
          spout.getComp().getConfig().getKvsList(), Config.TOPOLOGY_COMPONENT_PARALLELISM).trim();
      parallelismMap.put(componentName, Integer.parseInt(parallelism));
    }
    for (TopologyAPI.Bolt bolt : this.getTopology().getBoltsList()) {
      String componentName = bolt.getComp().getName();
      String parallelism = getConfigWithException(
          bolt.getComp().getConfig().getKvsList(), Config.TOPOLOGY_COMPONENT_PARALLELISM).trim();
      parallelismMap.put(componentName, Integer.parseInt(parallelism));
    }
    return parallelismMap;
  }

  /**
   * Get the stream consumers map that was generated from the topology
   *
   * @return the populated stream consumers' map
   */
  public HashMap<TopologyAPI.StreamId, List<Grouping>> getStreamConsumers() {
    if (this.streamConsumers == null) {
      this.streamConsumers = new HashMap<>();

      // First get a map of (TopologyAPI.StreamId -> TopologyAPI.StreamSchema)
      Map<TopologyAPI.StreamId, TopologyAPI.StreamSchema> streamToSchema =
          new HashMap<>();

      // Calculate spout's output stream
      for (TopologyAPI.Spout spout : this.getTopology().getSpoutsList()) {
        for (TopologyAPI.OutputStream outputStream : spout.getOutputsList()) {
          streamToSchema.put(outputStream.getStream(), outputStream.getSchema());
        }
      }

      // Calculate bolt's output stream
      for (TopologyAPI.Bolt bolt : this.getTopology().getBoltsList()) {
        for (TopologyAPI.OutputStream outputStream : bolt.getOutputsList()) {
          streamToSchema.put(outputStream.getStream(), outputStream.getSchema());
        }
      }

      // Only bolts could consume from input stream
      for (TopologyAPI.Bolt bolt : this.getTopology().getBoltsList()) {
        for (TopologyAPI.InputStream inputStream : bolt.getInputsList()) {
          TopologyAPI.StreamSchema schema = streamToSchema.get(inputStream.getStream());

          String componentName = bolt.getComp().getName();

          List<Integer> taskIds = this.getComponentToTaskIds().get(componentName);

          if (!this.streamConsumers.containsKey(inputStream.getStream())) {
            this.streamConsumers.put(inputStream.getStream(), new ArrayList<>());
          }

          this.streamConsumers.get(inputStream.getStream()).add(Grouping.create(
              inputStream.getGtype(),
              inputStream,
              schema,
              taskIds)
          );
        }
      }
    }

    return this.streamConsumers;
  }

  public List<Integer> getSpoutTasks() {
    if (this.spoutTasks == null) {
      this.spoutTasks = new ArrayList<>();

      for (TopologyAPI.Spout spout : this.getTopology().getSpoutsList()) {
        for (TopologyAPI.OutputStream outputStream : spout.getOutputsList()) {
          List<Integer> spoutTaskIds = this.getComponentToTaskIds()
              .get(
                  outputStream.getStream().getComponentName()
              );
          this.spoutTasks.addAll(spoutTaskIds);
        }
      }
    }

    return this.spoutTasks;
  }

  public List<Integer> getListToSend(
      TopologyAPI.StreamId streamId,
      HeronTuples.HeronDataTuple tuple
  ) {
    ArrayList<Integer> toReturn = new ArrayList<>();

    List<Grouping> consumers = this.getStreamConsumers().getOrDefault(
        streamId,
        new ArrayList<Grouping>()
    );

    for (Grouping consumer : consumers) {
      toReturn.addAll(consumer.getListToSend(tuple));
    }

    return toReturn;
  }

  private static String getConfigWithException(
      List<TopologyAPI.Config.KeyValue> config, String key) {
    for (TopologyAPI.Config.KeyValue kv : config) {
      if (kv.getKey().equals(key)) {
        return kv.getValue();
      }
    }
    throw new RuntimeException("Missing config for required key " + key);
  }
}
