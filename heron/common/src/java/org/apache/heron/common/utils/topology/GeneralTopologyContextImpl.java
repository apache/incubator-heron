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

package org.apache.heron.common.utils.topology;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.heron.api.Config;
import org.apache.heron.api.generated.TopologyAPI;
import org.apache.heron.api.topology.GeneralTopologyContext;
import org.apache.heron.api.tuple.Fields;
import org.apache.heron.common.basics.TypeUtils;

/**
 * From Heron. To get the topology info.
 */

public class GeneralTopologyContextImpl implements GeneralTopologyContext {

  private final TopologyAPI.Topology topology;

  // The topology as supplied by the cluster overloaded by any
  // component specific config
  private final Map<String, Object> topologyConfig;

  // Map from taskid to Component Id.
  private final Map<Integer, String> taskToComponentMap;

  // Map from component id to list of its inputs
  private final Map<String, List<TopologyAPI.InputStream>> inputs;

  // Map from component id to list of its outputs
  private final Map<String, List<TopologyAPI.OutputStream>> outputs;

  // Map <componentId -> <streamId -> Fields>>
  private final Map<String, Map<String, Fields>> componentsOutputFields;

  public GeneralTopologyContextImpl(Map<String, Object> clusterConfig,
                                    TopologyAPI.Topology topology,
                                    Map<Integer, String> taskToComponentMap) {
    this.topology = topology;
    this.topologyConfig = new HashMap<>(clusterConfig);
    this.taskToComponentMap = taskToComponentMap;
    this.inputs = new ConcurrentHashMap<>();
    this.outputs = new ConcurrentHashMap<>();
    this.componentsOutputFields = new HashMap<>();

    for (int i = 0; i < this.topology.getSpoutsCount(); ++i) {
      TopologyAPI.Spout spout = this.topology.getSpouts(i);

      // spouts don't have any inputs
      this.inputs.put(spout.getComp().getName(), new LinkedList<TopologyAPI.InputStream>());
      this.outputs.put(spout.getComp().getName(), spout.getOutputsList());
      this.componentsOutputFields.putAll(getOutputToComponentsFields(spout.getOutputsList()));
    }

    for (int i = 0; i < this.topology.getBoltsCount(); ++i) {
      TopologyAPI.Bolt bolt = this.topology.getBolts(i);
      this.inputs.put(bolt.getComp().getName(), bolt.getInputsList());
      this.outputs.put(bolt.getComp().getName(), bolt.getOutputsList());
      this.componentsOutputFields.putAll(getOutputToComponentsFields(bolt.getOutputsList()));
    }
  }

  public static Map<String, Map<String, Fields>> getOutputToComponentsFields(
      List<TopologyAPI.OutputStream> outputs) {
    Map<String, Map<String, Fields>> outputFields = new HashMap<>();
    for (TopologyAPI.OutputStream outputStream : outputs) {
      String componentName = outputStream.getStream().getComponentName();
      String streamId = outputStream.getStream().getId();

      Map<String, Fields> componentFields = outputFields.get(componentName);
      if (componentFields == null) {
        componentFields = new HashMap<>();
      }

      // Get the fields of a particular OutputStream
      List<String> retval = new ArrayList<>();
      for (TopologyAPI.StreamSchema.KeyType kt : outputStream.getSchema().getKeysList()) {
        retval.add(kt.getKey());
      }

      // Put it into the map
      componentFields.put(streamId, new Fields(retval));
      outputFields.put(componentName, componentFields);
    }

    return outputFields;
  }

  // accessors
  public Map<String, Object> getTopologyConfig() {
    return topologyConfig;
  }

  /**
   * Gets the unique id assigned to this topology. The id is the topology name with a
   * unique nonce appended to it.
   *
   * @return the topology id
   */
  public String getTopologyId() {
    return topology.getId();
  }

  @Override
  @SuppressWarnings("deprecation")
  public TopologyAPI.Topology getRawTopology() {
    return topology;
  }

  /**
   * Gets the component id for the specified task id. The component id maps
   * to a component id specified for a Spout or Bolt in the topology definition.
   *
   * @param taskId the task id
   * @return the component id for the input task id
   */
  public String getComponentId(int taskId) {
    if (taskToComponentMap.containsKey(taskId)) {
      return taskToComponentMap.get(taskId);
    } else {
      return null;
    }
  }

  /**
   * Gets the set of streams declared for the specified component.
   */
  public Set<String> getComponentStreams(String componentId) {
    if (outputs.containsKey(componentId)) {
      Set<String> streams = new HashSet<>();
      List<TopologyAPI.OutputStream> olist = outputs.get(componentId);
      for (TopologyAPI.OutputStream ostream : olist) {
        streams.add(ostream.getStream().getId());
      }
      return streams;
    } else {
      return null;
    }
  }

  /**
   * Gets the task ids allocated for the given component id. The task ids are
   * always returned in ascending order.
   */
  public List<Integer> getComponentTasks(String componentId) {
    List<Integer> retVal = new LinkedList<>();
    for (Map.Entry<Integer, String> entry : taskToComponentMap.entrySet()) {
      if (entry.getValue().equals(componentId)) {
        retVal.add(entry.getKey());
      }
    }
    return retVal;
  }

  /**
   * Gets the declared output fields for the specified component/stream.
   */
  public Fields getComponentOutputFields(String componentId, String streamId) {
    Map<String, Fields> componentFields = componentsOutputFields.get(componentId);
    if (componentFields != null) {
      return componentFields.get(streamId);
    }

    return null;
  }

  /**
   * Gets the declared output fields for the specified global stream id.
   */
  /*
  TODO:- Do we really need this? The above function shd cover it
  public Fields getComponentOutputFields(GlobalStreamId id);
  */

  /**
   * Gets the declared inputs to the specified component.
   *
   * @return A map from subscribed component/stream to the grouping subscribed with.
   */
  public Map<TopologyAPI.StreamId, TopologyAPI.Grouping> getSources(String componentId) {
    if (inputs.containsKey(componentId)) {
      Map<TopologyAPI.StreamId, TopologyAPI.Grouping> retVal =
          new HashMap<>();
      for (TopologyAPI.InputStream istream : inputs.get(componentId)) {
        retVal.put(istream.getStream(), istream.getGtype());
      }
      return retVal;
    } else {
      return null;
    }
  }

  /**
   * Gets information about who is consuming the outputs of the specified component,
   * and how.
   *
   * @return Map from stream id to component id to the Grouping used.
   */
  public Map<String, Map<String, TopologyAPI.Grouping>> getTargets(String componentId) {
    Map<String, Map<String, TopologyAPI.Grouping>> retVal =
        new HashMap<>();
    if (!outputs.containsKey(componentId)) {
      return retVal;
    }
    for (TopologyAPI.OutputStream ostream : outputs.get(componentId)) {
      Map<String, TopologyAPI.Grouping> targetMap =
          new HashMap<>();
      for (Map.Entry<String, List<TopologyAPI.InputStream>> e : inputs.entrySet()) {
        String targetComponentId = e.getKey();
        for (TopologyAPI.InputStream is : e.getValue()) {
          if (areStreamsEqual(ostream.getStream(), is.getStream())) {
            targetMap.put(targetComponentId, is.getGtype());
          }
        }
      }
      retVal.put(ostream.getStream().getId(), targetMap);
    }
    return retVal;
  }

  /**
   * Gets a map from task id to component id.
   */
  public Map<Integer, String> getTaskToComponent() {
    return taskToComponentMap;
  }

  /**
   * Gets a list of all component ids in this topology
   */
  public Set<String> getComponentIds() {
    return inputs.keySet();
  }

  /*
  TODO:- This should not be exposed. Take it out
  public ComponentCommon getComponentCommon(String componentId) {
      return ThriftTopologyUtils.getComponentCommon(getRawTopology(), componentId);
  }
  */

  public int maxTopologyMessageTimeout() {
    // TODO:- get the per component overrides implemented
    return TypeUtils.getInteger(topologyConfig.get(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS));
  }

  private boolean areStreamsEqual(TopologyAPI.StreamId a, TopologyAPI.StreamId b) {
    return a.getId().equals(b.getId()) && a.getComponentName().equals(b.getComponentName());
  }
}

