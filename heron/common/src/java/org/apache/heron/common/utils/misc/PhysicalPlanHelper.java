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

package org.apache.heron.common.utils.misc;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

import org.apache.heron.api.Config;
import org.apache.heron.api.generated.TopologyAPI;
import org.apache.heron.api.grouping.CustomStreamGrouping;
import org.apache.heron.api.utils.Utils;
import org.apache.heron.common.utils.metrics.MetricsCollector;
import org.apache.heron.common.utils.topology.TopologyContextImpl;
import org.apache.heron.proto.system.PhysicalPlans;

/**
 * PhysicalPlanHelper could use to fetch this instance's info according to workId
 */

public class PhysicalPlanHelper {
  private static final Logger LOG = Logger.getLogger(PhysicalPlanHelper.class.getName());

  private final PhysicalPlans.PhysicalPlan pplan;
  private final int myTaskId;
  private final String myComponent;
  private final String hostname;
  private final String myInstanceId;
  private final TopologyAPI.Component component;

  // Map from streamid to number of fields in that stream's schema
  private final Map<String, Integer> outputSchema;
  private final CustomStreamGroupingHelper customGrouper;
  private PhysicalPlans.Instance myInstance;
  private TopologyAPI.Spout mySpout;
  private TopologyAPI.Bolt myBolt;
  private TopologyContextImpl topologyContext;

  private final boolean isTerminatedComponent;

  /**
   * Constructor for physical plan helper
   */
  public PhysicalPlanHelper(PhysicalPlans.PhysicalPlan pplan, String instanceId) {
    this.pplan = pplan;

    // Get my instance
    for (int i = 0; i < pplan.getInstancesCount(); ++i) {
      if (pplan.getInstances(i).getInstanceId().equals(instanceId)) {
        myInstance = pplan.getInstances(i);
      }
    }
    if (myInstance == null) {
      throw new RuntimeException("There was no instance that matched my id " + instanceId);
    }
    myComponent = myInstance.getInfo().getComponentName();
    myTaskId = myInstance.getInfo().getTaskId();
    myInstanceId = myInstance.getInstanceId();

    // Am i a spout or a bolt
    TopologyAPI.Topology topo = pplan.getTopology();
    for (int i = 0; i < topo.getSpoutsCount(); ++i) {
      if (topo.getSpouts(i).getComp().getName().equals(myComponent)) {
        mySpout = topo.getSpouts(i);
        break;
      }
    }
    for (int i = 0; i < topo.getBoltsCount(); ++i) {
      if (topo.getBolts(i).getComp().getName().equals(myComponent)) {
        myBolt = topo.getBolts(i);
        break;
      }
    }
    if (mySpout != null && myBolt != null) {
      throw new RuntimeException("MyTaskId is both a bolt or a spout " + myTaskId);
    }
    if (mySpout == null && myBolt == null) {
      throw new RuntimeException("MyTaskId is neither a bolt or a spout " + myTaskId);
    }

    // setup outputSchema
    outputSchema = new ConcurrentHashMap<String, Integer>();
    List<TopologyAPI.OutputStream> outputs;
    if (mySpout != null) {
      outputs = mySpout.getOutputsList();
      component = mySpout.getComp();
    } else {
      outputs = myBolt.getOutputsList();
      component = myBolt.getComp();
    }
    for (TopologyAPI.OutputStream outputStream : outputs) {
      outputSchema.put(outputStream.getStream().getId(),
          outputStream.getSchema().getKeysCount());
    }

    try {
      this.hostname = InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException e) {
      throw new RuntimeException("GetHostName failed");
    }

    // Do some setup for any custom grouping
    customGrouper = new CustomStreamGroupingHelper();

    // Do we have any bolt that consumes any of my streams using custom grouping
    for (int i = 0; i < topo.getBoltsCount(); ++i) {
      for (TopologyAPI.InputStream inputStream : topo.getBolts(i).getInputsList()) {
        if (inputStream.getStream().getComponentName().equals(myComponent)
            && inputStream.getGtype() == TopologyAPI.Grouping.CUSTOM) {
          // This dude takes my output in custom grouping manner
          // This assumes that this custom grouping object is Java-serialized.
          CustomStreamGrouping customStreamGrouping =
              (CustomStreamGrouping) Utils.deserialize(
                  inputStream.getCustomGroupingObject().toByteArray());
          customGrouper.add(inputStream.getStream().getId(),
              getTaskIdsAsListForComponent(topo.getBolts(i).getComp().getName()),
              customStreamGrouping, myComponent);
        }
      }
    }

    // Check whether it is a terminated bolt
    HashSet<String> terminals = getTerminatedComponentSet();
    this.isTerminatedComponent = terminals.contains(myComponent);
  }

  public void checkOutputSchema(String streamId, List<Object> tuple) {
    // First do some checking to make sure that the number of fields match
    // whats expected
    Integer size = outputSchema.get(streamId);
    if (size == null) {
      throw new RuntimeException(myComponent + " emitting stream " + streamId
          + " but was not declared in declareOutputFields");
    } else if (!size.equals(tuple.size())) {
      throw new RuntimeException("Number of fields emitted in stream " + streamId
          + " does not match whats expected. Expected "
          + Integer.toString(size)
          + " Observed " + Integer.toString(tuple.size()));
    }
    // TODO:- Do more checks wrt type
  }

  public TopologyAPI.TopologyState getTopologyState() {
    return pplan.getTopology().getState();
  }

  public int getMyTaskId() {
    return myTaskId;
  }

  // Accessors

  public String getMyHostname() {
    return hostname;
  }

  public String getMyInstanceId() {
    return myInstanceId;
  }

  public int getMyInstanceIndex() {
    return myInstance.getInfo().getComponentIndex();
  }

  public String getMyComponent() {
    return myComponent;
  }

  public TopologyAPI.Spout getMySpout() {
    return mySpout;
  }

  public TopologyAPI.Bolt getMyBolt() {
    return myBolt;
  }

  public TopologyContextImpl getTopologyContext() {
    return topologyContext;
  }

  public void setTopologyContext(MetricsCollector metricsCollector) {
    topologyContext =
        new TopologyContextImpl(mergeConfigs(pplan.getTopology().getTopologyConfig(), component),
            pplan.getTopology(), makeTaskToComponentMap(), myTaskId, metricsCollector);
  }

  private Map<String, Object> mergeConfigs(TopologyAPI.Config config,
                                           TopologyAPI.Component acomponent) {
    LOG.info("Building configs for component: " + myComponent);

    Map<String, Object> map = new HashMap<>();
    addConfigsToMap(config, map);
    LOG.info("Added topology-level configs: " + map.toString());

    addConfigsToMap(acomponent.getConfig(), map); // Override any component specific configs
    LOG.info("Added component-specific configs: " + map.toString());
    return map;
  }

  private void addConfigsToMap(TopologyAPI.Config config, Map<String, Object> map) {
    for (TopologyAPI.Config.KeyValue kv : config.getKvsList()) {
      if (kv.hasValue()) {
        map.put(kv.getKey(), kv.getValue());
      } else {
        map.put(kv.getKey(), Utils.deserialize(kv.getSerializedValue().toByteArray()));
      }
    }
  }

  private Map<Integer, String> makeTaskToComponentMap() {
    Map<Integer, String> retval = new HashMap<Integer, String>();
    for (PhysicalPlans.Instance instance : pplan.getInstancesList()) {
      retval.put(instance.getInfo().getTaskId(),
          instance.getInfo().getComponentName());
    }
    return retval;
  }

  private List<Integer> getTaskIdsAsListForComponent(String comp) {
    List<Integer> retval = new LinkedList<Integer>();
    for (PhysicalPlans.Instance instance : pplan.getInstancesList()) {
      if (instance.getInfo().getComponentName().equals(comp)) {
        retval.add(instance.getInfo().getTaskId());
      }
    }
    return retval;
  }

  public void prepareForCustomStreamGrouping() {
    customGrouper.prepare(topologyContext);
  }

  public List<Integer> chooseTasksForCustomStreamGrouping(String streamId, List<Object> values) {
    return customGrouper.chooseTasks(streamId, values);
  }

  public boolean isTerminatedComponent() {
    return isTerminatedComponent;
  }

  private HashSet<String> getTerminatedComponentSet() {
    Map<String, TopologyAPI.Spout> spouts = new HashMap<>();
    Map<String, HashSet<String>> prev = new HashMap<>();

    for (TopologyAPI.Spout spout : pplan.getTopology().getSpoutsList()) {
      String name = spout.getComp().getName();
      spouts.put(name, spout);
    }

    // We will build the structure of the topologyBlr - a graph directed from children to parents,
    // by looking only on bolts, since spout will not have parents
    for (TopologyAPI.Bolt bolt : pplan.getTopology().getBoltsList()) {
      String name = bolt.getComp().getName();

      // To get the parent's component to construct a graph of topology structure
      for (TopologyAPI.InputStream inputStream : bolt.getInputsList()) {
        String parent = inputStream.getStream().getComponentName();
        if (prev.containsKey(name)) {
          prev.get(name).add(parent);
        } else {
          HashSet<String> parents = new HashSet<String>();
          parents.add(parent);
          prev.put(name, parents);
        }
      }
    }

    // To find the terminal bolts defined by users and link them with "AggregatorBolt"
    // First, "it" of course needs upstream component, we don't want the isolated bolt
    HashSet<String> terminals = new HashSet<>();
    // Second, "it" should not exists in the prev.valueSet, which means, it has no downstream
    HashSet<String> nonTerminals = new HashSet<>();
    for (HashSet<String> set : prev.values()) {
      nonTerminals.addAll(set);
    }
    // Here we iterate bolt in prev.keySet() rather than bolts.keySet() due to we don't want
    // a isolated bolt, including AggregatorBolt
    for (String bolt : prev.keySet()) {
      if (!nonTerminals.contains(bolt)) {
        terminals.add(bolt);
      }
    }
    // We will also consider the cases with spouts without children
    for (String spout : spouts.keySet()) {
      if (!nonTerminals.contains(spout)) {
        terminals.add(spout);
      }
    }

    return terminals;
  }

  public boolean isCustomGroupingEmpty() {
    return customGrouper.isCustomGroupingEmpty();
  }

  public boolean isTopologyStateful() {
    Map<String, Object> config = topologyContext.getTopologyConfig();
    if (config.get(Config.TOPOLOGY_RELIABILITY_MODE) == null) {
      return false;
    }
    Config.TopologyReliabilityMode mode =
        Config.TopologyReliabilityMode.valueOf(
            String.valueOf(config.get(Config.TOPOLOGY_RELIABILITY_MODE)));

    return Config.TopologyReliabilityMode.EFFECTIVELY_ONCE.equals(mode);
  }

  public boolean isTopologyRunning() {
    return getTopologyState().equals(TopologyAPI.TopologyState.RUNNING);
  }
}

