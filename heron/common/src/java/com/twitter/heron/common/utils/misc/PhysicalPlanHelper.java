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

package com.twitter.heron.common.utils.misc;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.api.grouping.CustomStreamGrouping;
import com.twitter.heron.api.topology.TopologyContext;
import com.twitter.heron.api.utils.Utils;
import com.twitter.heron.common.utils.metrics.MetricsCollector;
import com.twitter.heron.common.utils.topology.TopologyContextImpl;
import com.twitter.heron.proto.system.PhysicalPlans;

/**
 * PhysicalPlanHelper could use to fetch this instance's info according to workId
 */

public class PhysicalPlanHelper {
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

  /**
   * Constructor for physical plan helper
   */
  public PhysicalPlanHelper(
      PhysicalPlans.PhysicalPlan pplan,
      String instanceId) {
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
    outputSchema = new HashMap<String, Integer>();
    List<TopologyAPI.OutputStream> outputs;
    TopologyAPI.Component comp;
    if (mySpout != null) {
      outputs = mySpout.getOutputsList();
      comp = mySpout.getComp();
    } else {
      outputs = myBolt.getOutputsList();
      comp = myBolt.getComp();
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

    component = comp;

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
              GetTaskIdsAsListForComponent(topo.getBolts(i).getComp().getName()),
              customStreamGrouping, myComponent);
        }
      }
    }
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
        new TopologyContextImpl(constructConfig(pplan.getTopology().getTopologyConfig(), component),
            pplan.getTopology(), makeTaskToComponentMap(), myTaskId, metricsCollector);
  }

  private Map<String, Object> constructConfig(TopologyAPI.Config config,
                                              TopologyAPI.Component acomponent) {
    Map<String, Object> retval = new HashMap<String, Object>();
    for (TopologyAPI.Config.KeyValue kv : config.getKvsList()) {
      if (kv.hasValue()) {
        retval.put(kv.getKey(), kv.getValue());
      } else {
        retval.put(kv.getKey(), Utils.deserialize(kv.getJavaSerializedValue().toByteArray()));
      }
    }
    // Override any component specific configs
    for (TopologyAPI.Config.KeyValue kv : acomponent.getConfig().getKvsList()) {
      if (kv.hasValue()) {
        retval.put(kv.getKey(), kv.getValue());
      } else {
        retval.put(kv.getKey(), Utils.deserialize(kv.getJavaSerializedValue().toByteArray()));
      }
    }
    return retval;
  }

  private Map<Integer, String> makeTaskToComponentMap() {
    Map<Integer, String> retval = new HashMap<Integer, String>();
    for (PhysicalPlans.Instance instance : pplan.getInstancesList()) {
      retval.put(instance.getInfo().getTaskId(),
          instance.getInfo().getComponentName());
    }
    return retval;
  }

  private List<Integer> GetTaskIdsAsListForComponent(String comp) {
    List<Integer> retval = new LinkedList<Integer>();
    for (PhysicalPlans.Instance instance : pplan.getInstancesList()) {
      if (instance.getInfo().getComponentName().equals(comp)) {
        retval.add(instance.getInfo().getTaskId());
      }
    }
    return retval;
  }

  public void prepareForCustomStreamGrouping(TopologyContext context) {
    customGrouper.prepare(context);
  }

  public List<Integer> chooseTasksForCustomStreamGrouping(String streamId, List<Object> values) {
    return customGrouper.chooseTasks(streamId, values);
  }
}

