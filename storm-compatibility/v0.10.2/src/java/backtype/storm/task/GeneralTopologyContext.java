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

package backtype.storm.task;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.json.simple.JSONAware;

import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;

// import backtype.storm.generated.ComponentCommon;
// import backtype.storm.generated.GlobalStreamId;
// import backtype.storm.generated.Grouping;
// import backtype.storm.utils.ThriftTopologyUtils;

public class GeneralTopologyContext implements JSONAware {
  private org.apache.heron.api.topology.GeneralTopologyContext delegate;

  @SuppressWarnings("rawtypes")
  public GeneralTopologyContext(StormTopology topology, Map stormConf,
                                Map<Integer, String> taskToComponent,
                                Map<String, List<Integer>> componentToSortedTasks,
                                Map<String, Map<String, Fields>> componentToStreamToFields,
                                String stormId) {
    throw new RuntimeException("GeneralTopologyContext should never be initiated this way");
  }

  public GeneralTopologyContext(org.apache.heron.api.topology.GeneralTopologyContext delegate) {
    this.delegate = delegate;
  }

  /**
   * Gets the unique id assigned to this topology. The id is the storm name with a
   * unique nonce appended to it.
   *
   * @return the storm id
   */
  public String getStormId() {
    return delegate.getTopologyId();
  }

  /**
   * Gets the Thrift object representing the topology.
   *
   * @return the Thrift definition representing the topology
   */
  /*
  public StormTopology getRawTopology() {
    return null;
  }
  */

  /**
   * Gets the component id for the specified task id. The component id maps
   * to a component id specified for a Spout or Bolt in the topology definition.
   *
   * @param taskId the task id
   * @return the component id for the input task id
   */
  public String getComponentId(int taskId) {
    return delegate.getComponentId(taskId);
  }

  /**
   * Gets the set of streams declared for the specified component.
   * @param componentId component id
   * @return the set of streams
   */
  public Set<String> getComponentStreams(String componentId) {
    return delegate.getComponentStreams(componentId);
  }

  /**
   * Gets the task ids allocated for the given component id. The task ids are
   * always returned in ascending order.
   * @param componentId the component id
   * @return  the task ids
   */
  public List<Integer> getComponentTasks(String componentId) {
    return delegate.getComponentTasks(componentId);
  }

  /**
   * Gets the declared output fields for the specified component/stream.
   * @param  componentId the component id
   * @param  streamId the steam id
   * @return the declared output fields
   */
  public Fields getComponentOutputFields(String componentId, String streamId) {
    return new Fields(delegate.getComponentOutputFields(componentId, streamId));
  }

  /**
   * Gets the declared output fields for the specified global stream id.
   */
  /*
  public Fields getComponentOutputFields(GlobalStreamId id) {
  }
  */

  /**
   * Gets the declared inputs to the specified component.
   *
   * @return A map from subscribed component/stream to the grouping subscribed with.
   */
  /*
  public Map<GlobalStreamId, Grouping> getSources(String componentId) {
    return getComponentCommon(componentId).get_inputs();
  }
  */

  /**
   * Gets information about who is consuming the outputs of the specified component,
   * and how.
   *
   * @return Map from stream id to component id to the Grouping used.
   */
  /*
    public Map<String, Map<String, Grouping>> getTargets(String componentId) {
        Map<String, Map<String, Grouping>> ret = new HashMap<String, Map<String, Grouping>>();
        for(String otherComponentId: getComponentIds()) {
            Map<GlobalStreamId, Grouping> inputs =
              getComponentCommon(otherComponentId).get_inputs();
            for(GlobalStreamId id: inputs.keySet()) {
                if(id.get_componentId().equals(componentId)) {
                    Map<String, Grouping> curr = ret.get(id.get_streamId());
                    if(curr==null) curr = new HashMap<String, Grouping>();
                    curr.put(otherComponentId, inputs.get(id));
                    ret.put(id.get_streamId(), curr);
                }
            }
        }
        return ret;
    }
  */
  @Override
  public String toJSONString() {
    throw new RuntimeException("toJSONString not implemented");
  }

  /**
   * Gets a map from task id to component id.
   * @return a map from task id to component id
   */
  public Map<Integer, String> getTaskToComponent() {
    return delegate.getTaskToComponent();
  }

  /**
   * Gets a list of all component ids in this topology
   * @return the list of component ids in this topology
   */
  public Set<String> getComponentIds() {
    return delegate.getComponentIds();
  }

  /*
    public ComponentCommon getComponentCommon(String componentId) {
        return ThriftTopologyUtils.getComponentCommon(getRawTopology(), componentId);
    }
  */

  /*
    public int maxTopologyMessageTimeout() {
        Integer max = Utils.getInteger(_stormConf.get(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS));
        for(String spout: getRawTopology().get_spouts().keySet()) {
            ComponentCommon common = getComponentCommon(spout);
            String jsonConf = common.get_json_conf();
            if(jsonConf!=null) {
                Map conf = (Map) JSONValue.parse(jsonConf);
                Object comp = conf.get(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS);
                if(comp!=null) {
                    max = Math.max(Utils.getInteger(comp), max);
                }
            }
        }
        return max;
    }
  */
}
