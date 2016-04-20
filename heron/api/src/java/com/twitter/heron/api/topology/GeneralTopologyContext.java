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

package com.twitter.heron.api.topology;

import java.util.List;
import java.util.Map;
import java.util.Set;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.api.tuple.Fields;

public interface GeneralTopologyContext {
  /**
   * Gets the unique id assigned to this topology. The id is the topology name with a
   * unique nonce appended to it.
   *
   * @return the topology id
   */
  public String getTopologyId();

  /**
   * Gets the Thrift object representing the topology.
   *
   * @return the Thrift definition representing the topology
   */
    /*
    TODO:- This should not be exposed. Take this out
    public HeronTopology getRawTopology() {
        return _topology;
    }
    */

  /**
   * Gets the component id for the specified task id. The component id maps
   * to a component id specified for a Spout or Bolt in the topology definition.
   *
   * @param taskId the task id
   * @return the component id for the input task id
   */
  public String getComponentId(int taskId);

  /**
   * Gets the set of streams declared for the specified component.
   */
  public Set<String> getComponentStreams(String componentId);

  /**
   * Gets the task ids allocated for the given component id. The task ids are
   * always returned in ascending order.
   */
  public List<Integer> getComponentTasks(String componentId);

  /**
   * Gets the declared output fields for the specified component/stream.
   */
  public Fields getComponentOutputFields(String componentId, String streamId);

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
  public Map<TopologyAPI.StreamId, TopologyAPI.Grouping> getSources(String componentId);

  /**
   * Gets information about who is consuming the outputs of the specified component,
   * and how.
   *
   * @return Map from stream id to component id to the Grouping used.
   */
  public Map<String, Map<String, TopologyAPI.Grouping>> getTargets(String componentId);

  /**
   * Gets a map from task id to component id.
   */
  public Map<Integer, String> getTaskToComponent();

  /**
   * Gets a list of all component ids in this topology
   */
  public Set<String> getComponentIds();

    /*
    TODO:- This should not be exposed. Take it out
    public ComponentCommon getComponentCommon(String componentId) {
        return ThriftTopologyUtils.getComponentCommon(getRawTopology(), componentId);
    }
    */

  public int maxTopologyMessageTimeout();
}
