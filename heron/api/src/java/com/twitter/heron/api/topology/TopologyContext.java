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

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.api.hooks.ITaskHook;
import com.twitter.heron.api.metric.CombinedMetric;
import com.twitter.heron.api.metric.ICombiner;
import com.twitter.heron.api.metric.IMetricsRegister;
import com.twitter.heron.api.metric.IReducer;
import com.twitter.heron.api.metric.ReducedMetric;
import com.twitter.heron.api.tuple.Fields;

/**
 * A TopologyContext is given to bolts and spouts in their "prepare" and "open"
 * methods, respectively. This object provides information about the component's
 * place within the topology, such as task ids, inputs and outputs, etc.
 */
public interface TopologyContext extends GeneralTopologyContext, IMetricsRegister {
  /**
   * Gets the task id of this task.
   *
   * @return the task id
   */
  int getThisTaskId();

  /**
   * Gets the component id for this task. The component id maps
   * to a component id specified for a Spout or Bolt in the topology definition.
   */
  String getThisComponentId();

  /**
   * Gets the declared output fields for the specified stream id for the component
   * this task is a part of.
   */
  Fields getThisOutputFields(String streamId);

  /**
   * Gets the set of streams declared for the component of this task.
   */
  Set<String> getThisStreams();

  /**
   * Gets the index of this task id in getComponentTasks(getThisComponentId()).
   * An example use case for this method is determining which task
   * accesses which resource in a distributed resource to ensure an even distribution.
   */
  int getThisTaskIndex();

  /**
   * Gets the declared inputs to this component.
   *
   * @return A map from subscribed component/stream to the grouping subscribed with.
   */
  Map<TopologyAPI.StreamId, TopologyAPI.Grouping> getThisSources();

  /**
   * Gets information about who is consuming the outputs of this component, and how.
   *
   * @return Map from stream id to component id to the Grouping used.
   */
  Map<String, Map<String, TopologyAPI.Grouping>> getThisTargets();

  void setTaskData(String name, Object data);

  Object getTaskData(String name);

    /*
    TODO:- Do we really need this?
    public void setExecutorData(String name, Object data) {
        _executorData.put(name, data);
    }

    public Object getExecutorData(String name) {
        return _executorData.get(name);
    }
    */

  /**
   * Add a Task Hook for this instance
   */
  void addTaskHook(ITaskHook hook);

  /**
   * Get the list of all task hooks
   */
  Collection<ITaskHook> getHooks();

  /*
   * Convenience method for registering ReducedMetric.
   */
  <T> ReducedMetric<T> registerMetric(String name, IReducer<T> reducer, int timeBucketSizeInSecs);

  /*
   * Convenience method for registering CombinedMetric.
   */
  <T> CombinedMetric<T> registerMetric(String name,
                                       ICombiner<T> combiner,
                                       int timeBucketSizeInSecs);
}
