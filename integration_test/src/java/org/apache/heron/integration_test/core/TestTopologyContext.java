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

package org.apache.heron.integration_test.core;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.heron.api.generated.TopologyAPI;
import org.apache.heron.api.hooks.ITaskHook;
import org.apache.heron.api.metric.CombinedMetric;
import org.apache.heron.api.metric.ICombiner;
import org.apache.heron.api.metric.IMetric;
import org.apache.heron.api.metric.IReducer;
import org.apache.heron.api.metric.ReducedMetric;
import org.apache.heron.api.topology.TopologyContext;
import org.apache.heron.api.tuple.Fields;

public class TestTopologyContext implements TopologyContext {
  private TopologyContext delegate;

  public TestTopologyContext(TopologyContext topologyContext) {
    this.delegate = topologyContext;
  }

  @Override
  public int getThisTaskId() {
    return this.delegate.getThisTaskId();
  }

  @Override
  public String getThisComponentId() {
    return this.delegate.getThisComponentId();
  }

  @Override
  public Fields getThisOutputFields(String streamId) {
    return this.delegate.getThisOutputFields(streamId);
  }

  @Override
  public Set<String> getThisStreams() {
    return this.delegate.getThisStreams();
  }

  @Override
  public int getThisTaskIndex() {
    return this.delegate.getThisTaskIndex();
  }

  /**
   * remove INTEGRATION_TEST_CONTROL_STREAM_ID from topology context
   */
  @Override
  public Map<TopologyAPI.StreamId, TopologyAPI.Grouping> getThisSources() {
    Map<TopologyAPI.StreamId, TopologyAPI.Grouping> original = getSources(getThisComponentId());
    Map<TopologyAPI.StreamId, TopologyAPI.Grouping> ret = new HashMap<>();
    for (Map.Entry<TopologyAPI.StreamId, TopologyAPI.Grouping> entry : original.entrySet()) {
      if (!entry.getKey().getId().equals(Constants.INTEGRATION_TEST_CONTROL_STREAM_ID)) {
        ret.put(entry.getKey(), entry.getValue());
      }
    }
    return ret;
  }

  @Override
  public Map<String, Map<String, TopologyAPI.Grouping>> getThisTargets() {
    return this.delegate.getThisTargets();
  }

  @Override
  public void setTaskData(String name, Object data) {
    this.delegate.setTaskData(name, data);
  }

  @Override
  public Object getTaskData(String name) {
    return this.delegate.getTaskData(name);
  }

  @Override
  public void addTaskHook(ITaskHook hook) {
    this.delegate.addTaskHook(hook);
  }

  @Override
  public Collection<ITaskHook> getHooks() {
    return this.delegate.getHooks();
  }

  @Override
  public <T, U, V> ReducedMetric<T, U, V> registerMetric(String name, IReducer<T, U, V> reducer,
                                                         int timeBucketSizeInSecs) {
    return this.delegate.registerMetric(name, reducer, timeBucketSizeInSecs);
  }

  @Override
  public <T> CombinedMetric<T> registerMetric(String name, ICombiner<T> combiner, int
      timeBucketSizeInSecs) {
    return this.delegate.registerMetric(name, combiner, timeBucketSizeInSecs);
  }

  @Override
  public <T extends IMetric<U>, U> T registerMetric(String name, T metric, int
      timeBucketSizeInSecs) {
    return this.delegate.registerMetric(name, metric, timeBucketSizeInSecs);
  }

  @Override
  public String getTopologyId() {
    return this.delegate.getTopologyId();
  }

  @Override
  @SuppressWarnings("deprecation")
  public TopologyAPI.Topology getRawTopology() {
    return delegate.getRawTopology();
  }

  @Override
  public String getComponentId(int taskId) {
    return this.delegate.getComponentId(taskId);
  }

  @Override
  public Set<String> getComponentStreams(String componentId) {
    return this.delegate.getComponentStreams(componentId);
  }

  @Override
  public List<Integer> getComponentTasks(String componentId) {
    return this.delegate.getComponentTasks(componentId);
  }

  @Override
  public Fields getComponentOutputFields(String componentId, String streamId) {
    return this.delegate.getComponentOutputFields(componentId, streamId);
  }

  @Override
  public Map<TopologyAPI.StreamId, TopologyAPI.Grouping> getSources(String componentId) {
    return this.delegate.getSources(componentId);
  }

  @Override
  public Map<String, Map<String, TopologyAPI.Grouping>> getTargets(String componentId) {
    return this.delegate.getTargets(componentId);
  }

  @Override
  public Map<Integer, String> getTaskToComponent() {
    return this.delegate.getTaskToComponent();
  }

  @Override
  public Set<String> getComponentIds() {
    return this.delegate.getComponentIds();
  }

  @Override
  public int maxTopologyMessageTimeout() {
    return this.delegate.maxTopologyMessageTimeout();
  }
}
