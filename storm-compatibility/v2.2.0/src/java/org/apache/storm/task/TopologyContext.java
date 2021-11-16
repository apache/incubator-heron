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

package org.apache.storm.task;

// import org.apache.storm.generated.GlobalStreamId;
// import org.apache.storm.generated.Grouping;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.storm.generated.StormTopology;
import org.apache.storm.hooks.ITaskHook;
import org.apache.storm.hooks.ITaskHookDelegate;
import org.apache.storm.metric.api.CombinedMetric;
import org.apache.storm.metric.api.ICombiner;
import org.apache.storm.metric.api.IMetric;
import org.apache.storm.metric.api.IReducer;
import org.apache.storm.metric.api.MetricDelegate;
import org.apache.storm.metric.api.ReducedMetric;
import org.apache.storm.tuple.Fields;

// import org.apache.storm.state.ISubscribedState;

// import org.apache.storm.state.ISubscribedState;
// import org.apache.commons.lang.NotImplementedException;

/**
 * A TopologyContext is given to bolts and spouts in their "prepare" and "open"
 * methods, respectively. This object provides information about the component's
 * place within the topology, such as task ids, inputs and outputs, etc.
 * <p>The TopologyContext is also used to declare ISubscribedState objects to
 * synchronize state with StateSpouts this object is subscribed to.
 */
public class TopologyContext extends WorkerTopologyContext implements IMetricsContext {
  private org.apache.heron.api.topology.TopologyContext delegate;

  // Constructor to match the signature of the storm's TopologyContext
  // Note that here, we fake the clojure.lang.Atom by creating our own class
  // This is real hacking a hack!
  @SuppressWarnings("rawtypes")
  public TopologyContext(StormTopology topology, Map stormConf,
                         Map<Integer, String> taskToComponent,
                         Map<String, List<Integer>> componentToSortedTasks,
                         Map<String, Map<String, Fields>> componentToStreamToFields,
                         String stormId, String codeDir, String pidDir, Integer taskId,
                         Integer workerPort, List<Integer> workerTasks,
                         Map<String, Object> defaultResources,
                         Map<String, Object> userResources,
                         Map<String, Object> executorData, Map<String, Object> registeredMetrics,
                         org.apache.storm.clojure.lang.Atom openOrPrepareWasCalled) {
    super((org.apache.heron.api.topology.TopologyContext) null);
  }

  public TopologyContext(org.apache.heron.api.topology.TopologyContext delegate) {
    super(delegate);
    this.delegate = delegate;
  }

  /**
   * All state from all subscribed state spouts streams will be synced with
   * the provided object.
   *
   * <p>It is recommended that your ISubscribedState object is kept as an instance
   * variable of this object. The recommended usage of this method is as follows:</p>
   *
   * <p>
   * _myState = context.setAllSubscribedState(new MyState());
   * </p>
   * @param obj Provided ISubscribedState implementation
   * @return Returns the ISubscribedState object provided
   */
  /*
    public <T extends ISubscribedState> T setAllSubscribedState(T obj) {
        //check that only subscribed to one component/stream for statespout
        //setsubscribedstate appropriately
        throw new NotImplementedException();
    }
  */


  /**
   * Synchronizes the default stream from the specified state spout component
   * id with the provided ISubscribedState object.
   *
   * <p>The recommended usage of this method is as follows:</p>
   * <p>
   * _myState = context.setSubscribedState(componentId, new MyState());
   * </p>
   *
   * @param componentId the id of the StateSpout component to subscribe to
   * @param obj Provided ISubscribedState implementation
   * @return Returns the ISubscribedState object provided
   */
  /*
    public <T extends ISubscribedState> T setSubscribedState(String componentId, T obj) {
        return setSubscribedState(componentId, Utils.DEFAULT_STREAM_ID, obj);
    }
  */

  /**
   * Synchronizes the specified stream from the specified state spout component
   * id with the provided ISubscribedState object.
   *
   * <p>The recommended usage of this method is as follows:</p>
   * <p>
   * _myState = context.setSubscribedState(componentId, streamId, new MyState());
   * </p>
   *
   * @param componentId the id of the StateSpout component to subscribe to
   * @param streamId the stream to subscribe to
   * @param obj Provided ISubscribedState implementation
   * @return Returns the ISubscribedState object provided
   */
  /*
    public <T extends ISubscribedState> T setSubscribedState(
      String componentId, String streamId, T obj) {
        throw new NotImplementedException();
    }
  */

  /**
   * Gets the task id of this task.
   *
   * @return the task id
   */
  public int getThisTaskId() {
    return delegate.getThisTaskId();
  }

  /**
   * Gets the component id for this task. The component id maps
   * to a component id specified for a Spout or Bolt in the topology definition.
   */
  public String getThisComponentId() {
    return delegate.getThisComponentId();
  }

  /**
   * Gets the declared output fields for the specified stream id for the component
   * this task is a part of.
   */
  public Fields getThisOutputFields(String streamId) {
    return new Fields(delegate.getThisOutputFields(streamId));
  }

  /**
   * Gets the set of streams declared for the component of this task.
   */
  public Set<String> getThisStreams() {
    return delegate.getThisStreams();
  }

  /**
   * Gets the index of this task id in getComponentTasks(getThisComponentId()).
   * An example use case for this method is determining which task
   * accesses which resource in a distributed resource to ensure an even distribution.
   */
  public int getThisTaskIndex() {
    return delegate.getThisTaskIndex();
  }

  /*
   * Gets the declared inputs to this component.
   *
   * @return A map from subscribed component/stream to the grouping subscribed with.
   */
  /*
    public Map<GlobalStreamId, Grouping> getThisSources() {
        return getSources(getThisComponentId());
    }
  */

  /*
   * Gets information about who is consuming the outputs of this component, and how.
   *
   * @return Map from stream id to component id to the Grouping used.
   */
  /*
  public Map<String, Map<String, Grouping>> getThisTargets() {
      return getTargets(getThisComponentId());
  }
  */
  public void setTaskData(String name, Object data) {
    delegate.setTaskData(name, data);
  }

  public Object getTaskData(String name) {
    return delegate.getTaskData(name);
  }

  /*
    public void setExecutorData(String name, Object data) {
        _executorData.put(name, data);
    }

    public Object getExecutorData(String name) {
        return _executorData.get(name);
    }
  */

  public void addTaskHook(ITaskHook newHook) {
    Collection<org.apache.heron.api.hooks.ITaskHook> hooks = delegate.getHooks();
    if (hooks == null) {
      ITaskHookDelegate delegateHook = new ITaskHookDelegate();
      delegateHook.addHook(newHook);

      delegate.addTaskHook(delegateHook);
    } else {
      for (org.apache.heron.api.hooks.ITaskHook hook : hooks) {
        if (hook instanceof ITaskHookDelegate) {
          ITaskHookDelegate delegateHook = (ITaskHookDelegate) hook;
          delegateHook.addHook(newHook);
          return;
        }
      }
      throw new RuntimeException("StormCompat taskHooks not setup properly");
    }
  }

  public Collection<ITaskHook> getHooks() {
    Collection<org.apache.heron.api.hooks.ITaskHook> hooks = delegate.getHooks();
    if (hooks != null) {
      for (org.apache.heron.api.hooks.ITaskHook hook : hooks) {
        if (hook instanceof ITaskHookDelegate) {
          return ((ITaskHookDelegate) hook).getHooks();
        }
      }
    }
    return null;
  }

  /*
   * Register a IMetric instance.
   * Storm will then call getValueAndReset on the metric every timeBucketSizeInSecs
   * and the returned value is sent to all metrics consumers.
   * You must call this during IBolt::prepare or ISpout::open.
   * @return The IMetric argument unchanged.
   */
  @SuppressWarnings("unchecked")
  public <T extends IMetric> T registerMetric(String name, T metric, int timeBucketSizeInSecs) {
    MetricDelegate d = new MetricDelegate(metric);
    delegate.registerMetric(name, d, timeBucketSizeInSecs);
    return metric;
  }

  /*
   * Convenience method for registering ReducedMetric.
   */
  @SuppressWarnings("rawtypes")
  public ReducedMetric registerMetric(String name, IReducer reducer, int timeBucketSizeInSecs) {
    return registerMetric(name, new ReducedMetric(reducer), timeBucketSizeInSecs);
  }

  /*
   * Convenience method for registering CombinedMetric.
   */
  @SuppressWarnings("rawtypes")
  public CombinedMetric registerMetric(String name, ICombiner combiner, int timeBucketSizeInSecs) {
    return registerMetric(name, new CombinedMetric(combiner), timeBucketSizeInSecs);
  }
}
