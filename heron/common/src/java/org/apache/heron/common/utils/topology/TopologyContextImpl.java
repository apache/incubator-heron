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

import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.heron.api.Config;
import org.apache.heron.api.generated.TopologyAPI;
import org.apache.heron.api.hooks.ITaskHook;
import org.apache.heron.api.hooks.info.BoltAckInfo;
import org.apache.heron.api.hooks.info.BoltExecuteInfo;
import org.apache.heron.api.hooks.info.BoltFailInfo;
import org.apache.heron.api.hooks.info.EmitInfo;
import org.apache.heron.api.hooks.info.SpoutAckInfo;
import org.apache.heron.api.hooks.info.SpoutFailInfo;
import org.apache.heron.api.metric.CombinedMetric;
import org.apache.heron.api.metric.ICombiner;
import org.apache.heron.api.metric.IMetric;
import org.apache.heron.api.metric.IReducer;
import org.apache.heron.api.metric.ReducedMetric;
import org.apache.heron.api.topology.TopologyContext;
import org.apache.heron.api.tuple.Fields;
import org.apache.heron.api.tuple.Tuple;
import org.apache.heron.common.basics.TypeUtils;
import org.apache.heron.common.utils.metrics.MetricsCollector;

/**
 * A TopologyContext is given to bolts and spouts in their "prepare" and "open"
 * methods, respectively. This object provides information about the component's
 * place within the topology, such as task ids, inputs and outputs, etc.
 */
public class TopologyContextImpl extends GeneralTopologyContextImpl implements TopologyContext {
  private final int myTaskId;
  private final Map<String, Object> taskData;
  private final MetricsCollector metricsCollector;

  // List of task hooks to delegate
  private final List<ITaskHook> taskHooks;

  public TopologyContextImpl(Map<String, Object> clusterConfig,
                             TopologyAPI.Topology topology,
                             Map<Integer, String> taskToComponentMap,
                             int myTaskId, MetricsCollector metricsCollector) {
    super(clusterConfig, topology, taskToComponentMap);
    this.metricsCollector = metricsCollector;
    this.myTaskId = myTaskId;
    this.taskData = new HashMap<>();

    // Init task hooks
    this.taskHooks = new LinkedList<>();
    List<String> taskHooksClassNames =
        TypeUtils.getListOfStrings(clusterConfig.get(Config.TOPOLOGY_AUTO_TASK_HOOKS));

    if (taskHooksClassNames != null) {
      // task hooks are registered
      for (String className : taskHooksClassNames) {
        ITaskHook taskHook;
        try {
          taskHook = (ITaskHook) Class.forName(className).newInstance();
        } catch (ClassNotFoundException ex) {
          throw new RuntimeException(ex + " ITaskHook class must be in class path.");
        } catch (InstantiationException ex) {
          throw new RuntimeException(ex + " ITaskHook class must be concrete.");
        } catch (IllegalAccessException ex) {
          throw new RuntimeException(ex + " ITaskHook class must have a no-arg constructor.");
        }

        this.taskHooks.add(taskHook);
      }
    }
  }

  /**
   * Task Hook Called just after the spout/bolt's prepare method is called.
   */
  public void invokeHookPrepare() {
    for (ITaskHook taskHook : taskHooks) {
      taskHook.prepare(getTopologyConfig(), this);
    }
  }

  /**
   * Task Hook Called just before the spout/bolt's cleanup method is called.
   */
  public void invokeHookCleanup() {
    for (ITaskHook taskHook : taskHooks) {
      taskHook.cleanup();
    }
  }

  /**
   * Task hook called every time a tuple is emitted in spout/bolt
   */
  public void invokeHookEmit(List<Object> values, String stream, Collection<Integer> outTasks) {
    if (taskHooks.size() != 0) {
      EmitInfo emitInfo = new EmitInfo(values, stream, getThisTaskId(), outTasks);
      for (ITaskHook taskHook : taskHooks) {
        taskHook.emit(emitInfo);
      }
    }
  }

  /**
   * Task hook called in spout every time a tuple gets acked
   */
  public void invokeHookSpoutAck(Object messageId, Duration completeLatency) {
    if (taskHooks.size() != 0) {
      SpoutAckInfo ackInfo = new SpoutAckInfo(messageId, getThisTaskId(), completeLatency);

      for (ITaskHook taskHook : taskHooks) {
        taskHook.spoutAck(ackInfo);
      }
    }
  }

  /**
   * Task hook called in spout every time a tuple gets failed
   */
  public void invokeHookSpoutFail(Object messageId, Duration failLatency) {
    if (taskHooks.size() != 0) {
      SpoutFailInfo failInfo = new SpoutFailInfo(messageId, getThisTaskId(), failLatency);

      for (ITaskHook taskHook : taskHooks) {
        taskHook.spoutFail(failInfo);
      }
    }
  }

  /**
   * Task hook called in bolt every time a tuple gets executed
   */
  public void invokeHookBoltExecute(Tuple tuple, Duration executeLatency) {
    if (taskHooks.size() != 0) {
      BoltExecuteInfo executeInfo = new BoltExecuteInfo(tuple, getThisTaskId(), executeLatency);

      for (ITaskHook taskHook : taskHooks) {
        taskHook.boltExecute(executeInfo);
      }
    }
  }

  /**
   * Task hook called in bolt every time a tuple gets acked
   */
  public void invokeHookBoltAck(Tuple tuple, Duration processLatency) {
    if (taskHooks.size() != 0) {
      BoltAckInfo ackInfo = new BoltAckInfo(tuple, getThisTaskId(), processLatency);

      for (ITaskHook taskHook : taskHooks) {
        taskHook.boltAck(ackInfo);
      }
    }
  }

  /**
   * Task hook called in bolt every time a tuple gets failed
   */
  public void invokeHookBoltFail(Tuple tuple, Duration failLatency) {
    if (taskHooks.size() != 0) {
      BoltFailInfo failInfo = new BoltFailInfo(tuple, getThisTaskId(), failLatency);

      for (ITaskHook taskHook : taskHooks) {
        taskHook.boltFail(failInfo);
      }
    }
  }

  /**
   * Gets the task id of this task.
   *
   * @return the task id
   */
  public int getThisTaskId() {
    return myTaskId;
  }

  /**
   * Gets the component id for this task. The component id maps
   * to a component id specified for a Spout or Bolt in the topology definition.
   */
  public String getThisComponentId() {
    return getComponentId(myTaskId);
  }

  /**
   * Gets the declared output fields for the specified stream id for the component
   * this task is a part of.
   */
  public Fields getThisOutputFields(String streamId) {
    return getComponentOutputFields(getThisComponentId(), streamId);
  }

  /**
   * Gets the set of streams declared for the component of this task.
   */
  public Set<String> getThisStreams() {
    return getComponentStreams(getThisComponentId());
  }

  /**
   * Gets the index of this task id in getComponentTasks(getThisComponentId()).
   * An example use case for this method is determining which task
   * accesses which resource in a distributed resource to ensure an even distribution.
   */
  public int getThisTaskIndex() {
    List<Integer> allTasks = getComponentTasks(getThisComponentId());
    int retVal = 0;
    for (Integer tsk : allTasks) {
      if (tsk < myTaskId) {
        retVal++;
      }
    }
    return retVal;
  }

  /**
   * Gets the declared inputs to this component.
   *
   * @return A map from subscribed component/stream to the grouping subscribed with.
   */
  public Map<TopologyAPI.StreamId, TopologyAPI.Grouping> getThisSources() {
    return getSources(getThisComponentId());
  }

  /**
   * Gets information about who is consuming the outputs of this component, and how.
   *
   * @return Map from stream id to component id to the Grouping used.
   */
  public Map<String, Map<String, TopologyAPI.Grouping>> getThisTargets() {
    return getTargets(getThisComponentId());
  }

  public void setTaskData(String name, Object data) {
    taskData.put(name, data);
  }

  public Object getTaskData(String name) {
    return taskData.get(name);
  }

  /*
   * Register a IMetric instance.
   * Heron will then call getValueAndReset on the metric every timeBucketSizeInSecs
   * and the returned value is sent to all metrics consumers.
   * You must call this during IBolt::prepare or ISpout::open.
   * @return The IMetric argument unchanged.
   */
  @Override
  public <T extends IMetric<U>, U> T registerMetric(String name,
                                                    T metric,
                                                    int timeBucketSizeInSecs) {
    metricsCollector.registerMetric(name, metric, timeBucketSizeInSecs);
    return metric;
  }

  /*
   * Convenience method for registering ReducedMetric.
   */
  @Override
  public <T, U, V> ReducedMetric<T, U, V> registerMetric(String name,
                                                         IReducer<T, U, V> reducer,
                                                         int timeBucketSizeInSecs) {
    return registerMetric(name, new ReducedMetric<>(reducer), timeBucketSizeInSecs);
  }

  /*
   * Convenience method for registering CombinedMetric.
   */
  @Override
  public <T> CombinedMetric<T> registerMetric(String name,
                                              ICombiner<T> combiner,
                                              int timeBucketSizeInSecs) {
    return registerMetric(name, new CombinedMetric<>(combiner), timeBucketSizeInSecs);
  }

  @Override
  public void addTaskHook(ITaskHook hook) {
    taskHooks.add(hook);
  }

  @Override
  public Collection<ITaskHook> getHooks() {
    return taskHooks;
  }
}
