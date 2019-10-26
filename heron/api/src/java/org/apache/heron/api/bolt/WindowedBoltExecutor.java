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

package org.apache.heron.api.bolt;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.logging.Logger;

import org.apache.heron.api.Config;
import org.apache.heron.api.generated.TopologyAPI;
import org.apache.heron.api.state.HashMapState;
import org.apache.heron.api.state.State;
import org.apache.heron.api.topology.IStatefulComponent;
import org.apache.heron.api.topology.OutputFieldsDeclarer;
import org.apache.heron.api.topology.TopologyContext;
import org.apache.heron.api.tuple.Fields;
import org.apache.heron.api.tuple.Tuple;
import org.apache.heron.api.tuple.Values;
import org.apache.heron.api.windowing.Event;
import org.apache.heron.api.windowing.EvictionPolicy;
import org.apache.heron.api.windowing.TimestampExtractor;
import org.apache.heron.api.windowing.TriggerPolicy;
import org.apache.heron.api.windowing.TupleWindowImpl;
import org.apache.heron.api.windowing.WaterMarkEventGenerator;
import org.apache.heron.api.windowing.WindowLifecycleListener;
import org.apache.heron.api.windowing.WindowManager;
import org.apache.heron.api.windowing.WindowingConfigs;
import org.apache.heron.api.windowing.evictors.CountEvictionPolicy;
import org.apache.heron.api.windowing.evictors.TimeEvictionPolicy;
import org.apache.heron.api.windowing.evictors.WatermarkCountEvictionPolicy;
import org.apache.heron.api.windowing.evictors.WatermarkTimeEvictionPolicy;
import org.apache.heron.api.windowing.triggers.CountTriggerPolicy;
import org.apache.heron.api.windowing.triggers.TimeTriggerPolicy;
import org.apache.heron.api.windowing.triggers.WatermarkCountTriggerPolicy;
import org.apache.heron.api.windowing.triggers.WatermarkTimeTriggerPolicy;
import org.apache.heron.common.basics.TypeUtils;

import static org.apache.heron.api.bolt.BaseWindowedBolt.Count;

/**
 * An {@link IWindowedBolt} wrapper that does the windowing of tuples.
 */
public class WindowedBoltExecutor implements IRichBolt,
    IStatefulComponent<Serializable, Serializable> {
  private static final long serialVersionUID = -9204275913034895392L;

  private static final Logger LOG = Logger.getLogger(WindowedBoltExecutor.class.getName());
  private static final int DEFAULT_WATERMARK_EVENT_INTERVAL_MS = 1000; // 1s
  private static final int DEFAULT_MAX_LAG_MS = 0; // no lag
  public static final String LATE_TUPLE_FIELD = "late_tuple";
  private final IWindowedBolt bolt;
  private transient WindowedOutputCollector windowedOutputCollector;
  private transient WindowLifecycleListener<Tuple> listener;
  private transient WindowManager<Tuple> windowManager;
  private transient int maxLagMs;
  private TimestampExtractor timestampExtractor;
  private transient String lateTupleStream;
  private transient TriggerPolicy<Tuple, ?> triggerPolicy;
  private transient EvictionPolicy<Tuple, ?> evictionPolicy;
  private transient Long windowLengthDurationMs;
  private State<Serializable, Serializable> state;
  private static final String WINDOWING_INTERNAL_STATE = "windowing.internal.state";
  // package level for unit tests
  protected transient WaterMarkEventGenerator<Tuple> waterMarkEventGenerator;

  public WindowedBoltExecutor(IWindowedBolt bolt) {
    this.bolt = bolt;
    timestampExtractor = bolt.getTimestampExtractor();
  }

  protected int getTopologyTimeoutMillis(Map<String, Object> topoConf) {
    if (topoConf.get(Config.TOPOLOGY_ENABLE_MESSAGE_TIMEOUTS) != null) {
      boolean timeOutsEnabled = Boolean.parseBoolean(
          (String) topoConf.get(Config.TOPOLOGY_ENABLE_MESSAGE_TIMEOUTS));
      if (!timeOutsEnabled) {
        return Integer.MAX_VALUE;
      }
    }
    int timeout = 0;
    if (topoConf.get(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS) != null) {
      timeout =  TypeUtils.getInteger(topoConf.get(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS));
    }
    return timeout * 1000;
  }

  private int getMaxSpoutPending(Map<String, Object> topoConf) {
    int maxPending = Integer.MAX_VALUE;
    if (topoConf.get(Config.TOPOLOGY_MAX_SPOUT_PENDING) != null) {
      maxPending = TypeUtils.getInteger(topoConf.get(Config.TOPOLOGY_MAX_SPOUT_PENDING));
    }
    return maxPending;
  }

  private void ensureDurationLessThanTimeout(long duration, long timeout) {
    if (duration > timeout) {
      throw new IllegalArgumentException(
          "Window duration (length + sliding interval) value " + duration + " is more than "
              + Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS + " value " + timeout);
    }
  }

  private void ensureCountLessThanMaxPending(int count, int maxPending) {
    if (count > maxPending) {
      throw new IllegalArgumentException(
          "Window count (length + sliding interval) value " + count + " is more than "
              + Config.TOPOLOGY_MAX_SPOUT_PENDING + " value " + maxPending);
    }
  }

  @SuppressWarnings("HiddenField")
  protected void validate(Map<String, Object> topoConf, Count windowLengthCount, Long
      windowLengthDuration, Count slidingIntervalCount, Long slidingIntervalDuration) {

    int topologyTimeout = getTopologyTimeoutMillis(topoConf);
    int maxSpoutPending = getMaxSpoutPending(topoConf);
    if (windowLengthCount == null && windowLengthDuration == null) {
      throw new IllegalArgumentException("Window length is not specified");
    }

    if (windowLengthDuration != null && slidingIntervalDuration != null) {
      ensureDurationLessThanTimeout(
          windowLengthDuration + slidingIntervalDuration, topologyTimeout);
    } else if (windowLengthDuration != null) {
      ensureDurationLessThanTimeout(windowLengthDuration, topologyTimeout);
    } else if (slidingIntervalDuration != null) {
      ensureDurationLessThanTimeout(slidingIntervalDuration, topologyTimeout);
    }

    if (windowLengthCount != null && slidingIntervalCount != null) {
      ensureCountLessThanMaxPending(
          windowLengthCount.value + slidingIntervalCount.value, maxSpoutPending);
    } else if (windowLengthCount != null) {
      ensureCountLessThanMaxPending(windowLengthCount.value, maxSpoutPending);
    } else if (slidingIntervalCount != null) {
      ensureCountLessThanMaxPending(slidingIntervalCount.value, maxSpoutPending);
    }
  }

  @SuppressWarnings("unchecked")
  private WindowManager<Tuple> initWindowManager(WindowLifecycleListener<Tuple>
                                                     lifecycleListener, Map<String, Object>
      topoConf, TopologyContext context, Collection<Event<Tuple>> queue) {

    WindowManager<Tuple> manager = new WindowManager<>(lifecycleListener, queue);

    Count windowLengthCount = null;
    Long slidingIntervalDurationMs = null;
    Count slidingIntervalCount = null;
    // window length
    if (topoConf.containsKey(WindowingConfigs.TOPOLOGY_BOLTS_WINDOW_LENGTH_COUNT)) {
      windowLengthCount = new Count(((Number) topoConf.get(WindowingConfigs
          .TOPOLOGY_BOLTS_WINDOW_LENGTH_COUNT)).intValue());
    } else if (topoConf.containsKey(WindowingConfigs.TOPOLOGY_BOLTS_WINDOW_LENGTH_DURATION_MS)) {
      windowLengthDurationMs = (Long) topoConf.get(WindowingConfigs
          .TOPOLOGY_BOLTS_WINDOW_LENGTH_DURATION_MS);
    }
    // sliding interval
    if (topoConf.containsKey(WindowingConfigs.TOPOLOGY_BOLTS_SLIDING_INTERVAL_COUNT)) {
      slidingIntervalCount = new Count(((Number) topoConf.get(WindowingConfigs
          .TOPOLOGY_BOLTS_SLIDING_INTERVAL_COUNT)).intValue());
    } else if (topoConf.containsKey(WindowingConfigs.TOPOLOGY_BOLTS_SLIDING_INTERVAL_DURATION_MS)) {
      slidingIntervalDurationMs = (Long) topoConf.get(WindowingConfigs
          .TOPOLOGY_BOLTS_SLIDING_INTERVAL_DURATION_MS);
    } else {
      // default is a sliding window of count 1
      slidingIntervalCount = new Count(1);
    }
    // tuple ts
    if (timestampExtractor != null) {
      // late tuple stream
      lateTupleStream = (String) topoConf.get(WindowingConfigs.TOPOLOGY_BOLTS_LATE_TUPLE_STREAM);
      if (lateTupleStream != null) {
        if (!context.getThisStreams().contains(lateTupleStream)) {
          throw new IllegalArgumentException("Stream for late tuples must be defined with the "
              + "builder method withLateTupleStream");
        }
      }
      // max lag
      if (topoConf.containsKey(WindowingConfigs.TOPOLOGY_BOLTS_TUPLE_TIMESTAMP_MAX_LAG_MS)) {
        maxLagMs = ((Number) topoConf.get(
            WindowingConfigs.TOPOLOGY_BOLTS_TUPLE_TIMESTAMP_MAX_LAG_MS)).intValue();
      } else {
        maxLagMs = DEFAULT_MAX_LAG_MS;
      }
      // watermark interval
      long watermarkIntervalMs;
      if (topoConf.containsKey(WindowingConfigs.TOPOLOGY_BOLTS_WATERMARK_EVENT_INTERVAL_MS)) {
        watermarkIntervalMs = ((Number) topoConf.get(WindowingConfigs
            .TOPOLOGY_BOLTS_WATERMARK_EVENT_INTERVAL_MS)).intValue();
      } else {
        watermarkIntervalMs = DEFAULT_WATERMARK_EVENT_INTERVAL_MS;
      }
      waterMarkEventGenerator = new WaterMarkEventGenerator<>(manager, watermarkIntervalMs,
          maxLagMs, getComponentStreams(context), topoConf);
    } else {
      if (topoConf.containsKey(WindowingConfigs.TOPOLOGY_BOLTS_LATE_TUPLE_STREAM)) {
        throw new IllegalArgumentException(
            "Late tuple stream can be defined only when " + "specifying" + " a timestamp field");
      }
    }

    boolean hasCustomTrigger = topoConf
            .containsKey(WindowingConfigs.TOPOLOGY_BOLTS_WINDOW_CUSTOM_TRIGGER);
    boolean hasCustomEvictor = topoConf
            .containsKey(WindowingConfigs.TOPOLOGY_BOLTS_WINDOW_CUSTOM_EVICTOR);

    if (hasCustomTrigger && hasCustomEvictor) {
      triggerPolicy = (TriggerPolicy<Tuple, ?>)
              topoConf.get(WindowingConfigs.TOPOLOGY_BOLTS_WINDOW_CUSTOM_TRIGGER);
      evictionPolicy = (EvictionPolicy<Tuple, ?>)
              topoConf.get(WindowingConfigs.TOPOLOGY_BOLTS_WINDOW_CUSTOM_EVICTOR);
    } else if (!hasCustomEvictor && !hasCustomTrigger) {
      // validate
      validate(topoConf, windowLengthCount, windowLengthDurationMs, slidingIntervalCount,
              slidingIntervalDurationMs);

      evictionPolicy = getEvictionPolicy(windowLengthCount, windowLengthDurationMs);
      triggerPolicy = getTriggerPolicy(slidingIntervalCount, slidingIntervalDurationMs);
    } else {
      throw new IllegalArgumentException(
              "If either a custom TriggerPolicy or EvictionPolicy is defined, both must be."
      );
    }

    triggerPolicy.setEvictionPolicy(evictionPolicy);
    triggerPolicy.setTopologyConfig(topoConf);
    triggerPolicy.setTriggerHandler(manager);
    triggerPolicy.setWindowManager(manager);

    manager.setEvictionPolicy(evictionPolicy);
    manager.setTriggerPolicy(triggerPolicy);
    // restore state if there is existing state
    if (this.state != null
        && this.state.get(WINDOWING_INTERNAL_STATE) != null
        && !((HashMapState) this.state.get(WINDOWING_INTERNAL_STATE)).isEmpty()) {
      manager.restoreState((Map<String, Serializable>) state.get(WINDOWING_INTERNAL_STATE));
    }
    return manager;
  }

  protected Map<String, Serializable> getState() {
    return windowManager.getState();
  }

  private Set<TopologyAPI.StreamId> getComponentStreams(TopologyContext context) {
    Set<TopologyAPI.StreamId> streams = new HashSet<>();
    for (TopologyAPI.StreamId streamId : context.getThisSources().keySet()) {
      streams.add(streamId);
    }
    return streams;
  }

  /**
   * Start the trigger policy and waterMarkEventGenerator if set
   */
  protected void start() {
    if (waterMarkEventGenerator != null) {
      LOG.fine("Starting waterMarkEventGenerator");
      waterMarkEventGenerator.start();
    }
    LOG.fine("Starting trigger policy");
    triggerPolicy.start();
  }

  private boolean isTupleTs() {
    return timestampExtractor != null;
  }

  @SuppressWarnings("HiddenField")
  private TriggerPolicy<Tuple, ?> getTriggerPolicy(Count slidingIntervalCount, Long
      slidingIntervalDurationMs) {
    if (slidingIntervalCount != null) {
      if (isTupleTs()) {
        return new WatermarkCountTriggerPolicy<>(slidingIntervalCount.value);
      } else {
        return new CountTriggerPolicy<>(slidingIntervalCount.value);
      }
    } else {
      if (isTupleTs()) {
        return new WatermarkTimeTriggerPolicy<>(slidingIntervalDurationMs);
      } else {
        return new TimeTriggerPolicy<>(slidingIntervalDurationMs);
      }
    }
  }

  @SuppressWarnings("HiddenField")
  private EvictionPolicy<Tuple, ?> getEvictionPolicy(Count windowLengthCount, Long
      windowLengthDurationMs) {
    if (windowLengthCount != null) {
      if (isTupleTs()) {
        return new WatermarkCountEvictionPolicy<>(windowLengthCount.value);
      } else {
        return new CountEvictionPolicy<>(windowLengthCount.value);
      }
    } else {
      if (isTupleTs()) {
        return new WatermarkTimeEvictionPolicy<>(windowLengthDurationMs, maxLagMs);
      } else {
        return new TimeEvictionPolicy<>(windowLengthDurationMs);
      }
    }
  }

  @Override
  public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector
      collector) {
    doPrepare(topoConf, context, collector, new ConcurrentLinkedQueue<>());
  }

  // NOTE: the queue has to be thread safe.
  protected void doPrepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector
      collector, Collection<Event<Tuple>> queue) {
    Objects.requireNonNull(topoConf);
    Objects.requireNonNull(context);
    Objects.requireNonNull(collector);
    Objects.requireNonNull(queue);
    this.windowedOutputCollector = new WindowedOutputCollector(collector);
    bolt.prepare(topoConf, context, windowedOutputCollector);
    this.listener = newWindowLifecycleListener();
    this.windowManager = initWindowManager(listener, topoConf, context, queue);
    start();
    LOG.info(String.format("Initialized window manager %s", windowManager));
  }

  @Override
  public void execute(Tuple input) {
    if (isTupleTs()) {
      long ts = timestampExtractor.extractTimestamp(input);
      if (waterMarkEventGenerator.track(input.getSourceGlobalStreamId(), ts)) {
        windowManager.add(input, ts);
      } else {
        if (lateTupleStream != null) {
          windowedOutputCollector.emit(lateTupleStream, input, new Values(input));
        } else {
          LOG.info(String.format(
              "Received a late tuple %s with ts %d. This will not be " + "processed"
                  + ".", input, ts));
        }
        windowedOutputCollector.ack(input);
      }
    } else {
      windowManager.add(input);
    }
  }

  @Override
  public void cleanup() {
    if (windowManager != null) {
      windowManager.shutdown();
    }
    bolt.cleanup();
  }

  // for unit tests
  WindowManager<Tuple> getWindowManager() {
    return windowManager;
  }

  @Override
  @SuppressWarnings("HiddenField")
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    String lateTupleStream = (String) getComponentConfiguration().get(WindowingConfigs
        .TOPOLOGY_BOLTS_LATE_TUPLE_STREAM);
    if (lateTupleStream != null) {
      declarer.declareStream(lateTupleStream, new Fields(LATE_TUPLE_FIELD));
    }
    bolt.declareOutputFields(declarer);
  }

  @Override
  public Map<String, Object> getComponentConfiguration() {
    return bolt.getComponentConfiguration();
  }

  protected WindowLifecycleListener<Tuple> newWindowLifecycleListener() {
    return new WindowLifecycleListener<Tuple>() {
      @Override
      public void onExpiry(List<Tuple> tuples) {
        for (Tuple tuple : tuples) {
          windowedOutputCollector.ack(tuple);
        }
      }

      @Override
      public void onActivation(List<Tuple> tuples, List<Tuple> newTuples, List<Tuple>
          expiredTuples, Long timestamp) {
        windowedOutputCollector.setContext(tuples);
        boltExecute(tuples, newTuples, expiredTuples, timestamp);
      }

    };
  }

  protected void boltExecute(List<Tuple> tuples, List<Tuple> newTuples,
                             List<Tuple> expiredTuples,
                             Long timestamp) {
    bolt.execute(new TupleWindowImpl(tuples, newTuples,
        expiredTuples, getWindowStartTs(timestamp), timestamp));
  }

  private Long getWindowStartTs(Long endTs) {
    Long res = null;
    if (endTs != null && windowLengthDurationMs != null) {
      res = endTs - windowLengthDurationMs;
    }
    return res;
  }

  @Override
  @SuppressWarnings("HiddenField")
  public void initState(State<Serializable, Serializable> state) {
    // if effectively once is enabled
    if (state != null) {
      this.state = state;
      // if already contains state then that indicates that a rollback has happened
      if (!this.state.containsKey(WINDOWING_INTERNAL_STATE)) {
        this.state.put(WINDOWING_INTERNAL_STATE, new HashMapState<>());
      }
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public void preSave(String checkpointId) {
    if (this.state != null) {
      // getting current state from window manager to be included in checkpoint
      ((HashMapState<Serializable, Serializable>) this.state.get(WINDOWING_INTERNAL_STATE))
          .putAll(this.getWindowManager().getState());
    }
  }

  /**
   * Creates an {@link OutputCollector} wrapper that automatically
   * anchors the tuples to inputTuples while emitting.
   */
  private static class WindowedOutputCollector extends OutputCollector {
    private List<Tuple> inputTuples;

    WindowedOutputCollector(IOutputCollector delegate) {
      super(delegate);
    }

    @SuppressWarnings("HiddenField")
    void setContext(List<Tuple> inputTuples) {
      this.inputTuples = inputTuples;
    }

    @Override
    public List<Integer> emit(String streamId, List<Object> tuple) {
      return emit(streamId, inputTuples, tuple);
    }

    @Override
    public void emitDirect(int taskId, String streamId, List<Object> tuple) {
      emitDirect(taskId, streamId, inputTuples, tuple);
    }
  }

}
