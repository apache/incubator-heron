//  Copyright 2017 Twitter. All rights reserved.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
package com.twitter.heron.api.bolt;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.logging.Logger;

import com.twitter.heron.api.Config;
import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.api.topology.OutputFieldsDeclarer;
import com.twitter.heron.api.topology.TopologyContext;
import com.twitter.heron.api.tuple.Fields;
import com.twitter.heron.api.tuple.Tuple;
import com.twitter.heron.api.tuple.Values;
import com.twitter.heron.api.utils.TupleUtils;
import com.twitter.heron.api.windowing.Event;
import com.twitter.heron.api.windowing.EvictionPolicy;
import com.twitter.heron.api.windowing.TimerEvent;
import com.twitter.heron.api.windowing.TimestampExtractor;
import com.twitter.heron.api.windowing.TriggerPolicy;
import com.twitter.heron.api.windowing.TupleWindowImpl;
import com.twitter.heron.api.windowing.WaterMarkEventGenerator;
import com.twitter.heron.api.windowing.WindowLifecycleListener;
import com.twitter.heron.api.windowing.WindowManager;
import com.twitter.heron.api.windowing.WindowingConfigs;
import com.twitter.heron.api.windowing.evictors.CountEvictionPolicy;
import com.twitter.heron.api.windowing.evictors.TimeEvictionPolicy;
import com.twitter.heron.api.windowing.evictors.WatermarkCountEvictionPolicy;
import com.twitter.heron.api.windowing.evictors.WatermarkTimeEvictionPolicy;
import com.twitter.heron.api.windowing.triggers.CountTriggerPolicy;
import com.twitter.heron.api.windowing.triggers.TimeTriggerPolicy;
import com.twitter.heron.api.windowing.triggers.WatermarkCountTriggerPolicy;
import com.twitter.heron.api.windowing.triggers.WatermarkTimeTriggerPolicy;
import com.twitter.heron.common.basics.TypeUtils;

import static com.twitter.heron.api.bolt.BaseWindowedBolt.Count;

/**
 * An {@link IWindowedBolt} wrapper that does the windowing of tuples.
 */
public class WindowedBoltExecutor implements IRichBolt {
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
  private transient TriggerPolicy<Tuple> triggerPolicy;
  private transient EvictionPolicy<Tuple> evictionPolicy;
  private transient Long windowLengthDurationMs;
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
      int watermarkInterval;
      if (topoConf.containsKey(WindowingConfigs.TOPOLOGY_BOLTS_WATERMARK_EVENT_INTERVAL_MS)) {
        watermarkInterval = ((Number) topoConf.get(WindowingConfigs
            .TOPOLOGY_BOLTS_WATERMARK_EVENT_INTERVAL_MS)).intValue();
      } else {
        watermarkInterval = DEFAULT_WATERMARK_EVENT_INTERVAL_MS;
      }
      // Use tick tuple to perodically generate watermarks
      Config.setTickTupleFrequencyMs(topoConf, watermarkInterval);
      waterMarkEventGenerator = new WaterMarkEventGenerator<>(manager,
          maxLagMs, getComponentStreams(context));
    } else {
      if (topoConf.containsKey(WindowingConfigs.TOPOLOGY_BOLTS_LATE_TUPLE_STREAM)) {
        throw new IllegalArgumentException(
            "Late tuple stream can be defined only when " + "specifying" + " a timestamp field");
      }
    }
    // validate
    validate(topoConf, windowLengthCount, windowLengthDurationMs, slidingIntervalCount,
        slidingIntervalDurationMs);
    evictionPolicy = getEvictionPolicy(windowLengthCount, windowLengthDurationMs);
    triggerPolicy = getTriggerPolicy(slidingIntervalCount, slidingIntervalDurationMs, manager,
        evictionPolicy, topoConf);
    manager.setEvictionPolicy(evictionPolicy);
    manager.setTriggerPolicy(triggerPolicy);
    return manager;
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
  private TriggerPolicy<Tuple> getTriggerPolicy(Count slidingIntervalCount, Long
      slidingIntervalDurationMs, WindowManager<Tuple> manager, EvictionPolicy<Tuple>
      evictionPolicy, Map<String, Object> topoConf) {
    if (slidingIntervalCount != null) {
      if (isTupleTs()) {
        return new WatermarkCountTriggerPolicy<>(slidingIntervalCount.value, manager,
            evictionPolicy, manager);
      } else {
        return new CountTriggerPolicy<>(slidingIntervalCount.value, manager, evictionPolicy);
      }
    } else {
      if (isTupleTs()) {
        return new WatermarkTimeTriggerPolicy<>(slidingIntervalDurationMs, manager,
            evictionPolicy, manager);
      } else {
        // set tick tuple frequency in milliseconds for timer in TimeTriggerPolicy
        Config.setTickTupleFrequencyMs(topoConf, slidingIntervalDurationMs);
        return new TimeTriggerPolicy<>(slidingIntervalDurationMs, manager, evictionPolicy);
      }
    }
  }

  @SuppressWarnings("HiddenField")
  private EvictionPolicy<Tuple> getEvictionPolicy(Count windowLengthCount, Long
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
    if (TupleUtils.isTick(input)) {
      long currTime = System.currentTimeMillis();
      windowManager.add(new TimerEvent<>(input, currTime));
      if (isTupleTs()) {
        waterMarkEventGenerator.run();
      }
    } else {
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
  }

  @Override
  public void cleanup() {
    windowManager.shutdown();
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
