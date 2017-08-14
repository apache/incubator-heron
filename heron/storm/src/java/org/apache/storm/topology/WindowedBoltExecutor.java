/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storm.topology;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.storm.Config;
import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.task.IOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.windowing.CountEvictionPolicy;
import org.apache.storm.windowing.CountTriggerPolicy;
import org.apache.storm.windowing.EvictionPolicy;
import org.apache.storm.windowing.TimeEvictionPolicy;
import org.apache.storm.windowing.TimeTriggerPolicy;
import org.apache.storm.windowing.TriggerPolicy;
import org.apache.storm.windowing.TupleWindowImpl;
import org.apache.storm.windowing.WaterMarkEventGenerator;
import org.apache.storm.windowing.WatermarkCountEvictionPolicy;
import org.apache.storm.windowing.WatermarkCountTriggerPolicy;
import org.apache.storm.windowing.WatermarkTimeEvictionPolicy;
import org.apache.storm.windowing.WatermarkTimeTriggerPolicy;
import org.apache.storm.windowing.WindowLifecycleListener;
import org.apache.storm.windowing.WindowManager;

import static org.apache.storm.topology.base.BaseWindowedBolt.Count;
import static org.apache.storm.topology.base.BaseWindowedBolt.Duration;

/**
 * An {@link IWindowedBolt} wrapper that does the windowing of tuples.
 */
public class WindowedBoltExecutor implements IRichBolt {
  private static final long serialVersionUID = 8110332014773492905L;
  private static final Logger LOG = Logger.getLogger(WindowedBoltExecutor.class.getName());
  private static final int DEFAULT_WATERMARK_EVENT_INTERVAL_MS = 1000; // 1s
  private static final int DEFAULT_MAX_LAG_MS = 0; // no lag
  private final IWindowedBolt bolt;
  private transient WindowedOutputCollector windowedOutputCollector;
  private transient WindowLifecycleListener<Tuple> listener;
  private transient WindowManager<Tuple> windowManager;
  private transient int maxLagMs;
  private transient String tupleTsFieldName;
  private transient TriggerPolicy<Tuple> triggerPolicy;
  private transient EvictionPolicy<Tuple> evictionPolicy;
  // package level for unit tests
  // SUPPRESS CHECKSTYLE VisibilityModifier
  transient WaterMarkEventGenerator<Tuple> waterMarkEventGenerator;

  public WindowedBoltExecutor(IWindowedBolt bolt) {
    this.bolt = bolt;
  }

  @SuppressWarnings("rawtypes")
  private int getTopologyTimeoutMillis(Map stormConf) {
    if (stormConf.get(Config.TOPOLOGY_ENABLE_MESSAGE_TIMEOUTS) != null) {
      boolean timeOutsEnabled =
          Boolean.parseBoolean(stormConf.get(Config.TOPOLOGY_ENABLE_MESSAGE_TIMEOUTS).toString());
      if (!timeOutsEnabled) {
        return Integer.MAX_VALUE;
      }
    }
    int timeout = 0;
    if (stormConf.get(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS) != null) {
      timeout = Integer.parseInt(stormConf.get(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS).toString());
    }
    return timeout * 1000;
  }

  @SuppressWarnings("rawtypes")
  private int getMaxSpoutPending(Map stormConf) {
    int maxPending = Integer.MAX_VALUE;
    if (stormConf.get(Config.TOPOLOGY_MAX_SPOUT_PENDING) != null) {
      maxPending = Integer.parseInt(stormConf.get(Config.TOPOLOGY_MAX_SPOUT_PENDING).toString());
    }
    return maxPending;
  }

  private void ensureDurationLessThanTimeout(int duration, int timeout) {
    if (duration > timeout) {
      throw new IllegalArgumentException("Window duration (length + sliding interval) value "
          + duration + " is more than " + Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS + " value "
          + timeout);
    }
  }

  private void ensureCountLessThanMaxPending(int count, int maxPending) {
    if (count > maxPending) {
      throw new IllegalArgumentException("Window count (length + sliding interval) value " + count
          + " is more than " + Config.TOPOLOGY_MAX_SPOUT_PENDING + " value " + maxPending);
    }
  }

  @SuppressWarnings("rawtypes")
  private void validate(Map stormConf, Count windowLengthCount, Duration windowLengthDuration,
                        Count slidingIntervalCount, Duration slidingIntervalDuration) {

    int topologyTimeout = getTopologyTimeoutMillis(stormConf);
    int maxSpoutPending = getMaxSpoutPending(stormConf);
    if (windowLengthCount == null && windowLengthDuration == null) {
      throw new IllegalArgumentException("Window length is not specified");
    }

    if (windowLengthDuration != null && slidingIntervalDuration != null) {
      ensureDurationLessThanTimeout(windowLengthDuration.value
          + slidingIntervalDuration.value, topologyTimeout);
    } else if (windowLengthDuration != null) {
      ensureDurationLessThanTimeout(windowLengthDuration.value, topologyTimeout);
    } else if (slidingIntervalDuration != null) {
      ensureDurationLessThanTimeout(slidingIntervalDuration.value, topologyTimeout);
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

  @SuppressWarnings("rawtypes")
  private WindowManager<Tuple> initWindowManager(
      WindowLifecycleListener<Tuple> lifecycleListener, Map stormConf, TopologyContext context) {
    WindowManager<Tuple> manager = new WindowManager<>(lifecycleListener);
    Duration windowLengthDuration = null;
    Count windowLengthCount = null;
    Duration slidingIntervalDuration = null;
    Count slidingIntervalCount = null;
    // window length
    if (stormConf.containsKey(Config.TOPOLOGY_BOLTS_WINDOW_LENGTH_COUNT)) {
      windowLengthCount = new Count(Integer.parseInt(stormConf.get(
          Config.TOPOLOGY_BOLTS_WINDOW_LENGTH_COUNT).toString()));
    } else if (stormConf.containsKey(Config.TOPOLOGY_BOLTS_WINDOW_LENGTH_DURATION_MS)) {
      windowLengthDuration = new Duration(
          Integer.parseInt(stormConf.get(
              Config.TOPOLOGY_BOLTS_WINDOW_LENGTH_DURATION_MS).toString()), TimeUnit.MILLISECONDS);
    }
    // sliding interval
    if (stormConf.containsKey(Config.TOPOLOGY_BOLTS_SLIDING_INTERVAL_COUNT)) {
      slidingIntervalCount = new Count(Integer.parseInt(stormConf.get(
          Config.TOPOLOGY_BOLTS_SLIDING_INTERVAL_COUNT).toString()));
    } else if (stormConf.containsKey(Config.TOPOLOGY_BOLTS_SLIDING_INTERVAL_DURATION_MS)) {
      slidingIntervalDuration = new Duration(Integer.parseInt(stormConf.get(
          Config.TOPOLOGY_BOLTS_SLIDING_INTERVAL_DURATION_MS).toString()), TimeUnit.MILLISECONDS);
    } else {
      // default is a sliding window of count 1
      slidingIntervalCount = new Count(1);
    }
    // tuple ts
    if (stormConf.containsKey(Config.TOPOLOGY_BOLTS_TUPLE_TIMESTAMP_FIELD_NAME)) {
      tupleTsFieldName = (String) stormConf.get(Config.TOPOLOGY_BOLTS_TUPLE_TIMESTAMP_FIELD_NAME);
      // max lag
      if (stormConf.containsKey(Config.TOPOLOGY_BOLTS_TUPLE_TIMESTAMP_MAX_LAG_MS)) {
        maxLagMs = Integer.parseInt(
            stormConf.get(Config.TOPOLOGY_BOLTS_TUPLE_TIMESTAMP_MAX_LAG_MS).toString());
      } else {
        maxLagMs = DEFAULT_MAX_LAG_MS;
      }
      // watermark interval
      int watermarkInterval;
      if (stormConf.containsKey(Config.TOPOLOGY_BOLTS_WATERMARK_EVENT_INTERVAL_MS)) {
        watermarkInterval = Integer.parseInt(
            stormConf.get(Config.TOPOLOGY_BOLTS_WATERMARK_EVENT_INTERVAL_MS).toString());
      } else {
        watermarkInterval = DEFAULT_WATERMARK_EVENT_INTERVAL_MS;
      }
      waterMarkEventGenerator = new WaterMarkEventGenerator<>(manager, watermarkInterval,
          maxLagMs, getComponentStreams(context));
    }
    // validate
    validate(stormConf, windowLengthCount, windowLengthDuration,
        slidingIntervalCount, slidingIntervalDuration);
    evictionPolicy = getEvictionPolicy(windowLengthCount, windowLengthDuration,
        manager);
    triggerPolicy = getTriggerPolicy(slidingIntervalCount, slidingIntervalDuration, manager);
    manager.setEvictionPolicy(evictionPolicy);
    manager.setTriggerPolicy(triggerPolicy);
    return manager;
  }

  private Set<GlobalStreamId> getComponentStreams(TopologyContext context) {
    Set<GlobalStreamId> streams = new HashSet<>();
    for (GlobalStreamId streamId : context.getThisSourceIds()) {
      streams.add(streamId);
    }
    return streams;
  }

  /**
   * Start the trigger policy and waterMarkEventGenerator if set
   */
  protected void start() {
    if (waterMarkEventGenerator != null) {
      LOG.log(Level.FINE, "Starting waterMarkEventGenerator");
      waterMarkEventGenerator.start();
    }
    LOG.log(Level.FINE, "Starting trigger policy");
    triggerPolicy.start();
  }

  private boolean isTupleTs() {
    return tupleTsFieldName != null;
  }

  private TriggerPolicy<Tuple> getTriggerPolicy(
      Count slidingIntervalCount, Duration slidingIntervalDuration, WindowManager<Tuple> manager) {
    if (slidingIntervalCount != null) {
      if (isTupleTs()) {
        return new WatermarkCountTriggerPolicy<>(
            slidingIntervalCount.value, manager, evictionPolicy, manager);
      } else {
        return new CountTriggerPolicy<>(slidingIntervalCount.value, manager, evictionPolicy);
      }
    } else {
      if (isTupleTs()) {
        return new WatermarkTimeTriggerPolicy<>(
            slidingIntervalDuration.value, manager, evictionPolicy, manager);
      } else {
        return new TimeTriggerPolicy<>(slidingIntervalDuration.value, manager, evictionPolicy);
      }
    }
  }

  private EvictionPolicy<Tuple> getEvictionPolicy(
      Count windowLengthCount, Duration windowLengthDuration, WindowManager<Tuple> manager) {
    if (windowLengthCount != null) {
      if (isTupleTs()) {
        return new WatermarkCountEvictionPolicy<>(windowLengthCount.value);
      } else {
        return new CountEvictionPolicy<>(windowLengthCount.value);
      }
    } else {
      if (isTupleTs()) {
        return new WatermarkTimeEvictionPolicy<>(windowLengthDuration.value, maxLagMs);
      } else {
        return new TimeEvictionPolicy<>(windowLengthDuration.value);
      }
    }
  }

  @Override
  @SuppressWarnings("rawtypes")
  public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
    this.windowedOutputCollector = new WindowedOutputCollector(collector);
    bolt.prepare(stormConf, context, windowedOutputCollector);
    this.listener = newWindowLifecycleListener();
    this.windowManager = initWindowManager(listener, stormConf, context);
    start();
    LOG.log(Level.FINE, "Initialized window manager {} ", this.windowManager);
  }

  @Override
  public void execute(Tuple input) {
    if (isTupleTs()) {
      long ts = input.getLongByField(tupleTsFieldName);
      GlobalStreamId stream =
          new GlobalStreamId(input.getSourceComponent(), input.getSourceStreamId());
      if (waterMarkEventGenerator.track(stream, ts)) {
        windowManager.add(input, ts);
      } else {
        windowedOutputCollector.ack(input);
        LOG.log(Level.INFO, String.format(
                "Received a late tuple %s with ts %d. This will not be processed.", input, ts));
      }
    } else {
      windowManager.add(input);
    }
  }

  @Override
  public void cleanup() {
    windowManager.shutdown();
    bolt.cleanup();
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
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
      public void onActivation(
          List<Tuple> tuples, List<Tuple> newTuples, List<Tuple> expiredTuples) {
        windowedOutputCollector.setContext(tuples);
        bolt.execute(new TupleWindowImpl(tuples, newTuples, expiredTuples));
      }
    };
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

    void setContext(List<Tuple> newInputTuples) {
      inputTuples = newInputTuples;
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
