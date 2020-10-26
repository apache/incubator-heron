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

package org.apache.heron.metricsmgr.executor;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.heron.common.basics.Communicator;
import org.apache.heron.common.basics.ExecutorLooper;
import org.apache.heron.common.basics.SysUtils;
import org.apache.heron.common.basics.TypeUtils;
import org.apache.heron.metricsmgr.MetricsSinksConfig;
import org.apache.heron.spi.metricsmgr.metrics.MetricsRecord;
import org.apache.heron.spi.metricsmgr.sink.IMetricsSink;
import org.apache.heron.spi.metricsmgr.sink.SinkContext;

/**
 * SinkExecutor is a Runnable, running in a specific thread to drive the IMetricsSink.
 * The IMetricsSink does not need to care about thread-safe since:
 * 1. It is always running in the same specific thread
 * 2. All arguments passed to IMetricsSink are immutable
 * <p>
 * The thread of SinkExecutor would be blocked to save resources except:
 * 1. New MetricsRecord comes and notify the SinkExecutor to wake up to process it
 * 2. The time interval to invoke flush() is met so SinkExecutor would wake up to invoke flush()
 */
public class SinkExecutor implements Runnable, AutoCloseable {
  private final IMetricsSink metricsSink;
  private final ExecutorLooper executorLooper;

  // Communicator to read MetricsRecord
  private final Communicator<MetricsRecord> metricsInSinkQueue;

  // An unmodifiable Map to store config
  private final Map<String, Object> sinkConfig;

  // Context for a sink to init()
  private final SinkContext sinkContext;

  // The name of Executor, would be used as the name of running thread
  private final String executorName;

  /**
   * Construct a SinkExecutor, which is a Runnable
   *
   * @param executorName the name of this executor used as the name of running thread
   * @param metricsSink the class implementing IMetricsSink
   * @param executorLooper the ExecutorLooper to bind with
   * @param metricsInSinkQueue the queue to read MetricsRecord from
   */
  public SinkExecutor(String executorName, IMetricsSink metricsSink,
                      ExecutorLooper executorLooper, Communicator<MetricsRecord> metricsInSinkQueue,
                      SinkContext sinkContext) {
    this.executorName = executorName;
    this.metricsSink = metricsSink;
    this.executorLooper = executorLooper;
    this.metricsInSinkQueue = metricsInSinkQueue;
    this.sinkContext = sinkContext;
    this.sinkConfig = new HashMap<String, Object>();
  }

  public void setProperty(String key, Object value) {
    sinkConfig.put(key, value);
  }

  public void setPropertyMap(Map<? extends String, Object> configMap) {
    sinkConfig.putAll(configMap);
  }

  public Communicator<MetricsRecord> getCommunicator() {
    return metricsInSinkQueue;
  }

  @Override
  public void close() {
    SysUtils.closeIgnoringExceptions(metricsSink);
  }

  @Override
  public void run() {
    // Set current running thread's name as executorName
    Thread.currentThread().setName(executorName);
    // Add task to invoke processRecord method when the WakeableLooper is waken up
    addSinkTasks();

    // Invoke flush method at a interval
    flushSinkAtInterval();

    metricsSink.init(Collections.unmodifiableMap(sinkConfig), sinkContext);

    executorLooper.loop();
  }

  // Add task to invoke processRecord method when the WakeableLooper is waken up
  private void addSinkTasks() {
    Runnable sinkTasks = new Runnable() {
      @Override
      public void run() {
        while (!metricsInSinkQueue.isEmpty()) {
          metricsSink.processRecord(metricsInSinkQueue.poll());
        }
      }
    };

    executorLooper.addTasksOnWakeup(sinkTasks);
  }

  // Add TimerTask to invoke flush() in IMetricsSink
  private void flushSinkAtInterval() {
    Object flushIntervalObj = sinkConfig.get(MetricsSinksConfig.CONFIG_KEY_FLUSH_FREQUENCY_MS);

    // If the config is not set, we consider the flush() would never be invoked
    if (flushIntervalObj != null) {
      final Duration flushInterval = TypeUtils.getDuration(flushIntervalObj, ChronoUnit.MILLIS);

      Runnable flushSink = new Runnable() {
        @Override
        public void run() {
          metricsSink.flush();
          //Plan itself in future
          executorLooper.registerTimerEvent(flushInterval, this);
        }
      };

      // Plan the runnable explicitly at the first time
      executorLooper.registerTimerEvent(flushInterval, flushSink);
    }
  }
}
