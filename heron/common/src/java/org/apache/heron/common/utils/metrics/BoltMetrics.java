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

package org.apache.heron.common.utils.metrics;

import org.apache.heron.api.metric.CountMetric;
import org.apache.heron.api.metric.CumulativeCountMetric;
import org.apache.heron.api.metric.MeanReducer;
import org.apache.heron.api.metric.MeanReducerState;
import org.apache.heron.api.metric.ReducedMetric;
import org.apache.heron.common.basics.SingletonRegistry;
import org.apache.heron.common.config.SystemConfig;
import org.apache.heron.common.utils.misc.PhysicalPlanHelper;
import org.apache.heron.common.utils.topology.TopologyContextImpl;

/**
 * Bolt's metrics to be collect
 * We need to:
 * 1. Define the metrics to be collected
 * 2. New them in the constructor
 * 3. Register them in registerMetrics(...) by using MetricsCollector's registerMetric(...)
 * 4. Expose methods which could be called externally to change the value of metrics
 *
 * This is a fast bolt metrics object and it is NOT used in heron core. To use this
 * metrics object, go to BoltInstance.java and replace "new FullBoltMetrcs" to "new BoltMetrics"
 */

public class BoltMetrics implements IBoltMetrics {
  private final CountMetric ackCount;
  private final ReducedMetric<MeanReducerState, Number, Double> processLatency;
  private final ReducedMetric<MeanReducerState, Number, Double> failLatency;
  private final CountMetric failCount;
  private final CountMetric executeCount;
  private final ReducedMetric<MeanReducerState, Number, Double> executeLatency;
  private final CountMetric tupleAddedToQueue;
  // Time in nano-seconds spending in execute() at every interval
  private final CountMetric emitCount;

  // The # of times back-pressure happens on outStreamQueue
  // so instance could not produce more tuples
  private final CountMetric outQueueFullCount;
  /*
   * Metrics for how many times spout instance task is run.
   */
  private CumulativeCountMetric taskRunCount;
  /*
   * Metrics for how many times spout produceTuple is called.
   */
  private CumulativeCountMetric executionCount;
  /*
   * Metrics for how many times spout continue work is true.
   */
  private CumulativeCountMetric continueWorkCount;

  public BoltMetrics() {
    ackCount = new CountMetric();
    processLatency = new ReducedMetric<>(new MeanReducer());
    failLatency = new ReducedMetric<>(new MeanReducer());
    failCount = new CountMetric();
    executeCount = new CountMetric();
    executeLatency = new ReducedMetric<>(new MeanReducer());
    emitCount = new CountMetric();
    outQueueFullCount = new CountMetric();
    tupleAddedToQueue = new CountMetric();
    taskRunCount = new CumulativeCountMetric();
    executionCount = new CumulativeCountMetric();
    continueWorkCount = new CumulativeCountMetric();
  }

  public void registerMetrics(TopologyContextImpl topologyContext) {
    SystemConfig systemConfig =
        (SystemConfig) SingletonRegistry.INSTANCE.getSingleton(SystemConfig.HERON_SYSTEM_CONFIG);

    int interval = (int) systemConfig.getHeronMetricsExportInterval().getSeconds();

    topologyContext.registerMetric("__ack-count/default", ackCount, interval);
    topologyContext.registerMetric("__process-latency/default", processLatency, interval);
    topologyContext.registerMetric("__fail-latency/default", failLatency, interval);
    topologyContext.registerMetric("__fail-count/default", failCount, interval);
    topologyContext.registerMetric("__execute-count/default", executeCount, interval);
    topologyContext.registerMetric("__execute-latency/default", executeLatency, interval);
    topologyContext.registerMetric("__emit-count/default", emitCount, interval);
    topologyContext.registerMetric("__out-queue-full-count", outQueueFullCount, interval);
    topologyContext.registerMetric("__data-tuple-added-to-outgoing-queue/default",
        tupleAddedToQueue, interval);
    topologyContext.registerMetric("__task-run-count", taskRunCount, interval);
    topologyContext.registerMetric("__execution-count", executionCount, interval);
    topologyContext.registerMetric("__continue-work-count", continueWorkCount, interval);
  }

  // For MultiCountMetrics, we need to set the default value for all streams.
  // Otherwise, it is possible one metric for a particular stream is null.
  // For instance, the fail-count on a particular stream could be undefined
  // causing metrics not be exported.
  // However, it will not set the Multi Reduced/Assignable Metrics,
  // since we could not have default values for them
  public void initMultiCountMetrics(PhysicalPlanHelper helper) {

  }

  public void ackedTuple(String streamId, String sourceComponent, long latency) {
    ackCount.incr();
    processLatency.update(latency);
  }

  public void failedTuple(String streamId, String sourceComponent, long latency) {
    failCount.incr();
    failLatency.update(latency);
  }

  public void executeTuple(String streamId, String sourceComponent, long latency) {
    executeCount.incr();
    executeLatency.update(latency);
  }

  public void emittedTuple(String streamId) {
    emitCount.incr();
  }

  public void addTupleToQueue(int size) {
    tupleAddedToQueue.incr();
  }

  public void updateOutQueueFullCount() {
    outQueueFullCount.incr();
  }

  public void deserializeDataTuple(String streamId, String sourceComponent, long latency) {
  }

  public void serializeDataTuple(String streamId, long latency) {
  }

  public void updateTaskRunCount() {
    taskRunCount.incr();
  }

  public void updateExecutionCount() {
    executionCount.incr();
  }

  public void updateContinueWorkCount() {
    continueWorkCount.incr();
  }
}
