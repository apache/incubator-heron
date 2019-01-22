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

import java.util.List;

import org.apache.heron.api.generated.TopologyAPI;
import org.apache.heron.api.metric.CountMetric;
import org.apache.heron.api.metric.CumulativeCountMetric;
import org.apache.heron.api.metric.MeanReducer;
import org.apache.heron.api.metric.MeanReducerState;
import org.apache.heron.api.metric.MultiCountMetric;
import org.apache.heron.api.metric.MultiReducedMetric;
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
 * This is a bolt metrics object with more information and it is used in heron core. To change to a faster
 * metrics object, go to BoltInstance.java and replace "new FullBoltMetrcs" to "new BoltMetrics"
 */

public class FullBoltMetrics implements IBoltMetrics {
  private final MultiCountMetric ackCount;
  private final MultiReducedMetric<MeanReducerState, Number, Double> processLatency;
  private final MultiReducedMetric<MeanReducerState, Number, Double> failLatency;
  private final MultiCountMetric failCount;
  private final MultiCountMetric executeCount;
  private final MultiReducedMetric<MeanReducerState, Number, Double> executeLatency;

  // Time in nano-seconds spending in execute() at every interval
  private final MultiCountMetric executeTimeNs;
  private final MultiCountMetric emitCount;
  private final CountMetric tupleAddedToQueue;
  private final MultiCountMetric totalDeserializationTimeNs;
  private final MultiCountMetric totalSerializationTimeNs;
  private final MultiReducedMetric<MeanReducerState, Number, Double> averageSerializationTimeNs;
  private final MultiReducedMetric<MeanReducerState, Number, Double> averageDeserializationTimeNs;

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

  public FullBoltMetrics() {
    ackCount = new MultiCountMetric();
    processLatency = new MultiReducedMetric<>(new MeanReducer());
    failLatency = new MultiReducedMetric<>(new MeanReducer());
    failCount = new MultiCountMetric();
    executeCount = new MultiCountMetric();
    executeLatency = new MultiReducedMetric<>(new MeanReducer());
    executeTimeNs = new MultiCountMetric();
    emitCount = new MultiCountMetric();
    outQueueFullCount = new CountMetric();
    tupleAddedToQueue = new CountMetric();
    taskRunCount = new CumulativeCountMetric();
    executionCount = new CumulativeCountMetric();
    continueWorkCount = new CumulativeCountMetric();

    totalDeserializationTimeNs = new MultiCountMetric();
    totalSerializationTimeNs = new MultiCountMetric();

    averageSerializationTimeNs = new MultiReducedMetric<>(new MeanReducer());
    averageDeserializationTimeNs = new MultiReducedMetric<>(new MeanReducer());
  }

  public void registerMetrics(TopologyContextImpl topologyContext) {
    SystemConfig systemConfig =
        (SystemConfig) SingletonRegistry.INSTANCE.getSingleton(SystemConfig.HERON_SYSTEM_CONFIG);

    int interval = (int) systemConfig.getHeronMetricsExportInterval().getSeconds();

    topologyContext.registerMetric("__ack-count", ackCount, interval);
    topologyContext.registerMetric("__process-latency", processLatency, interval);
    topologyContext.registerMetric("__fail-latency", failLatency, interval);
    topologyContext.registerMetric("__fail-count", failCount, interval);
    topologyContext.registerMetric("__execute-count", executeCount, interval);
    topologyContext.registerMetric("__execute-latency", executeLatency, interval);
    topologyContext.registerMetric("__execute-time-ns", executeTimeNs, interval);
    topologyContext.registerMetric("__emit-count", emitCount, interval);
    topologyContext.registerMetric("__out-queue-full-count", outQueueFullCount, interval);
    topologyContext.registerMetric("__task-run-count", taskRunCount, interval);
    topologyContext.registerMetric("__execution-count", executionCount, interval);
    topologyContext.registerMetric("__continue-work-count", continueWorkCount, interval);

    topologyContext.registerMetric(
        "__tuple-deserialization-time-ns", totalDeserializationTimeNs, interval);
    topologyContext.registerMetric(
        "__tuple-serialization-time-ns", totalSerializationTimeNs, interval);
    topologyContext.registerMetric(
        "__av-tuple-deserialization-time-ns", totalDeserializationTimeNs, interval);
    topologyContext.registerMetric(
        "__av-tuple-serialization-time-ns", totalSerializationTimeNs, interval);
    topologyContext.registerMetric("__data-tuple-added-to-outgoing-queue/default",
        tupleAddedToQueue, interval);
  }

  // For MultiCountMetrics, we need to set the default value for all streams.
  // Otherwise, it is possible one metric for a particular stream is null.
  // For instance, the fail-count on a particular stream could be undefined
  // causing metrics not be exported.
  // However, it will not set the Multi Reduced/Assignable Metrics,
  // since we could not have default values for them
  public void initMultiCountMetrics(PhysicalPlanHelper helper) {
    // For bolt, we would consider both input stream and output stream
    List<TopologyAPI.InputStream> inputs = helper.getMyBolt().getInputsList();
    for (TopologyAPI.InputStream inputStream : inputs) {
      String streamId = inputStream.getStream().getId();
      String globalStreamId =
          new StringBuilder(inputStream.getStream().getComponentName()).
              append("/").append(streamId).toString();

      ackCount.scope(streamId);
      failCount.scope(streamId);
      executeCount.scope(streamId);
      executeTimeNs.scope(streamId);

      ackCount.scope(globalStreamId);
      failCount.scope(globalStreamId);
      executeCount.scope(globalStreamId);
      executeTimeNs.scope(globalStreamId);
    }
    List<TopologyAPI.OutputStream> outputs = helper.getMyBolt().getOutputsList();
    for (TopologyAPI.OutputStream outputStream : outputs) {
      String streamId = outputStream.getStream().getId();
      emitCount.scope(streamId);
    }
  }

  public void ackedTuple(String streamId, String sourceComponent, long latency) {
    ackCount.scope(streamId).incr();
    processLatency.scope(streamId).update(latency);

    // Consider there are cases that different streams with the same streamId,
    // but with different source component. We need to distinguish them too.
    String globalStreamId =
        new StringBuilder(sourceComponent).append("/").append(streamId).toString();
    ackCount.scope(globalStreamId).incr();
    processLatency.scope(globalStreamId).update(latency);
  }

  public void failedTuple(String streamId, String sourceComponent, long latency) {
    failCount.scope(streamId).incr();
    failLatency.scope(streamId).update(latency);

    // Consider there are cases that different streams with the same streamId,
    // but with different source component. We need to distinguish them too.
    String globalStreamId =
        new StringBuilder(sourceComponent).append("/").append(streamId).toString();
    failCount.scope(globalStreamId).incr();
    failLatency.scope(globalStreamId).update(latency);
  }

  public void executeTuple(String streamId, String sourceComponent, long latency) {
    executeCount.scope(streamId).incr();
    executeLatency.scope(streamId).update(latency);
    executeTimeNs.scope(streamId).incrBy(latency);

    // Consider there are cases that different streams with the same streamId,
    // but with different source component. We need to distinguish them too.
    String globalStreamId =
        new StringBuilder(sourceComponent).append("/").append(streamId).toString();
    executeCount.scope(globalStreamId).incr();
    executeLatency.scope(globalStreamId).update(latency);
    executeTimeNs.scope(globalStreamId).incrBy(latency);
  }

  public void emittedTuple(String streamId) {
    emitCount.scope(streamId).incr();
  }

  public void addTupleToQueue(int size) {
    tupleAddedToQueue.incr();
  }

  public void updateOutQueueFullCount() {
    outQueueFullCount.incr();
  }

  public void deserializeDataTuple(String streamId, String sourceComponent, long latency) {
    totalDeserializationTimeNs.scope(streamId).incrBy(latency);
    averageDeserializationTimeNs.scope(streamId).update(latency);

    // Consider there are cases that different streams with the same streamId,
    // but with different source component. We need to distinguish them too.
    String globalStreamId =
        new StringBuilder(sourceComponent).append("/").append(streamId).toString();
    totalDeserializationTimeNs.scope(globalStreamId).incrBy(latency);
    averageDeserializationTimeNs.scope(globalStreamId).update(latency);
  }

  public void serializeDataTuple(String streamId, long latency) {
    totalSerializationTimeNs.scope(streamId).incrBy(latency);
    averageSerializationTimeNs.scope(streamId).update(latency);
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

