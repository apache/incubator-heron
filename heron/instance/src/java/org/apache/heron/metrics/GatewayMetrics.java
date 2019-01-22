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

package org.apache.heron.metrics;

import org.apache.heron.api.metric.CountMetric;
import org.apache.heron.api.metric.MeanReducer;
import org.apache.heron.api.metric.MeanReducerState;
import org.apache.heron.api.metric.ReducedMetric;
import org.apache.heron.common.basics.SingletonRegistry;
import org.apache.heron.common.config.SystemConfig;
import org.apache.heron.common.utils.metrics.MetricsCollector;

/**
 * Gateway's metrics to be collected, mainly the status of the in &amp; out stream queues.
 */
public class GatewayMetrics {
  // # of packets received from stream manager
  private final CountMetric receivedPacketsCount;
  // # of packets sent to stream manager
  private final CountMetric sentPacketsCount;

  // The size in bytes received from stream manager
  private final CountMetric receivedPacketsSize;
  // The size in bytes sent to stream manager
  private final CountMetric sentPacketsSize;

  // The size in byte sent to metrics manager
  private final CountMetric sentMetricsPacketsCount;
  private final CountMetric sentMetricsSize;
  private final CountMetric sentMetricsCount;
  private final CountMetric sentExceptionsCount;
  // The # of items in inStreamQueue
  private final ReducedMetric<MeanReducerState, Number, Double> inStreamQueueSize;
  // The # of items in outStreamQueue
  private final ReducedMetric<MeanReducerState, Number, Double> outStreamQueueSize;

  private final ReducedMetric<MeanReducerState, Number, Double> inStreamQueueExpectedCapacity;

  private final ReducedMetric<MeanReducerState, Number, Double> outStreamQueueExpectedCapacity;

  // The # of times back-pressure happens on inStreamQueue or outMetricQueue so instance could not
  // receive more tuples from stream manager
  private final CountMetric inQueueFullCount;

  public GatewayMetrics() {
    receivedPacketsCount = new CountMetric();
    sentPacketsCount = new CountMetric();
    receivedPacketsSize = new CountMetric();
    sentPacketsSize = new CountMetric();

    sentMetricsSize = new CountMetric();
    sentMetricsPacketsCount = new CountMetric();
    sentMetricsCount = new CountMetric();
    sentExceptionsCount = new CountMetric();

    inStreamQueueSize = new ReducedMetric<>(new MeanReducer());
    outStreamQueueSize = new ReducedMetric<>(new MeanReducer());
    inStreamQueueExpectedCapacity = new ReducedMetric<>(new MeanReducer());
    outStreamQueueExpectedCapacity = new ReducedMetric<>(new MeanReducer());

    inQueueFullCount = new CountMetric();
  }

  /**
   * Register default Gateway Metrics to given MetricsCollector
   *
   * @param metricsCollector the MetricsCollector to register Metrics on
   */
  public void registerMetrics(MetricsCollector metricsCollector) {
    SystemConfig systemConfig =
        (SystemConfig) SingletonRegistry.INSTANCE.getSingleton(SystemConfig.HERON_SYSTEM_CONFIG);

    int interval = (int) systemConfig.getHeronMetricsExportInterval().getSeconds();

    metricsCollector.registerMetric("__gateway-received-packets-size",
        receivedPacketsSize,
        interval);
    metricsCollector.registerMetric("__gateway-sent-packets-size",
        sentPacketsSize,
        interval);
    metricsCollector.registerMetric("__gateway-received-packets-count",
        receivedPacketsCount,
        interval);
    metricsCollector.registerMetric("__gateway-sent-packets-count",
        sentPacketsCount,
        interval);

    metricsCollector.registerMetric("__gateway-sent-metrics-size",
        sentMetricsSize,
        interval);
    metricsCollector.registerMetric("__gateway-sent-metrics-packets-count",
        sentMetricsPacketsCount,
        interval);
    metricsCollector.registerMetric("__gateway-sent-metrics-count",
        sentMetricsCount,
        interval);
    metricsCollector.registerMetric("__gateway-sent-exceptions-count",
        sentExceptionsCount,
        interval);

    metricsCollector.registerMetric("__gateway-in-stream-queue-size",
        inStreamQueueSize,
        interval);
    metricsCollector.registerMetric("__gateway-out-stream-queue-size",
        outStreamQueueSize,
        interval);
    metricsCollector.registerMetric("__gateway-in-stream-queue-expected-capacity",
        inStreamQueueExpectedCapacity,
        interval);
    metricsCollector.registerMetric("__gateway-out-stream-queue-expected-capacity",
        outStreamQueueExpectedCapacity,
        interval);

    metricsCollector.registerMetric("__gateway-in-queue-full-count",
        inQueueFullCount,
        interval);
  }

  public void updateReceivedPacketsCount(long count) {
    receivedPacketsCount.incrBy(count);
  }

  public void updateSentPacketsCount(long count) {
    sentPacketsCount.incrBy(count);
  }

  public void updateReceivedPacketsSize(long size) {
    receivedPacketsSize.incrBy(size);
  }

  public void updateSentPacketsSize(long size) {
    sentPacketsSize.incrBy(size);
  }

  public void updateSentMetricsSize(long size) {
    sentMetricsSize.incrBy(size);
  }

  public void updateSentMetrics(long metricsCount, long exceptionsCount) {
    sentMetricsPacketsCount.incr();
    sentMetricsCount.incrBy(metricsCount);
    sentExceptionsCount.incrBy(exceptionsCount);
  }

  public void setInStreamQueueSize(long size) {
    inStreamQueueSize.update(size);
  }

  public void setOutStreamQueueSize(long size) {
    outStreamQueueSize.update(size);
  }

  public void setInStreamQueueExpectedCapacity(long capacity) {
    inStreamQueueExpectedCapacity.update(capacity);
  }

  public void setOutStreamQueueExpectedCapacity(long capacity) {
    outStreamQueueExpectedCapacity.update(capacity);
  }

  public void updateInQueueFullCount() {
    inQueueFullCount.incr();
  }
}
