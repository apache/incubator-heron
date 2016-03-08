package com.twitter.heron.metrics;

import com.twitter.heron.api.metric.CountMetric;
import com.twitter.heron.api.metric.MeanReducer;
import com.twitter.heron.api.metric.ReducedMetric;
import com.twitter.heron.common.config.SystemConfig;
import com.twitter.heron.common.basics.SingletonRegistry;
import com.twitter.heron.common.utils.metrics.MetricsCollector;

/**
 * Gateway's metrics to be collected, mainly the status of the in & out stream queues.
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
  private final ReducedMetric inStreamQueueSize;
  // The # of items in outStreamQueue
  private final ReducedMetric outStreamQueueSize;

  private final ReducedMetric inStreamQueueExpectedCapacity;

  private final ReducedMetric outStreamQueueExpectedCapacity;

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

    inStreamQueueSize = new ReducedMetric(new MeanReducer());
    outStreamQueueSize = new ReducedMetric(new MeanReducer());
    inStreamQueueExpectedCapacity = new ReducedMetric(new MeanReducer());
    outStreamQueueExpectedCapacity = new ReducedMetric(new MeanReducer());

    inQueueFullCount = new CountMetric();
  }

  public void registerMetrics(MetricsCollector metricsCollector) {
    SystemConfig systemConfig = (SystemConfig) SingletonRegistry.INSTANCE.getSingleton(SystemConfig.HERON_SYSTEM_CONFIG);

    int interval = systemConfig.getHeronMetricsExportIntervalSec();

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
