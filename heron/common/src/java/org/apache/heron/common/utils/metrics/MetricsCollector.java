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


import java.time.Duration;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

import org.apache.heron.api.metric.CumulativeCountMetric;
import org.apache.heron.api.metric.IMetric;
import org.apache.heron.api.metric.IMetricsRegister;
import org.apache.heron.common.basics.Communicator;
import org.apache.heron.common.basics.WakeableLooper;
import org.apache.heron.proto.system.Metrics;

/**
 * MetricsCollector could:
 * 1. Register a list of metrics with gathering interval
 * 2. Send the metrics to a queue after gathering the metrics
 */
public class MetricsCollector implements IMetricsRegister {
  private static final Logger LOG = Logger.getLogger(MetricsCollector.class.getName());
  private static final String COLLECTION_COUNT_NAME = "__collector-collection-count";

  private Map<String, IMetric<?>> metrics;
  private Map<Integer, List<String>> timeBucketToMetricNames;
  private WakeableLooper runnableToGatherMetrics;
  private CumulativeCountMetric metricCollectionCount;

  private Communicator<Metrics.MetricPublisherPublishMessage> queue;

  public MetricsCollector(WakeableLooper runnableToGatherMetrics,
                          Communicator<Metrics.MetricPublisherPublishMessage> queue) {
    metrics = new ConcurrentHashMap<>();
    timeBucketToMetricNames = new ConcurrentHashMap<>();
    this.queue = queue;
    this.runnableToGatherMetrics = runnableToGatherMetrics;
    metricCollectionCount = new CumulativeCountMetric();
  }

  @Override
  public <T extends IMetric<U>, U> T registerMetric(
      String name,
      T metric,
      final int timeBucketSizeInSecs) {
    if (metrics.containsKey(name)) {
      throw new RuntimeException("Another metric has already been registered with name: " + name);
    }
    metrics.put(name, metric);
    if (timeBucketToMetricNames.containsKey(timeBucketSizeInSecs)) {
      timeBucketToMetricNames.get(timeBucketSizeInSecs).add(name);
    } else {
      timeBucketToMetricNames.put(timeBucketSizeInSecs, new LinkedList<String>());
      timeBucketToMetricNames.get(timeBucketSizeInSecs).add(name);

      Runnable r = new Runnable() {
        public void run() {
          gatherMetrics(timeBucketSizeInSecs);
        }
      };

      runnableToGatherMetrics.registerTimerEvent(Duration.ofSeconds(timeBucketSizeInSecs), r);
    }

    return metric;
  }

  public void registerMetricSampleRunnable(
      final Runnable sampleRunnable,
      final Duration sampleInterval) {
    Runnable sampleTimer = new Runnable() {
      @Override
      public void run() {
        sampleRunnable.run();
        runnableToGatherMetrics.registerTimerEvent(sampleInterval, this);
      }
    };

    runnableToGatherMetrics.registerTimerEvent(sampleInterval, sampleTimer);
  }

  // Force to gather all metrics and put them into the queue
  // It is most likely happen when we are handling some unexpected cases, such as exiting
  public void forceGatherAllMetrics() {
    LOG.info("Forcing to gather all metrics and flush out.");
    Metrics.MetricPublisherPublishMessage.Builder builder =
        Metrics.MetricPublisherPublishMessage.newBuilder();

    for (List<String> metricNames : timeBucketToMetricNames.values()) {
      for (String metricName : metricNames) {
        gatherOneMetric(metricName, builder);
      }
    }

    metricCollectionCount.incr();
    addDataToMetricPublisher(builder, COLLECTION_COUNT_NAME, metricCollectionCount);

    Metrics.MetricPublisherPublishMessage msg = builder.build();

    queue.offer(msg);
  }

  private void addDataToMetricPublisher(Metrics.MetricPublisherPublishMessage.Builder builder,
                                        String metricName,
                                        Object metricValue) {
    // Metric name is discarded if value is of type MetricsDatum or ExceptionData.
    if (metricValue instanceof Metrics.MetricDatum.Builder) {
      builder.addMetrics((Metrics.MetricDatum.Builder) metricValue);
    } else if (metricValue instanceof Metrics.ExceptionData.Builder) {
      builder.addExceptions((Metrics.ExceptionData.Builder) metricValue);
    } else {
      assert metricName != null;
      Metrics.MetricDatum.Builder d = Metrics.MetricDatum.newBuilder();
      d.setName(metricName).setValue(metricValue.toString());
      builder.addMetrics(d);
    }
  }

  @SuppressWarnings("unchecked")
  private void gatherMetrics(final int timeBucketSizeInSecs) {
    // Gather the metrics in Map<String, IMetric> metrics
    // We will get the correct metrics by:
    // 1. Find the name in Map<Integer, List<String>> timeBucketToMetricNames
    //    by timeBucketSizeInSecs
    // 2. Find the IMetric in Map<String, IMetric> metrics by the name
    if (timeBucketToMetricNames.containsKey(timeBucketSizeInSecs)) {
      Metrics.MetricPublisherPublishMessage.Builder builder =
          Metrics.MetricPublisherPublishMessage.newBuilder();
      for (String metricName : timeBucketToMetricNames.get(timeBucketSizeInSecs)) {
        gatherOneMetric(metricName, builder);
      }

      metricCollectionCount.incr();
      addDataToMetricPublisher(builder, COLLECTION_COUNT_NAME,
                               metricCollectionCount.getValueAndReset());

      Metrics.MetricPublisherPublishMessage msg = builder.build();

      queue.offer(msg);

      // Schedule ourselves again -- Replay itself
      // TODO: Use TimerTask.
      Runnable r = new Runnable() {
        public void run() {
          gatherMetrics(timeBucketSizeInSecs);
        }
      };
      runnableToGatherMetrics.registerTimerEvent(Duration.ofSeconds(timeBucketSizeInSecs), r);
    }
  }

  // Gather the value of given metricName, convert it  into protobuf,
  // and add it to MetricPublisherPublishMessage builder given.
  @SuppressWarnings("unchecked")
  private void gatherOneMetric(
      String metricName,
      Metrics.MetricPublisherPublishMessage.Builder builder) {
    Object metricValue = metrics.get(metricName).getValueAndReset();
    // Decide how to handle the metric based on type
    if (metricValue == null) {
      return;
    }
    if (metricValue instanceof Map) {
      for (Map.Entry<Object, Object> entry : ((Map<Object, Object>) metricValue).entrySet()) {
        if (entry.getKey() != null && entry.getValue() != null) {
          addDataToMetricPublisher(
              builder, metricName + "/" + entry.getKey().toString(), entry.getValue());
        }
      }
    } else if (metricValue instanceof Collection) {
      int index = 0;
      for (Object value : (Collection) metricValue) {
        addDataToMetricPublisher(builder, metricName + "/" + (index++), value);
      }
    } else {
      addDataToMetricPublisher(builder, metricName, metricValue);
    }
  }
}
