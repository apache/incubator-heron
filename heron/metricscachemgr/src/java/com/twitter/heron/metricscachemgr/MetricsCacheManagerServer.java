// Copyright 2016 Twitter. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.twitter.heron.metricscachemgr;

import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.protobuf.Message;

import com.twitter.heron.api.metric.MultiCountMetric;
import com.twitter.heron.common.basics.NIOLooper;
import com.twitter.heron.common.network.HeronServer;
import com.twitter.heron.common.network.HeronSocketOptions;
import com.twitter.heron.common.network.REQID;
import com.twitter.heron.metricsmgr.MetricsManagerServer;
import com.twitter.heron.proto.system.Metrics;
import com.twitter.heron.spi.metricsmgr.metrics.ExceptionInfo;
import com.twitter.heron.spi.metricsmgr.metrics.MetricsInfo;
import com.twitter.heron.spi.metricsmgr.metrics.MetricsRecord;

/**
 * server to accept metrics from a particular sink in metrics manager
 */
public class MetricsCacheManagerServer extends HeronServer {
  private static final Logger LOG = Logger.getLogger(MetricsManagerServer.class.getName());
  // Internal MultiCountMetric Counters
  private final MultiCountMetric serverMetricsCounters;

  /**
   * Constructor
   *
   * @param s the NIOLooper bind with this socket server
   * @param host the host of remote endpoint to communicate with
   * @param port the port of remote endpoint to communicate with
   */
  public MetricsCacheManagerServer(NIOLooper s, String host,
                                   int port, HeronSocketOptions options,
                                   MultiCountMetric serverMetricsCounters) {
    super(s, host, port, options);

    if (serverMetricsCounters == null) {
      throw new IllegalArgumentException("Server Metrics Counters is needed.");
    }
    this.serverMetricsCounters = serverMetricsCounters;

  }

  // We also allow directly send Metrics Message internally to invoke IMetricsSink
  // This method is thread-safe, since we would push Messages into a Concurrent Queue.
  public void onInternalMessage(Metrics.MetricPublisher request,
                                Metrics.MetricPublisherPublishMessage message) {
    handlePublisherPublishMessage(request, message);
  }

  private void handlePublisherPublishMessage(Metrics.MetricPublisher request,
                                             Metrics.MetricPublisherPublishMessage message) {
    if (message.getMetricsCount() <= 0 && message.getExceptionsCount() <= 0) {
      LOG.log(Level.SEVERE,
          "Publish message has no metrics nor exceptions for message from hostname: {0},"
              + " component_name: {1}, port: {2}, instance_id: {3}, instance_index: {4}",
          new Object[]{request.getHostname(), request.getComponentName(), request.getPort(),
              request.getInstanceId(), request.getInstanceIndex()});
      return;
    }

    // Convert the message to MetricsRecord
    String source = String.format("%s:%d/%s/%s",
        request.getHostname(), request.getPort(),
        request.getComponentName(), request.getInstanceId());

    List<MetricsInfo> metricsInfos = new ArrayList<MetricsInfo>(message.getMetricsCount());
    for (Metrics.MetricDatum metricDatum : message.getMetricsList()) {
      MetricsInfo info = new MetricsInfo(metricDatum.getName(), metricDatum.getValue());
      metricsInfos.add(info);
    }

    List<ExceptionInfo> exceptionInfos = new ArrayList<ExceptionInfo>(message.getExceptionsCount());
    for (Metrics.ExceptionData exceptionData : message.getExceptionsList()) {
      ExceptionInfo exceptionInfo =
          new ExceptionInfo(exceptionData.getStacktrace(),
              exceptionData.getLasttime(),
              exceptionData.getFirsttime(),
              exceptionData.getCount(),
              exceptionData.getLogging());
      exceptionInfos.add(exceptionInfo);
    }

    LOG.info(String.format("%d MetricsInfo and %d ExceptionInfo to push",
        metricsInfos.size(), exceptionInfos.size()));

    // Update the metrics
//    serverMetricsCounters.scope(SERVER_METRICS_RECEIVED).incrBy(metricsInfos.size());
//    serverMetricsCounters.scope(SERVER_EXCEPTIONS_RECEIVED).incrBy(exceptionInfos.size());


    MetricsRecord record = new MetricsRecord(source, metricsInfos, exceptionInfos);

    // Push MetricsRecord to Communicator, which would wake up SlaveLooper bind with IMetricsSink
//    for (Communicator<MetricsRecord> c : metricsSinkCommunicators) {
//      c.offer(record);
//    }
  }

  @Override
  public void onConnect(SocketChannel channel) {

  }

  @Override
  public void onRequest(REQID rid, SocketChannel channel, Message request) {

  }

  @Override
  public void onMessage(SocketChannel channel, Message message) {

  }

  @Override
  public void onClose(SocketChannel channel) {

  }
}
