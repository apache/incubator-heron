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

package com.twitter.heron.metricsmgr.sink.metricscache;

import java.util.logging.Logger;

import com.google.protobuf.Message;

import com.twitter.heron.common.basics.Communicator;
import com.twitter.heron.common.basics.NIOLooper;
import com.twitter.heron.common.network.HeronClient;
import com.twitter.heron.common.network.HeronSocketOptions;
import com.twitter.heron.common.network.StatusCode;
import com.twitter.heron.proto.tmaster.TopologyMaster;

/**
 * MetricsCacheClient connects to MetricsCache and then send TopologyMaster.PublishMetrics continuously.
 * Note that MetricsCache will not send registerRequest or wait for registerResponse.
 */
public class MetricsCacheClient extends HeronClient implements Runnable {
  private static final Logger LOG = Logger.getLogger(MetricsCacheClient.class.getName());
  private final Communicator<TopologyMaster.PublishMetrics> publishMetricsCommunicator;
  private String host;
  private int port;
  private long reconnectIntervalSec = -1;

  /**
   * Constructor
   *
   * @param s the NIOLooper bind with this socket client
   * @param host the host of remote endpoint to communicate with
   * @param port the port of remote endpoint to communicate with
   * @param publishMetricsCommunicator the queue to read PublishMetrics from and send to MetricsCache
   */
  public MetricsCacheClient(
      NIOLooper s, String host, int port, HeronSocketOptions options,
      Communicator<TopologyMaster.PublishMetrics> publishMetricsCommunicator) {
    super(s, host, port, options);
    this.host = host;
    this.port = port;
    this.publishMetricsCommunicator = publishMetricsCommunicator;
  }

  public void setReconnectIntervalSec(long interval) {
    this.reconnectIntervalSec = interval;
  }

  @Override
  public void onError() {
    LOG.severe("Disconnected from MetricsCache.");
    throw new RuntimeException("Errors happened due to write or read failure from MetricsCache.");
    // We would not clear the publishMetricsCommunicator since we need to copy items from it
    // to the new one to avoid data loss
  }

  @Override
  public void onConnect(StatusCode status) {
    if (status != StatusCode.OK) {
      LOG.severe("Cannot connect to the MetricsCache at " + host + ":" + port + ", Will Retry..");
      if (reconnectIntervalSec > 0) {
        Runnable r = new Runnable() {
          public void run() {
            start();
          }
        };
        getNIOLooper().registerTimerEventInSeconds(reconnectIntervalSec, r);
      }
      return;
    }

    addMetricsCacheClientTasksOnWakeUp();

    LOG.info("Connected to MetricsCache. Ready to send metrics");
  }

  private void addMetricsCacheClientTasksOnWakeUp() {
    Runnable task = new Runnable() {
      @Override
      public void run() {
        while (!publishMetricsCommunicator.isEmpty()) {
          TopologyMaster.PublishMetrics publishMetrics = publishMetricsCommunicator.poll();
          LOG.info(String.format("%d Metrics, %d Exceptions to send to MetricsCache",
              publishMetrics.getMetricsCount(), publishMetrics.getExceptionsCount()));
          LOG.fine("Publish Metrics sending to MetricsCache: " + publishMetrics.toString());
          sendMessage(publishMetrics);
        }
      }
    };
    getNIOLooper().addTasksOnWakeup(task);
  }

  @Override
  public void onResponse(StatusCode status, Object ctx, Message response) {
    LOG.severe("MetricsCacheClient got an unknown response from MetricsCache");
  }

  @Override
  public void onIncomingMessage(Message message) {
    LOG.severe("MetricsCacheClient got an unknown message from MetricsCache");
  }

  @Override
  public void onClose() {
    LOG.info("MetricsCacheClient exits");
  }

  @Override
  public void run() {
    this.start();
    getNIOLooper().loop();
  }
}
