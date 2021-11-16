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

package org.apache.heron.metricsmgr.sink.tmanager;

import java.time.Duration;
import java.util.logging.Logger;

import com.google.protobuf.Message;

import org.apache.heron.common.basics.Communicator;
import org.apache.heron.common.basics.NIOLooper;
import org.apache.heron.common.network.HeronClient;
import org.apache.heron.common.network.HeronSocketOptions;
import org.apache.heron.common.network.StatusCode;
import org.apache.heron.proto.tmanager.TopologyManager;

/**
 * TManagerClient connects to TManager and then send TopologyManager.PublishMetrics continuously.
 * Note that TManager will not send registerRequest or wait for registerResponse.
 */
public class TManagerClient extends HeronClient implements Runnable {
  private static final Logger LOG = Logger.getLogger(TManagerClient.class.getName());
  private final Communicator<TopologyManager.PublishMetrics> publishMetricsCommunicator;
  private final Duration reconnectInterval;

  /**
   * Constructor
   *
   * @param s the NIOLooper bind with this socket client
   * @param host the host of remote endpoint to communicate with
   * @param port the port of remote endpoint to communicate with
   * @param publishMetricsCommunicator the queue to read PublishMetrics from and send to TManager
   */
  public TManagerClient(NIOLooper s, String host, int port, HeronSocketOptions options,
                       Communicator<TopologyManager.PublishMetrics> publishMetricsCommunicator,
                       Duration reconnectInterval) {
    super(s, host, port, options);
    this.publishMetricsCommunicator = publishMetricsCommunicator;
    this.reconnectInterval = reconnectInterval;
  }

  @Override
  public void onError() {
    LOG.severe("Disconnected from TManager.");
    throw new RuntimeException("Errors happened due to write or read failure from TManager.");
    // We would not clear the publishMetricsCommunicator since we need to copy items from it
    // to the new one to avoid data loss
  }

  @Override
  public void onConnect(StatusCode status) {
    if (status != StatusCode.OK) {
      LOG.severe("Cannot connect to the TManager port, Will Retry..");
      if (reconnectInterval != Duration.ZERO) {
        Runnable r = new Runnable() {
          public void run() {
            start();
          }
        };
        getNIOLooper().registerTimerEvent(reconnectInterval, r);
      }
      return;
    }

    addTManagerClientTasksOnWakeUp();

    LOG.info("Connected to TManager. Ready to send metrics");
  }

  private void addTManagerClientTasksOnWakeUp() {
    Runnable task = new Runnable() {
      @Override
      public void run() {
        TopologyManager.PublishMetrics publishMetrics;
        while (true) {
          synchronized (publishMetricsCommunicator) {
            publishMetrics = publishMetricsCommunicator.poll();
          }
          if (publishMetrics == null) {
            break;  // No metrics left
          }

          LOG.info(String.format("%d Metrics, %d Exceptions to send to TManager",
              publishMetrics.getMetricsCount(), publishMetrics.getExceptionsCount()));
          LOG.fine("Publish Metrics sending to TManager: " + publishMetrics.toString());

          sendMessage(publishMetrics);
        }
      }
    };
    getNIOLooper().addTasksOnWakeup(task);
  }

  @Override
  public void onResponse(StatusCode status, Object ctx, Message response) {
    LOG.severe("TManagerClient got an unknown response from TManager");
  }

  @Override
  public void onIncomingMessage(Message message) {
    LOG.severe("TManagerClient got an unknown message from TManager");
  }

  @Override
  public void onClose() {
    LOG.info("TManagerClient exits");
  }

  @Override
  public void run() {
    this.start();
    getNIOLooper().loop();
  }
}
