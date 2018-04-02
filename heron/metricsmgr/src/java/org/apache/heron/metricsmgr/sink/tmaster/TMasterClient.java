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

package org.apache.heron.metricsmgr.sink.tmaster;

import java.time.Duration;
import java.util.logging.Logger;

import com.google.protobuf.Message;

import org.apache.heron.common.basics.Communicator;
import org.apache.heron.common.basics.NIOLooper;
import org.apache.heron.common.network.HeronClient;
import org.apache.heron.common.network.HeronSocketOptions;
import org.apache.heron.common.network.StatusCode;
import org.apache.heron.proto.tmaster.TopologyMaster;

/**
 * TMasterClient connects to TMaster and then send TopologyMaster.PublishMetrics continuously.
 * Note that TMaster will not send registerRequest or wait for registerResponse.
 */
public class TMasterClient extends HeronClient implements Runnable {
  private static final Logger LOG = Logger.getLogger(TMasterClient.class.getName());
  private final Communicator<TopologyMaster.PublishMetrics> publishMetricsCommunicator;
  private final Duration reconnectInterval;

  /**
   * Constructor
   *
   * @param s the NIOLooper bind with this socket client
   * @param host the host of remote endpoint to communicate with
   * @param port the port of remote endpoint to communicate with
   * @param publishMetricsCommunicator the queue to read PublishMetrics from and send to TMaster
   */
  public TMasterClient(NIOLooper s, String host, int port, HeronSocketOptions options,
                       Communicator<TopologyMaster.PublishMetrics> publishMetricsCommunicator,
                       Duration reconnectInterval) {
    super(s, host, port, options);
    this.publishMetricsCommunicator = publishMetricsCommunicator;
    this.reconnectInterval = reconnectInterval;
  }

  @Override
  public void onError() {
    LOG.severe("Disconnected from TMaster.");
    throw new RuntimeException("Errors happened due to write or read failure from TMaster.");
    // We would not clear the publishMetricsCommunicator since we need to copy items from it
    // to the new one to avoid data loss
  }

  @Override
  public void onConnect(StatusCode status) {
    if (status != StatusCode.OK) {
      LOG.severe("Cannot connect to the TMaster port, Will Retry..");
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

    addTMasterClientTasksOnWakeUp();

    LOG.info("Connected to TMaster. Ready to send metrics");
  }

  private void addTMasterClientTasksOnWakeUp() {
    Runnable task = new Runnable() {
      @Override
      public void run() {
        while (!publishMetricsCommunicator.isEmpty()) {
          TopologyMaster.PublishMetrics publishMetrics = publishMetricsCommunicator.poll();
          LOG.info(String.format("%d Metrics, %d Exceptions to send to TMaster",
              publishMetrics.getMetricsCount(), publishMetrics.getExceptionsCount()));
          LOG.fine("Publish Metrics sending to TMaster: " + publishMetrics.toString());
          sendMessage(publishMetrics);
        }
      }
    };
    getNIOLooper().addTasksOnWakeup(task);
  }

  @Override
  public void onResponse(StatusCode status, Object ctx, Message response) {
    LOG.severe("TMasterClient got an unknown response from TMaster");
  }

  @Override
  public void onIncomingMessage(Message message) {
    LOG.severe("TMasterClient got an unknown message from TMaster");
  }

  @Override
  public void onClose() {
    LOG.info("TMasterClient exits");
  }

  @Override
  public void run() {
    this.start();
    getNIOLooper().loop();
  }
}
