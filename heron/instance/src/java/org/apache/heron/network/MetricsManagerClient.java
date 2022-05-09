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

package org.apache.heron.network;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.protobuf.Message;

import org.apache.heron.common.basics.Communicator;
import org.apache.heron.common.basics.NIOLooper;
import org.apache.heron.common.basics.SingletonRegistry;
import org.apache.heron.common.config.SystemConfig;
import org.apache.heron.common.network.HeronClient;
import org.apache.heron.common.network.HeronSocketOptions;
import org.apache.heron.common.network.StatusCode;
import org.apache.heron.metrics.GatewayMetrics;
import org.apache.heron.proto.system.Common;
import org.apache.heron.proto.system.Metrics;
import org.apache.heron.proto.system.PhysicalPlans;

/**
 * MetricsClient implements SocketClient and communicate with Metrics Manager, it will:
 * 1. Check whether we got a PhysicalPlanHelper in singletonRegistry, and if we got one we will
 * send the register request; so it will not send registerRequest directly when it is connected
 * 2. Handle relative response for register request
 * 3. It will no need call the onIncomingMessage(), since it will not accept any message
 */
public class MetricsManagerClient extends HeronClient {
  private static final Logger LOG = Logger.getLogger(MetricsManagerClient.class.getName());

  private final PhysicalPlans.Instance instance;

  private final List<Communicator<Metrics.MetricPublisherPublishMessage>> outMetricsQueues;

  private final SystemConfig systemConfig;

  private final GatewayMetrics gatewayMetrics;

  private String hostname;

  public MetricsManagerClient(NIOLooper s, String metricsHost, int metricsPort,
                              PhysicalPlans.Instance instance,
                              List<Communicator<Metrics.MetricPublisherPublishMessage>> outs,
                              HeronSocketOptions options,
                              GatewayMetrics gatewayMetrics) {
    super(s, metricsHost, metricsPort, options);

    this.instance = instance;
    this.outMetricsQueues = outs;

    this.systemConfig =
        (SystemConfig) SingletonRegistry.INSTANCE.getSingleton(SystemConfig.HERON_SYSTEM_CONFIG);

    this.gatewayMetrics = gatewayMetrics;

    addMetricsManagerClientTasksOnWakeUp();

    try {
      this.hostname = InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException e) {
      throw new RuntimeException("GetHostName failed");
    }
  }

  private void addMetricsManagerClientTasksOnWakeUp() {
    Runnable task = new Runnable() {
      @Override
      public void run() {
        sendMetricsMessageIfNeeded();
      }
    };
    getNIOLooper().addTasksOnWakeup(task);
  }

  private void sendMetricsMessageIfNeeded() {
    if (isConnected()) {
      // We consider # of MetricsMessage would be small
      // So we just write them out all in one time
      for (Communicator<Metrics.MetricPublisherPublishMessage> c : outMetricsQueues) {

        while (!c.isEmpty()) {
          Metrics.MetricPublisherPublishMessage m = c.poll();
          gatewayMetrics.updateSentMetricsSize(m.getSerializedSize());
          gatewayMetrics.updateSentMetrics(m.getMetricsCount(), m.getExceptionsCount());

          sendMessage(m);
        }
      }
    }
  }

  // Send out all the data
  public void sendAllMessage() {
    if (!isConnected()) {
      return;
    }

    LOG.info("Flushing all pending data in MetricsManagerClient");
    // Collect all tuples in queue
    for (Communicator<Metrics.MetricPublisherPublishMessage> c : outMetricsQueues) {
      int size = c.size();
      for (int i = 0; i < size; i++) {
        Metrics.MetricPublisherPublishMessage m = c.poll();
        sendMessage(m);
      }
    }
  }

  @Override
  public void onError() {
    LOG.severe("Disconnected from Metrics Manager.");

    // Dispatch to onConnect(...)
    onConnect(StatusCode.CONNECT_ERROR);
  }

  @Override
  public void onConnect(StatusCode status) {
    // We will not send registerRequest when we are onConnect
    // We will send when we receive the PhysicalPlan sent by executor
    if (status != StatusCode.OK) {
      LOG.log(Level.WARNING,
          "Cannot connect to the metrics port with status: {0}, Will Retry..", status);
      Runnable r = new Runnable() {
        public void run() {
          start();
        }
      };
      getNIOLooper().registerTimerEvent(
          systemConfig.getInstanceReconnectMetricsmgrInterval(), r);
      return;
    }

    LOG.info("Connected to Metrics Manager. Ready to send register request");
    sendRegisterRequest();
  }

  // Build register request and send to metrics mgr
  private void sendRegisterRequest() {
    Metrics.MetricPublisher publisher = Metrics.MetricPublisher.newBuilder().
        setHostname(hostname).
        setPort(instance.getInfo().getTaskId()).
        setComponentName(instance.getInfo().getComponentName()).
        setInstanceId(instance.getInstanceId()).
        setInstanceIndex(instance.getInfo().getComponentIndex()).
        build();
    Metrics.MetricPublisherRegisterRequest request =
        Metrics.MetricPublisherRegisterRequest.newBuilder().
            setPublisher(publisher).
            build();

    // The timeout would be the reconnect-interval-seconds
    sendRequest(request, null,
        Metrics.MetricPublisherRegisterResponse.newBuilder(),
        systemConfig.getInstanceReconnectMetricsmgrInterval());
  }

  @Override
  public void onResponse(StatusCode status, Object ctx, Message response) {
    if (status != StatusCode.OK) {
      //TODO:- is this a good thing?
      throw new RuntimeException("Response from Metrics Manager not ok");
    }
    if (Metrics.MetricPublisherRegisterResponse.class.isInstance(response)) {
      handleRegisterResponse((Metrics.MetricPublisherRegisterResponse) response);
    } else {
      throw new RuntimeException("Unknown kind of response received from Metrics Manager");
    }
  }

  @Override
  public void onIncomingMessage(Message message) {
    throw new RuntimeException("MetricsClient got an unknown message from Metrics Manager");
  }

  @Override
  public void onClose() {
    LOG.info("MetricsManagerClient exits");
  }

  private void handleRegisterResponse(Metrics.MetricPublisherRegisterResponse response) {
    if (response.getStatus().getStatus() != Common.StatusCode.OK) {
      throw new RuntimeException("Metrics Manager returned a not ok response for register");
    }

    LOG.info("We registered ourselves to the Metrics Manager");
  }
}
