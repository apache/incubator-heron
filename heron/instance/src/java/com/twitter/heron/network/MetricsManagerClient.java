package com.twitter.heron.network;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.logging.Logger;

import com.google.protobuf.Message;

import com.twitter.heron.common.config.SystemConfig;
import com.twitter.heron.common.basics.Communicator;
import com.twitter.heron.common.basics.NIOLooper;
import com.twitter.heron.common.basics.SingletonRegistry;
import com.twitter.heron.common.network.HeronClient;
import com.twitter.heron.common.network.HeronSocketOptions;
import com.twitter.heron.common.network.StatusCode;
import com.twitter.heron.metrics.GatewayMetrics;
import com.twitter.heron.proto.system.Common;
import com.twitter.heron.proto.system.Metrics;
import com.twitter.heron.proto.system.PhysicalPlans;

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

  public MetricsManagerClient(NIOLooper s, String MetricsHost, int MetricsPort,
                              PhysicalPlans.Instance instance,
                              List<Communicator<Metrics.MetricPublisherPublishMessage>> outs,
                              HeronSocketOptions options,
                              GatewayMetrics gatewayMetrics) {
    super(s, MetricsHost, MetricsPort, options);

    this.instance = instance;
    this.outMetricsQueues = outs;

    this.systemConfig =
        (SystemConfig) SingletonRegistry.INSTANCE.getSingleton(SystemConfig.HERON_SYSTEM_CONFIG);

    this.gatewayMetrics = gatewayMetrics;

    addMetricsManagerClientTasksOnWakeUp();
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
    // We will send when we receive the PhysicalPlan sent by slave
    if (status != StatusCode.OK) {
      LOG.severe(String.format("Cannot connect to the metrics port with status: %s, Will Retry..", status));
      Runnable r = new Runnable() {
        public void run() {
          start();
        }
      };
      getNIOLooper().registerTimerEventInSeconds(systemConfig.getInstanceReconnectMetricsmgrIntervalSec(), r);
      return;
    }

    LOG.info("Connected to Metrics Manager. Ready to send register request");
    sendRegisterRequest();
  }

  // Build register request and send to metrics mgr
  private void sendRegisterRequest() {
    String hostname;
    try {
      hostname = InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException e) {
      throw new RuntimeException("GetHostName failed");
    }

    Metrics.MetricPublisher publisher = Metrics.MetricPublisher.newBuilder().
        setHostname(hostname).
        setPort(instance.getInfo().getTaskId()).
        setComponentName(instance.getInfo().getComponentName()).
        setInstanceId(instance.getInstanceId()).
        setInstanceIndex(instance.getInfo().getComponentIndex()).
        build();
    Metrics.MetricPublisherRegisterRequest request = Metrics.MetricPublisherRegisterRequest.newBuilder().
        setPublisher(publisher).build();

    // The timeout would be the reconnect-interval-seconds
    sendRequest(request, null,
        Metrics.MetricPublisherRegisterResponse.newBuilder(),
        systemConfig.getInstanceReconnectMetricsmgrIntervalSec());
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
