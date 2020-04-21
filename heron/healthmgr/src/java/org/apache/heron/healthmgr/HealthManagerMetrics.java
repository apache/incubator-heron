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

package org.apache.heron.healthmgr;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.Map.Entry;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.inject.Inject;
import javax.inject.Singleton;

import com.google.protobuf.Message;

import org.apache.heron.api.metric.MultiCountMetric;
import org.apache.heron.common.basics.NIOLooper;
import org.apache.heron.common.basics.SingletonRegistry;
import org.apache.heron.common.config.SystemConfig;
import org.apache.heron.common.network.HeronClient;
import org.apache.heron.common.network.HeronSocketOptions;
import org.apache.heron.common.network.StatusCode;
import org.apache.heron.common.utils.metrics.JVMMetrics;
import org.apache.heron.proto.system.Common;
import org.apache.heron.proto.system.Metrics;

/**
 * HealthMgr's metrics to be collect
 */
@Singleton
public class HealthManagerMetrics implements Runnable, AutoCloseable {
  public static final String METRICS_THREAD = "HealthManagerMetrics";
  private static final Logger LOG = Logger.getLogger(HealthManagerMetrics.class.getName());
  private static final String METRICS_MGR_HOST = "127.0.0.1";

  private final String metricsPrefix = "__healthmgr/";
  private final String metricsSensor = metricsPrefix + "sensor/";
  private final String metricsDetector = metricsPrefix + "detector/";
  private final String metricsDiagnoser = metricsPrefix + "diagnoser/";
  private final String metricsResolver = metricsPrefix + "resolver/";
  private final String metricsName = metricsPrefix + "customized/";
  private final JVMMetrics jvmMetrics;
  private final MultiCountMetric executeSensorCount;
  private final MultiCountMetric executeDetectorCount;
  private final MultiCountMetric executeDiagnoserCount;
  private final MultiCountMetric executeResolverCount;
  private final MultiCountMetric executeCount;

  private NIOLooper looper;
  private HeronClient metricsMgrClient;

  /**
   * constructor to expose healthmgr metrics to local metricsmgr
   * @param metricsMgrPort local MetricsMgr port
   * @throws IOException
   */
  @Inject
  public HealthManagerMetrics(int metricsMgrPort) throws IOException {
    jvmMetrics = new JVMMetrics();

    executeSensorCount = new MultiCountMetric();
    executeDetectorCount = new MultiCountMetric();
    executeDiagnoserCount = new MultiCountMetric();
    executeResolverCount = new MultiCountMetric();
    executeCount = new MultiCountMetric();

    looper = new NIOLooper();

    SystemConfig systemConfig =
        (SystemConfig) SingletonRegistry.INSTANCE.getSingleton(SystemConfig.HERON_SYSTEM_CONFIG);

    HeronSocketOptions socketOptions =
        new HeronSocketOptions(systemConfig.getInstanceNetworkWriteBatchSize(),
            systemConfig.getInstanceNetworkWriteBatchTime(),
            systemConfig.getInstanceNetworkReadBatchSize(),
            systemConfig.getInstanceNetworkReadBatchTime(),
            systemConfig.getInstanceNetworkOptionsSocketSendBufferSize(),
            systemConfig.getInstanceNetworkOptionsSocketReceivedBufferSize(),
            systemConfig.getInstanceNetworkOptionsMaximumPacketSize());
    metricsMgrClient =
        new SimpleMetricsManagerClient(looper, METRICS_MGR_HOST, metricsMgrPort, socketOptions);

    int interval = (int) systemConfig.getHeronMetricsExportInterval().getSeconds();


    looper.registerPeriodicEvent(Duration.ofSeconds(interval), new Runnable() {
      @Override
      public void run() {
        sendMetrics();
      }
    });
  }

  private void sendMetrics() {
    jvmMetrics.getJVMSampleRunnable().run();

    if (!metricsMgrClient.isConnected()) {
      return;
    }

    LOG.info("Flushing sensor/detector/diagnoser/resolver metrics");
    Metrics.MetricPublisherPublishMessage.Builder builder =
        Metrics.MetricPublisherPublishMessage.newBuilder();
    addMetrics(builder, executeSensorCount, metricsSensor);
    addMetrics(builder, executeDetectorCount, metricsDetector);
    addMetrics(builder, executeDiagnoserCount, metricsDiagnoser);
    addMetrics(builder, executeResolverCount, metricsResolver);
    addMetrics(builder, executeCount, metricsName);
    Metrics.MetricPublisherPublishMessage msg = builder.build();
    LOG.fine(msg.toString());
    metricsMgrClient.sendMessage(msg);
  }

  private void addMetrics(Metrics.MetricPublisherPublishMessage.Builder b, MultiCountMetric m,
      String prefix) {
    for (Entry<String, Long> e : m.getValueAndReset().entrySet()) {
      b.addMetrics(Metrics.MetricDatum.newBuilder().setName(prefix + e.getKey())
          .setValue(e.getValue().toString()));
    }
  }

  public synchronized void executeIncr(String metricName) {
    executeCount.scope(metricName).incr();
  }

  public synchronized void executeSensorIncr(String sensor) {
    executeSensorCount.scope(sensor).incr();
  }

  public synchronized void executeDetectorIncr(String detector) {
    executeDetectorCount.scope(detector).incr();
  }

  public synchronized void executeDiagnoserIncr(String diagnoser) {
    executeDiagnoserCount.scope(diagnoser).incr();
  }

  public synchronized void executeResolver(String resolver) {
    executeResolverCount.scope(resolver).incr();
  }

  @Override
  public void run() {
    metricsMgrClient.start();
    looper.loop();
  }

  @Override
  public void close() throws Exception {
    looper.exitLoop();
    metricsMgrClient.stop();
  }

  class SimpleMetricsManagerClient extends HeronClient {
    private SystemConfig systemConfig;
    private String hostname;

    SimpleMetricsManagerClient(NIOLooper s, String host, int port, HeronSocketOptions options) {
      super(s, host, port, options);
      systemConfig =
          (SystemConfig) SingletonRegistry.INSTANCE.getSingleton(SystemConfig.HERON_SYSTEM_CONFIG);
      try {
        this.hostname = InetAddress.getLocalHost().getHostName();
      } catch (UnknownHostException e) {
        throw new RuntimeException("GetHostName failed");
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
      if (status != StatusCode.OK) {
        LOG.log(Level.WARNING,
            "Cannot connect to the local metrics mgr with status: {0}, Will Retry..", status);
        Runnable r = new Runnable() {
          public void run() {
            start();
          }
        };

        getNIOLooper().registerTimerEvent(systemConfig.getInstanceReconnectMetricsmgrInterval(), r);
        return;
      }

      LOG.info("Connected to Metrics Manager. Ready to send register request");
      sendRegisterRequest();
    }

    private void sendRegisterRequest() {
      Metrics.MetricPublisher publisher = Metrics.MetricPublisher.newBuilder().setHostname(hostname)
          .setPort(getSocketChannel().socket().getPort()).setComponentName("__healthmgr__")
          .setInstanceId("healthmgr-0").setInstanceIndex(-1).build();
      Metrics.MetricPublisherRegisterRequest request =
          Metrics.MetricPublisherRegisterRequest.newBuilder().setPublisher(publisher).build();

      // The timeout would be the reconnect-interval-seconds
      sendRequest(request, null, Metrics.MetricPublisherRegisterResponse.newBuilder(),
          systemConfig.getInstanceReconnectMetricsmgrInterval());
    }

    @Override
    public void onResponse(StatusCode status, Object ctx, Message response) {
      if (status != StatusCode.OK) {
        throw new RuntimeException("Response from Metrics Manager not ok");
      }
      if (Metrics.MetricPublisherRegisterResponse.class.isInstance(response)) {
        handleRegisterResponse((Metrics.MetricPublisherRegisterResponse) response);
      } else {
        throw new RuntimeException("Unknown kind of response received from Metrics Manager");
      }
    }

    private void handleRegisterResponse(Metrics.MetricPublisherRegisterResponse response) {
      if (response.getStatus().getStatus() != Common.StatusCode.OK) {
        throw new RuntimeException("Metrics Manager returned a not ok response for register");
      }

      LOG.info("We registered ourselves to the Metrics Manager");
    }

    @Override
    public void onIncomingMessage(Message message) {
      throw new RuntimeException(
          "SimpleMetricsManagerClient got an unknown message from Metrics Manager");
    }

    @Override
    public void onClose() {
      LOG.info("SimpleMetricsManagerClient exits");
    }

  }

}
