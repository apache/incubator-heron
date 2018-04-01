// Copyright 2018 Twitter. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.twitter.heron.healthmgr;

import java.io.IOException;
import java.time.Duration;
import java.util.logging.Logger;

import com.google.protobuf.Message;

import com.twitter.heron.api.metric.MultiCountMetric;
import com.twitter.heron.common.basics.Communicator;
import com.twitter.heron.common.basics.NIOLooper;
import com.twitter.heron.common.basics.SingletonRegistry;
import com.twitter.heron.common.config.SystemConfig;
import com.twitter.heron.common.network.HeronClient;
import com.twitter.heron.common.network.HeronSocketOptions;
import com.twitter.heron.common.network.StatusCode;
import com.twitter.heron.common.utils.metrics.JVMMetrics;
import com.twitter.heron.proto.system.Metrics;

/**
 * HealthMgr's metrics to be collect
 */

public class HealthManagerMetrics implements Runnable, AutoCloseable {
  private static final Logger LOG = Logger.getLogger(HealthManagerMetrics.class.getName());
  private static final String METRICS_MGR_HOST = "127.0.0.1";

  private final JVMMetrics jvmMetrics;
  private final MultiCountMetric executeSensorCount;
  private final MultiCountMetric executeDetectorCount;
  private final MultiCountMetric executeDiagnoserCount;
  private final MultiCountMetric executeResolverCount;

  private NIOLooper looper;
  private HeronClient metricsMgrClient;
  private Communicator<Metrics.MetricPublisherPublishMessage> outMetricsQueues;

  /**
   * constructor to expose healthmgr metrics to local metricsmgr
   * @param metricsMgrPort local MetricsMgr port
   * @throws IOException
   */
  public HealthManagerMetrics(int metricsMgrPort) throws IOException {
    jvmMetrics = new JVMMetrics();

    executeSensorCount = new MultiCountMetric();
    executeDetectorCount = new MultiCountMetric();
    executeDiagnoserCount = new MultiCountMetric();
    executeResolverCount = new MultiCountMetric();

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
        new MetricsMgrClient(looper, METRICS_MGR_HOST, metricsMgrPort, socketOptions);

    outMetricsQueues = new Communicator<Metrics.MetricPublisherPublishMessage>(null, looper);
    outMetricsQueues.init(systemConfig.getInstanceInternalMetricsWriteQueueCapacity(),
        systemConfig.getInstanceTuningExpectedMetricsWriteQueueSize(),
        systemConfig.getInstanceTuningCurrentSampleWeight());

    int interval = (int) systemConfig.getHeronMetricsExportInterval().getSeconds();

    looper.registerTimerEvent(Duration.ofSeconds(interval), new Runnable() {
      @Override
      public void run() {
        jvmMetrics.getJVMSampleRunnable().run();

        // push to container 0 metricsMgr
        if (!metricsMgrClient.isConnected()) {
          return;
        }

        LOG.info("Flushing all pending data in MetricsManagerClient");
        // Collect all tuples in queue
        int size = outMetricsQueues.size();
        for (int i = 0; i < size; i++) {
          Metrics.MetricPublisherPublishMessage m = outMetricsQueues.poll();
          metricsMgrClient.sendMessage(m);
        }
      }
    });
  }

  public Communicator<Metrics.MetricPublisherPublishMessage> getMetricsQueue() {
    return outMetricsQueues;
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
    outMetricsQueues.clear();
  }

  class MetricsMgrClient extends HeronClient {

    MetricsMgrClient(NIOLooper s, String host, int port, HeronSocketOptions options) {
      super(s, host, port, options);
      // TODO Auto-generated constructor stub
    }

    @Override
    public void onError() {
      // TODO Auto-generated method stub

    }

    @Override
    public void onConnect(StatusCode status) {
      // TODO Auto-generated method stub

    }

    @Override
    public void onResponse(StatusCode status, Object ctx, Message response) {
      // TODO Auto-generated method stub

    }

    @Override
    public void onIncomingMessage(Message message) {
      // TODO Auto-generated method stub

    }

    @Override
    public void onClose() {
      // TODO Auto-generated method stub

    }

  }

}
