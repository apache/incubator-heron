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

import java.io.IOException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.common.annotations.VisibleForTesting;

import com.twitter.heron.common.basics.Communicator;
import com.twitter.heron.common.basics.NIOLooper;
import com.twitter.heron.common.basics.SingletonRegistry;
import com.twitter.heron.common.basics.SysUtils;
import com.twitter.heron.common.basics.TypeUtils;
import com.twitter.heron.common.config.SystemConfig;
import com.twitter.heron.common.network.HeronSocketOptions;
import com.twitter.heron.metricsmgr.MetricsManagerServer;
import com.twitter.heron.proto.tmaster.TopologyMaster;
import com.twitter.heron.spi.metricsmgr.metrics.ExceptionInfo;
import com.twitter.heron.spi.metricsmgr.metrics.MetricsFilter;
import com.twitter.heron.spi.metricsmgr.metrics.MetricsInfo;
import com.twitter.heron.spi.metricsmgr.metrics.MetricsRecord;
import com.twitter.heron.spi.metricsmgr.sink.IMetricsSink;
import com.twitter.heron.spi.metricsmgr.sink.SinkContext;

/**
 * An IMetricsSink sends Metrics to MetricsCache.
 * 1. It gets the MetricsCacheLocation
 * <p>
 * 2. Then it would construct a long-live Service running metricsCacheClient, which could automatically
 * recover from uncaught exceptions, i.e. close the old one and start a new one.
 * Also, it provides api to update the MetricsCacheLocation that metricsCacheClient need to connect and
 * restart the metricsCacheClient.
 * There are two scenarios we need to restart a metricsCacheClient in our case:
 * <p>
 * -- Uncaught exceptions happen within metricsCacheClient; then we would restart metricsCacheClient inside
 * the same ExecutorService inside the UncaughtExceptionHandlers.
 * Notice that, in java, exceptions occur inside UncaughtExceptionHandlers would not invoke
 * UncaughtExceptionHandlers; instead, it would kill the thread with that exception.
 * So if exceptions thrown during restart a new metricsCacheClient, this MetricsCacheSink would die, and
 * external logic would take care of it.
 * <p>
 * -- MetricsCacheLocation changes (though in fact, metricsCacheClient might also throw exceptions in this case),
 * in this case, we would invoke MetricsCacheService to start from tMasterLocationStarter's thread.
 * But the MetricsCacheService and metricsCacheClient still start wihtin the thread they run.
 * <p>
 * 3. When a new MetricsRecord comes by invoking processRecord, it would push the MetricsRecord
 * to the Communicator Queue to metricsCacheClient
 * <p>
 * Notice that we would not send all metrics to MetricsCache; we would use MetricsFilter to figure out
 * needed metrics.
 */

public class MetricsCacheSink implements IMetricsSink {
  private static final Logger LOG = Logger.getLogger(MetricsCacheSink.class.getName());

  // These configs would be read from metrics-sink-configs.yaml
  private static final String KEY_TMASTER_LOCATION_CHECK_INTERVAL_SEC =
      "metricscache-location-check-interval-sec";
  private static final String KEY_TMASTER = "metricscache-client";
  private static final String KEY_TMASTER_RECONNECT_INTERVAL_SEC = "reconnect-interval-second";
  private static final String KEY_NETWORK_WRITE_BATCH_SIZE_BYTES = "network-write-batch-size-bytes";
  private static final String KEY_NETWORK_WRITE_BATCH_TIME_MS = "network-write-batch-time-ms";
  private static final String KEY_NETWORK_READ_BATCH_SIZE_BYTES = "network-read-batch-size-bytes";
  private static final String KEY_NETWORK_READ_BATCH_TIME_MS = "network-read-batch-time-ms";
  private static final String KEY_SOCKET_SEND_BUFFER_BYTES = "socket-send-buffer-size-bytes";
  private static final String KEY_SOCKET_RECEIVED_BUFFER_BYTES =
      "socket-received-buffer-size-bytes";
  private static final String KEY_TMASTER_METRICS_TYPE = "metricscache-metrics-type";

  // Bean name to fetch the MetricsCacheLocation object from SingletonRegistry
//  private static final String TMASTER_LOCATION_BEAN_NAME =
//      TopologyMaster.MetricsCacheLocation.newBuilder().getDescriptorForType().getFullName();
  // Metrics Counter Name
  private static final String METRICS_COUNT = "metrics-count";
  private static final String EXCEPTIONS_COUNT = "exceptions-count";
  private static final String RECORD_PROCESS_COUNT = "record-process-count";
  private static final String TMASTER_RESTART_COUNT = "tmaster-restart-count";
  private static final String TMASTER_LOCATION_UPDATE_COUNT = "tmaster-location-update-count";
  private final Communicator<TopologyMaster.PublishMetrics> metricsCommunicator =
      new Communicator<>();
  private final MetricsFilter tMasterMetricsFilter = new MetricsFilter();
  private final Map<String, Object> sinkConfig = new HashMap<>();
  // A scheduled executor service to check whether the MetricsCacheLocation has changed
  // If so, restart the metricsCacheClientService with the new MetricsCacheLocation
  // Start of metricsCacheClientService will also be in this thread
  private final ScheduledExecutorService tMasterLocationStarter =
      Executors.newSingleThreadScheduledExecutor();
  private MetricsCacheClientService metricsCacheClientService;
  // We need to cache it locally to check whether the MetricsCacheLocation is changed
  // This field is changed only in ScheduledExecutorService's thread,
  // so no need to make it volatile
  private TopologyMaster.MetricsCacheLocation currentMetricsCacheLocation = null;
  private SinkContext sinkContext;

  @Override
  @SuppressWarnings("unchecked")
  public void init(Map<String, Object> conf, SinkContext context) {
    LOG.info("metricscache sink init");
    sinkConfig.putAll(conf);

    sinkContext = context;

    // Fill the tMasterMetricsFilter according to metrics-sink-configs.yaml
    Map<String, String> tmasterMetricsType =
        (Map<String, String>) sinkConfig.get(KEY_TMASTER_METRICS_TYPE);
    if (tmasterMetricsType != null) {
      for (Map.Entry<String, String> metricToType : tmasterMetricsType.entrySet()) {
        String value = metricToType.getValue();
        MetricsFilter.MetricAggregationType type =
            MetricsFilter.MetricAggregationType.valueOf(value);
        tMasterMetricsFilter.setPrefixToType(metricToType.getKey(), type);
      }
    }

    // Construct the long-live metricsCacheClientService
    metricsCacheClientService =
        new MetricsCacheClientService((Map<String, Object>)
            sinkConfig.get(KEY_TMASTER), metricsCommunicator);

    // Start the tMasterLocationStarter
    startMetricsCacheChecker();
  }

  // Start the MetricsCacheCheck, which would check whether the MetricsCacheLocation is changed
  // at an interval.
  // If so, restart the metricsCacheClientService with the new MetricsCacheLocation
  private void startMetricsCacheChecker() {
    final int checkIntervalSec =
        TypeUtils.getInteger(sinkConfig.get(KEY_TMASTER_LOCATION_CHECK_INTERVAL_SEC));

    Runnable runnable = new Runnable() {
      @Override
      public void run() {
        TopologyMaster.MetricsCacheLocation location =
            (TopologyMaster.MetricsCacheLocation) SingletonRegistry.INSTANCE.getSingleton(
                MetricsManagerServer.METRICSCACHE_LOCATION_BEAN_NAME);

        if (location != null) {
          if (currentMetricsCacheLocation == null
              || !location.equals(currentMetricsCacheLocation)) {
            LOG.info("Update current MetricsCacheLocation to: " + location);
            currentMetricsCacheLocation = location;
            metricsCacheClientService.updateMetricsCacheLocation(currentMetricsCacheLocation);
            metricsCacheClientService.startNewMasterClient();

            // Update Metrics
            sinkContext.exportCountMetric(TMASTER_LOCATION_UPDATE_COUNT, 1);
          }
        }

        // Schedule itself in future
        tMasterLocationStarter.schedule(this, checkIntervalSec, TimeUnit.SECONDS);
      }
    };

    // First Entry
    tMasterLocationStarter.schedule(runnable, checkIntervalSec, TimeUnit.SECONDS);
  }

  @Override
  public void processRecord(MetricsRecord record) {
    LOG.info("metricscache sink processRecord");
    // Format it into TopologyMaster.PublishMetrics

    // The format of source is "host:port/componentName/instanceId"
    // So source.split("/") would be an array with 3 elements:
    // ["host:port", componentName, instanceId]
    String[] sources = record.getSource().split("/");
    String hostPort = sources[0];
    String componentName = sources[1];
    String instanceId = sources[2];

    TopologyMaster.PublishMetrics.Builder publishMetrics =
        TopologyMaster.PublishMetrics.newBuilder();

    for (MetricsInfo metricsInfo : tMasterMetricsFilter.filter(record.getMetrics())) {
      // We would filter out unneeded metrics
      TopologyMaster.MetricDatum metricDatum = TopologyMaster.MetricDatum.newBuilder().
          setComponentName(componentName).setInstanceId(instanceId).setName(metricsInfo.getName()).
          setValue(metricsInfo.getValue()).setTimestamp(record.getTimestamp()).build();
      publishMetrics.addMetrics(metricDatum);
    }

    for (ExceptionInfo exceptionInfo : record.getExceptions()) {
      TopologyMaster.TmasterExceptionLog exceptionLog =
          TopologyMaster.TmasterExceptionLog.newBuilder()
              .setComponentName(componentName)
              .setHostname(hostPort)
              .setInstanceId(instanceId)
              .setStacktrace(exceptionInfo.getStackTrace())
              .setLasttime(exceptionInfo.getLastTime())
              .setFirsttime(exceptionInfo.getFirstTime())
              .setCount(exceptionInfo.getCount())
              .setLogging(exceptionInfo.getLogging()).build();
      publishMetrics.addExceptions(exceptionLog);
    }

    metricsCommunicator.offer(publishMetrics.build());

    // Update metrics
    sinkContext.exportCountMetric(RECORD_PROCESS_COUNT, 1);
    sinkContext.exportCountMetric(METRICS_COUNT, publishMetrics.getMetricsCount());
    sinkContext.exportCountMetric(EXCEPTIONS_COUNT, publishMetrics.getExceptionsCount());
  }

  @Override
  public void flush() {
    // We do nothing here but update metrics
    sinkContext.exportCountMetric(TMASTER_RESTART_COUNT,
        metricsCacheClientService.startedAttempts.longValue());
  }

  @Override
  public void close() {
    metricsCacheClientService.close();
    metricsCommunicator.clear();
  }

  @VisibleForTesting
  MetricsCacheClientService getMetricsCacheClientService() {
    return metricsCacheClientService;
  }

  @VisibleForTesting
  void createSimpleMetricsCacheClientService(Map<String, Object> serviceConfig) {
    metricsCacheClientService =
        new MetricsCacheClientService(serviceConfig, metricsCommunicator);
  }

  @VisibleForTesting
  MetricsCacheClient getMetricsCacheClient() {
    return metricsCacheClientService.getMetricsCacheClient();
  }

  @VisibleForTesting
  void startNewMetricsCacheClient(TopologyMaster.MetricsCacheLocation location) {
    metricsCacheClientService.updateMetricsCacheLocation(location);
    metricsCacheClientService.startNewMasterClient();
  }

  @VisibleForTesting
  int getMetricsCacheStartedAttempts() {
    return metricsCacheClientService.startedAttempts.get();
  }

  @VisibleForTesting
  TopologyMaster.MetricsCacheLocation getCurrentMetricsCacheLocation() {
    return currentMetricsCacheLocation;
  }

  @VisibleForTesting
  TopologyMaster.MetricsCacheLocation getCurrentMetricsCacheLocationInService() {
    return metricsCacheClientService.getCurrentMetricsCacheLocation();
  }

  /**
   * A long-live Service running metricsCacheClient
   * It would automatically restart the metricsCacheClient connecting and communicating to the latest
   * MetricsCacheLocation if any uncaught exceptions throw.
   * <p>
   * It provides startNewMasterClient(TopologyMaster.MetricsCacheLocation location), which would also
   * update the currentMetricsCacheLocation to the lastest location.
   * <p>
   * So a new metricsCacheClient would start in two cases:
   * 1. The old one threw exceptions and died.
   * 2. startNewMasterClient() is invoked externally with MetricsCacheLocation.
   */
  private static final class MetricsCacheClientService {
    private final AtomicInteger startedAttempts = new AtomicInteger(0);
    private final Map<String, Object> metricsCacheClientConfig;
    private final Communicator<TopologyMaster.PublishMetrics> metricsCommunicator;
    private final ExecutorService metricsCacheClientExecutor =
        Executors.newSingleThreadExecutor(new MetricsCacheClientThreadFactory());
    private volatile MetricsCacheClient metricsCacheClient;
    // We need to cache MetricsCacheLocation for failover case
    // This value is set in ScheduledExecutorService' thread while
    // it is used in metricsCacheClientService thread,
    // so we need to make it volatile to guarantee the visiability.
    private volatile TopologyMaster.MetricsCacheLocation currentMetricsCacheLocation;

    private MetricsCacheClientService(
        Map<String, Object> metricsCacheClientConfig,
        Communicator<TopologyMaster.PublishMetrics> metricsCommunicator) {
      this.metricsCacheClientConfig = metricsCacheClientConfig;
      this.metricsCommunicator = metricsCommunicator;
    }

    // Update the MetricsCacheLocation to connect within the metricsCacheClient
    // This method is thread-safe, since
    // currentMetricsCacheLocation is volatile and we just replace it.
    // In our scenario, it is only invoked when MetricsCacheLocation is changed,
    // i.e. this method is only invoked in scheduled executor thread.
    public void updateMetricsCacheLocation(TopologyMaster.MetricsCacheLocation location) {
      currentMetricsCacheLocation = location;
    }

    // This method could be invoked by different threads
    // Make it synchronized to guarantee thread-safe
    public synchronized void startNewMasterClient() {

      // Exit any running metricsCacheClient if there is any to release
      // the thread in metricsCacheClientExecutor
      if (metricsCacheClient != null) {
        metricsCacheClient.stop();
        metricsCacheClient.getNIOLooper().exitLoop();
      }

      // Construct the new metricsCacheClient
      final NIOLooper looper;
      try {
        looper = new NIOLooper();
      } catch (IOException e) {
        throw new RuntimeException("Could not create the NIOLooper", e);
      }

      SystemConfig systemConfig =
          (SystemConfig) SingletonRegistry.INSTANCE.getSingleton(SystemConfig.HERON_SYSTEM_CONFIG);
      HeronSocketOptions socketOptions =
          new HeronSocketOptions(
              TypeUtils.getByteAmount(
                  metricsCacheClientConfig.get(KEY_NETWORK_WRITE_BATCH_SIZE_BYTES)),
              TypeUtils.getDuration(
                  metricsCacheClientConfig.get(KEY_NETWORK_WRITE_BATCH_TIME_MS), ChronoUnit.MILLIS),
              TypeUtils.getByteAmount(
                  metricsCacheClientConfig.get(KEY_NETWORK_READ_BATCH_SIZE_BYTES)),
              TypeUtils.getDuration(
                  metricsCacheClientConfig.get(KEY_NETWORK_READ_BATCH_TIME_MS), ChronoUnit.MILLIS),
              TypeUtils.getByteAmount(
                  metricsCacheClientConfig.get(KEY_SOCKET_SEND_BUFFER_BYTES)),
              TypeUtils.getByteAmount(
                  metricsCacheClientConfig.get(KEY_SOCKET_RECEIVED_BUFFER_BYTES)),
              systemConfig.getMetricsMgrNetworkOptionsMaximumPacketSize());

      // Reset the Consumer
      metricsCommunicator.setConsumer(looper);

      metricsCacheClient = new MetricsCacheClient(looper,
          currentMetricsCacheLocation.getHost(),
          currentMetricsCacheLocation.getMasterPort(),
          socketOptions, metricsCommunicator,
          TypeUtils.getDuration(
              metricsCacheClientConfig.get(KEY_TMASTER_RECONNECT_INTERVAL_SEC),
              ChronoUnit.SECONDS));

      LOG.severe(String.format("Starting metricsCacheClient for the %d time.",
          startedAttempts.incrementAndGet()));
      metricsCacheClientExecutor.execute(metricsCacheClient);
    }

    // This method could be invoked by different threads
    // Make it synchronized to guarantee thread-safe
    public synchronized void close() {
      metricsCacheClient.getNIOLooper().exitLoop();
      metricsCacheClientExecutor.shutdownNow();
    }

    @VisibleForTesting
    MetricsCacheClient getMetricsCacheClient() {
      return metricsCacheClient;
    }

    @VisibleForTesting
    int getMetricsCacheStartedAttempts() {
      return startedAttempts.get();
    }

    @VisibleForTesting
    TopologyMaster.MetricsCacheLocation getCurrentMetricsCacheLocation() {
      return currentMetricsCacheLocation;
    }

    // An UncaughtExceptionHandler, which would restart MetricsCacheLocation with
    // current MetricsCacheLocation.
    private class MetricsCacheClientThreadFactory implements ThreadFactory {
      @Override
      public Thread newThread(Runnable r) {
        final Thread thread = new Thread(r);
        thread.setUncaughtExceptionHandler(new MetricsCacheClientExceptionHandler());
        return thread;
      }

      private class MetricsCacheClientExceptionHandler implements Thread.UncaughtExceptionHandler {
        @Override
        public void uncaughtException(Thread t, Throwable e) {
          LOG.log(Level.SEVERE, "metricsCacheClient dies in thread: " + t, e);

          Duration reconnectInterval = TypeUtils.getDuration(
              metricsCacheClientConfig.get(KEY_TMASTER_RECONNECT_INTERVAL_SEC), ChronoUnit.SECONDS);
          SysUtils.sleep(reconnectInterval);
          LOG.info("Restarting metricsCacheClient");

          // We would use the MetricsCacheLocation in cache, since
          // the new metricsCacheClient is started due to exception thrown,
          // rather than MetricsCacheLocation changes
          startNewMasterClient();
        }
      }
    }
  }
}
