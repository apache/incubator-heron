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

package org.apache.heron.metricsmgr.sink.metricscache;

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

import org.apache.heron.common.basics.Communicator;
import org.apache.heron.common.basics.NIOLooper;
import org.apache.heron.common.basics.SingletonRegistry;
import org.apache.heron.common.basics.SysUtils;
import org.apache.heron.common.basics.TypeUtils;
import org.apache.heron.common.config.SystemConfig;
import org.apache.heron.common.network.HeronSocketOptions;
import org.apache.heron.metricsmgr.MetricsManagerServer;
import org.apache.heron.metricsmgr.MetricsUtil;
import org.apache.heron.proto.tmanager.TopologyManager;
import org.apache.heron.spi.metricsmgr.metrics.ExceptionInfo;
import org.apache.heron.spi.metricsmgr.metrics.MetricsFilter;
import org.apache.heron.spi.metricsmgr.metrics.MetricsInfo;
import org.apache.heron.spi.metricsmgr.metrics.MetricsRecord;
import org.apache.heron.spi.metricsmgr.sink.IMetricsSink;
import org.apache.heron.spi.metricsmgr.sink.SinkContext;

/**
 * An IMetricsSink sends Metrics to MetricsCache.
 * 1. It gets the MetricsCacheLocation
 * <p>
 * 2. Then it would construct a long-live Service running metricsCacheClient, which could automatically
 * recover from uncaught exceptions, i.e. close the old one and start a new one.
 * Also, it provides API to update the MetricsCacheLocation that metricsCacheClient need to connect and
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
 * in this case, we would invoke MetricsCacheService to start from tManagerLocationStarter's thread.
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

  private static final int MAX_COMMUNICATOR_SIZE = 128;

  // These configs would be read from metrics-sink-configs.yaml
  private static final String KEY_TMANAGER_LOCATION_CHECK_INTERVAL_SEC =
      "metricscache-location-check-interval-sec";
  private static final String KEY_TMANAGER = "metricscache-client";
  private static final String KEY_TMANAGER_RECONNECT_INTERVAL_SEC = "reconnect-interval-second";
  private static final String KEY_NETWORK_WRITE_BATCH_SIZE_BYTES = "network-write-batch-size-bytes";
  private static final String KEY_NETWORK_WRITE_BATCH_TIME_MS = "network-write-batch-time-ms";
  private static final String KEY_NETWORK_READ_BATCH_SIZE_BYTES = "network-read-batch-size-bytes";
  private static final String KEY_NETWORK_READ_BATCH_TIME_MS = "network-read-batch-time-ms";
  private static final String KEY_SOCKET_SEND_BUFFER_BYTES = "socket-send-buffer-size-bytes";
  private static final String KEY_SOCKET_RECEIVED_BUFFER_BYTES =
      "socket-received-buffer-size-bytes";
  private static final String KEY_TMANAGER_METRICS_TYPE = "metricscache-metrics-type";

  // Bean name to fetch the MetricsCacheLocation object from SingletonRegistry
//  private static final String TMANAGER_LOCATION_BEAN_NAME =
//      TopologyManager.MetricsCacheLocation.newBuilder().getDescriptorForType().getFullName();
  // Metrics Counter Name
  private static final String METRICS_COUNT = "metrics-count";
  private static final String EXCEPTIONS_COUNT = "exceptions-count";
  private static final String RECORD_PROCESS_COUNT = "record-process-count";
  private static final String METRICSMGR_RESTART_COUNT = "metricsmgr-restart-count";
  private static final String METRICSMGR_LOCATION_UPDATE_COUNT = "metricsmgr-location-update-count";
  private final Communicator<TopologyManager.PublishMetrics> metricsCommunicator =
      new Communicator<>();
  private final MetricsFilter tManagerMetricsFilter = new MetricsFilter();
  private final Map<String, Object> sinkConfig = new HashMap<>();
  // A scheduled executor service to check whether the MetricsCacheLocation has changed
  // If so, restart the metricsCacheClientService with the new MetricsCacheLocation
  // Start of metricsCacheClientService will also be in this thread
  private final ScheduledExecutorService tManagerLocationStarter =
      Executors.newSingleThreadScheduledExecutor();
  private MetricsCacheClientService metricsCacheClientService;
  // We need to cache it locally to check whether the MetricsCacheLocation is changed
  // This field is changed only in ScheduledExecutorService's thread,
  // so no need to make it volatile
  private TopologyManager.MetricsCacheLocation currentMetricsCacheLocation = null;
  private SinkContext sinkContext;

  @Override
  @SuppressWarnings("unchecked")
  public void init(Map<String, Object> conf, SinkContext context) {
    LOG.info("metricscache sink init");
    sinkConfig.putAll(conf);

    sinkContext = context;

    // Fill the tManagerMetricsFilter according to metrics-sink-configs.yaml
    Map<String, String> tmanagerMetricsType =
        (Map<String, String>) sinkConfig.get(KEY_TMANAGER_METRICS_TYPE);
    if (tmanagerMetricsType != null) {
      for (Map.Entry<String, String> metricToType : tmanagerMetricsType.entrySet()) {
        String value = metricToType.getValue();
        MetricsFilter.MetricAggregationType type =
            MetricsFilter.MetricAggregationType.valueOf(value);
        tManagerMetricsFilter.setPrefixToType(metricToType.getKey(), type);
      }
    }

    // Construct the long-live metricsCacheClientService
    metricsCacheClientService =
        new MetricsCacheClientService((Map<String, Object>)
            sinkConfig.get(KEY_TMANAGER), metricsCommunicator);

    // Start the tManagerLocationStarter
    startMetricsCacheChecker();
  }

  // Start the MetricsCacheCheck, which would check whether the MetricsCacheLocation is changed
  // at an interval.
  // If so, restart the metricsCacheClientService with the new MetricsCacheLocation
  private void startMetricsCacheChecker() {
    final int checkIntervalSec =
        TypeUtils.getInteger(sinkConfig.get(KEY_TMANAGER_LOCATION_CHECK_INTERVAL_SEC));

    Runnable runnable = new Runnable() {
      @Override
      public void run() {
        TopologyManager.MetricsCacheLocation location =
            (TopologyManager.MetricsCacheLocation) SingletonRegistry.INSTANCE.getSingleton(
                MetricsManagerServer.METRICSCACHE_LOCATION_BEAN_NAME);

        if (location != null) {
          if (currentMetricsCacheLocation == null
              || !location.equals(currentMetricsCacheLocation)) {
            LOG.info("Update current MetricsCacheLocation to: " + location);
            currentMetricsCacheLocation = location;
            metricsCacheClientService.updateMetricsCacheLocation(currentMetricsCacheLocation);
            metricsCacheClientService.startNewPrimaryClient();

            // Update Metrics
            sinkContext.exportCountMetric(METRICSMGR_LOCATION_UPDATE_COUNT, 1);
          }
        }

        // Schedule itself in future
        tManagerLocationStarter.schedule(this, checkIntervalSec, TimeUnit.SECONDS);
      }
    };

    // First Entry
    tManagerLocationStarter.schedule(runnable, checkIntervalSec, TimeUnit.SECONDS);
    LOG.info("MetricsCacheChecker started with interval: " + checkIntervalSec);
  }

  @Override
  public void processRecord(MetricsRecord record) {
    LOG.info("metricscache sink processRecord");
    // Format it into TopologyManager.PublishMetrics

    // The format of record is "host:port/componentName/instanceId"
    // So MetricsRecord.getSource().split("/") would be an array with 3 elements:
    // ["host:port", componentName, instanceId]
    String[] sources = MetricsUtil.splitRecordSource(record);
    String hostPort = sources[0];
    String componentName = sources[1];
    String instanceId = sources[2];

    TopologyManager.PublishMetrics.Builder publishMetrics =
        TopologyManager.PublishMetrics.newBuilder();

    for (MetricsInfo metricsInfo : tManagerMetricsFilter.filter(record.getMetrics())) {
      // We would filter out unneeded metrics
      TopologyManager.MetricDatum metricDatum = TopologyManager.MetricDatum.newBuilder().
          setComponentName(componentName).setInstanceId(instanceId).setName(metricsInfo.getName()).
          setValue(metricsInfo.getValue()).setTimestamp(record.getTimestamp()).build();
      publishMetrics.addMetrics(metricDatum);
    }

    for (ExceptionInfo exceptionInfo : record.getExceptions()) {
      String exceptionStackTrace = exceptionInfo.getStackTrace();
      String[] exceptionStackTraceLines = exceptionStackTrace.split("\r\n|[\r\n]", 3);
      String exceptionStackTraceFirstTwoLines = String.join(System.lineSeparator(),
          exceptionStackTraceLines[0], exceptionStackTraceLines[1]);
      TopologyManager.TmanagerExceptionLog exceptionLog =
          TopologyManager.TmanagerExceptionLog.newBuilder()
              .setComponentName(componentName)
              .setHostname(hostPort)
              .setInstanceId(instanceId)
              .setStacktrace(exceptionStackTraceFirstTwoLines)
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

    checkCommunicator(metricsCommunicator, MAX_COMMUNICATOR_SIZE);
  }

  // Check if the communicator is full/overflow. Poll and drop extra elements that
  // are over the queue limit from the head.
  public static void checkCommunicator(Communicator<TopologyManager.PublishMetrics> communicator,
                                        int maxSize) {
    synchronized (communicator) {
      int size = communicator.size();

      for (int i = 0; i < size - maxSize; ++i) {
        communicator.poll();
      }
    }
  }

  @Override
  public void flush() {
    // We do nothing here but update metrics
    sinkContext.exportCountMetric(METRICSMGR_RESTART_COUNT,
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
  void startNewMetricsCacheClient(TopologyManager.MetricsCacheLocation location) {
    metricsCacheClientService.updateMetricsCacheLocation(location);
    metricsCacheClientService.startNewPrimaryClient();
  }

  @VisibleForTesting
  int getMetricsCacheStartedAttempts() {
    return metricsCacheClientService.startedAttempts.get();
  }

  @VisibleForTesting
  TopologyManager.MetricsCacheLocation getCurrentMetricsCacheLocation() {
    return currentMetricsCacheLocation;
  }

  @VisibleForTesting
  TopologyManager.MetricsCacheLocation getCurrentMetricsCacheLocationInService() {
    return metricsCacheClientService.getCurrentMetricsCacheLocation();
  }

  /**
   * A long-live Service running metricsCacheClient
   * It would automatically restart the metricsCacheClient connecting and communicating to the latest
   * MetricsCacheLocation if any uncaught exceptions throw.
   * <p>
   * It provides startNewPrimaryClient(TopologyManager.MetricsCacheLocation location), which would also
   * update the currentMetricsCacheLocation to the lastest location.
   * <p>
   * So a new metricsCacheClient would start in two cases:
   * 1. The old one threw exceptions and died.
   * 2. startNewPrimaryClient() is invoked externally with MetricsCacheLocation.
   */
  private static final class MetricsCacheClientService {
    private final AtomicInteger startedAttempts = new AtomicInteger(0);
    private final Map<String, Object> metricsCacheClientConfig;
    private final Communicator<TopologyManager.PublishMetrics> metricsCommunicator;
    private final ExecutorService metricsCacheClientExecutor =
        Executors.newSingleThreadExecutor(new MetricsCacheClientThreadFactory());
    private volatile MetricsCacheClient metricsCacheClient;
    // We need to cache MetricsCacheLocation for failover case
    // This value is set in ScheduledExecutorService' thread while
    // it is used in metricsCacheClientService thread,
    // so we need to make it volatile to guarantee the visiability.
    private volatile TopologyManager.MetricsCacheLocation currentMetricsCacheLocation;

    private MetricsCacheClientService(
        Map<String, Object> metricsCacheClientConfig,
        Communicator<TopologyManager.PublishMetrics> metricsCommunicator) {
      this.metricsCacheClientConfig = metricsCacheClientConfig;
      this.metricsCommunicator = metricsCommunicator;
    }

    // Update the MetricsCacheLocation to connect within the metricsCacheClient
    // This method is thread-safe, since
    // currentMetricsCacheLocation is volatile and we just replace it.
    // In our scenario, it is only invoked when MetricsCacheLocation is changed,
    // i.e. this method is only invoked in scheduled executor thread.
    public void updateMetricsCacheLocation(TopologyManager.MetricsCacheLocation location) {
      currentMetricsCacheLocation = location;
    }

    // This method could be invoked by different threads
    // Make it synchronized to guarantee thread-safe
    public synchronized void startNewPrimaryClient() {

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
          currentMetricsCacheLocation.getServerPort(),
          socketOptions, metricsCommunicator,
          TypeUtils.getDuration(
              metricsCacheClientConfig.get(KEY_TMANAGER_RECONNECT_INTERVAL_SEC),
              ChronoUnit.SECONDS));

      int attempts = startedAttempts.incrementAndGet();
      LOG.severe(String.format("Starting metricsCacheClient for the %d time.", attempts));
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
    TopologyManager.MetricsCacheLocation getCurrentMetricsCacheLocation() {
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
              metricsCacheClientConfig.get(KEY_TMANAGER_RECONNECT_INTERVAL_SEC),
              ChronoUnit.SECONDS);
          SysUtils.sleep(reconnectInterval);
          LOG.info("Restarting metricsCacheClient");

          // We would use the MetricsCacheLocation in cache, since
          // the new metricsCacheClient is started due to exception thrown,
          // rather than MetricsCacheLocation changes
          startNewPrimaryClient();
        }
      }
    }
  }
}
