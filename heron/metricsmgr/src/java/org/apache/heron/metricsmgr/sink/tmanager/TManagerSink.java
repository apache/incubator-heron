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

import org.apache.heron.common.basics.Communicator;
import org.apache.heron.common.basics.NIOLooper;
import org.apache.heron.common.basics.SingletonRegistry;
import org.apache.heron.common.basics.SysUtils;
import org.apache.heron.common.basics.TypeUtils;
import org.apache.heron.common.config.SystemConfig;
import org.apache.heron.common.network.HeronSocketOptions;
import org.apache.heron.metricsmgr.MetricsUtil;
import org.apache.heron.proto.tmanager.TopologyManager;
import org.apache.heron.spi.metricsmgr.metrics.ExceptionInfo;
import org.apache.heron.spi.metricsmgr.metrics.MetricsFilter;
import org.apache.heron.spi.metricsmgr.metrics.MetricsInfo;
import org.apache.heron.spi.metricsmgr.metrics.MetricsRecord;
import org.apache.heron.spi.metricsmgr.sink.IMetricsSink;
import org.apache.heron.spi.metricsmgr.sink.SinkContext;

/**
 * An IMetricsSink sends Metrics to TManager.
 * 1. It gets the TManagerLocation
 * <p>
 * 2. Then it would construct a long-live Service running TManagerClient, which could automatically
 * recover from uncaught exceptions, i.e. close the old one and start a new one.
 * Also, it provides API to update the TManagerLocation that TManagerClient need to connect and
 * restart the TManagerClient.
 * There are two scenarios we need to restart a TManagerClient in our case:
 * <p>
 * -- Uncaught exceptions happen within TManagerClient; then we would restart TManagerClient inside
 * the same ExecutorService inside the UncaughtExceptionHandlers.
 * Notice that, in java, exceptions occur inside UncaughtExceptionHandlers would not invoke
 * UncaughtExceptionHandlers; instead, it would kill the thread with that exception.
 * So if exceptions thrown during restart a new TManagerClient, this TManagerSink would die, and
 * external logic would take care of it.
 * <p>
 * -- TManagerLocation changes (though in fact, TManagerClient might also throw exceptions in this case),
 * in this case, we would invoke TManagerService to start from tManagerLocationStarter's thread.
 * But the TManagerService and TManagerClient still start wihtin the thread they run.
 * <p>
 * 3. When a new MetricsRecord comes by invoking processRecord, it would push the MetricsRecord
 * to the Communicator Queue to TManagerClient
 * <p>
 * Notice that we would not send all metrics to TManager; we would use MetricsFilter to figure out
 * needed metrics.
 */

public class TManagerSink implements IMetricsSink {
  private static final Logger LOG = Logger.getLogger(TManagerSink.class.getName());

  private static final int MAX_COMMUNICATOR_SIZE = 128;

  // These configs would be read from metrics-sink-configs.yaml
  private static final String KEY_TMANAGER_LOCATION_CHECK_INTERVAL_SEC =
      "tmanager-location-check-interval-sec";
  private static final String KEY_TMANAGER = "tmanager-client";
  private static final String KEY_TMANAGER_RECONNECT_INTERVAL_SEC = "reconnect-interval-second";
  private static final String KEY_NETWORK_WRITE_BATCH_SIZE_BYTES = "network-write-batch-size-bytes";
  private static final String KEY_NETWORK_WRITE_BATCH_TIME_MS = "network-write-batch-time-ms";
  private static final String KEY_NETWORK_READ_BATCH_SIZE_BYTES = "network-read-batch-size-bytes";
  private static final String KEY_NETWORK_READ_BATCH_TIME_MS = "network-read-batch-time-ms";
  private static final String KEY_SOCKET_SEND_BUFFER_BYTES = "socket-send-buffer-size-bytes";
  private static final String KEY_SOCKET_RECEIVED_BUFFER_BYTES =
      "socket-received-buffer-size-bytes";
  private static final String KEY_TMANAGER_METRICS_TYPE = "tmanager-metrics-type";

  // Bean name to fetch the TManagerLocation object from SingletonRegistry
  private static final String TMANAGER_LOCATION_BEAN_NAME =
      TopologyManager.TManagerLocation.newBuilder().getDescriptorForType().getFullName();
  // Metrics Counter Name
  private static final String METRICS_COUNT = "metrics-count";
  private static final String EXCEPTIONS_COUNT = "exceptions-count";
  private static final String RECORD_PROCESS_COUNT = "record-process-count";
  private static final String TMANAGER_RESTART_COUNT = "tmanager-restart-count";
  private static final String TMANAGER_LOCATION_UPDATE_COUNT = "tmanager-location-update-count";
  private final Communicator<TopologyManager.PublishMetrics> metricsCommunicator =
      new Communicator<>();
  private final MetricsFilter tManagerMetricsFilter = new MetricsFilter();
  private final Map<String, Object> sinkConfig = new HashMap<>();
  // A scheduled executor service to check whether the TManagerLocation has changed
  // If so, restart the TManagerClientService with the new TManagerLocation
  // Start of TManagerClientService will also be in this thread
  private final ScheduledExecutorService tManagerLocationStarter =
      Executors.newSingleThreadScheduledExecutor();
  private TManagerClientService tManagerClientService;
  // We need to cache it locally to check whether the TManagerLocation is changed
  // This field is changed only in ScheduledExecutorService's thread,
  // so no need to make it volatile
  private TopologyManager.TManagerLocation currentTManagerLocation = null;
  private SinkContext sinkContext;

  @Override
  @SuppressWarnings("unchecked")
  public void init(Map<String, Object> conf, SinkContext context) {
    sinkConfig.putAll(conf);

    sinkContext = context;

    // Fill the tManagerMetricsFilter according to metrics-sink-configs.yaml
    Map<String, String> tmanagerMetricsType =
        (Map<String, String>) sinkConfig.get(KEY_TMANAGER_METRICS_TYPE);
    if (tmanagerMetricsType != null) {
      for (Map.Entry<String, String> metricToType : tmanagerMetricsType.entrySet()) {
        String value = metricToType.getValue();
        MetricsFilter.MetricAggregationType type;
        if ("SUM".equals(value)) {
          type = MetricsFilter.MetricAggregationType.SUM;
        } else if ("AVG".equals(value)) {
          type = MetricsFilter.MetricAggregationType.AVG;
        } else if ("LAST".equals(value)) {
          type = MetricsFilter.MetricAggregationType.LAST;
        } else {
          type = MetricsFilter.MetricAggregationType.UNKNOWN;
        }
        tManagerMetricsFilter.setPrefixToType(metricToType.getKey(), type);
      }
    }

    // Construct the long-live TManagerClientService
    tManagerClientService =
        new TManagerClientService((Map<String, Object>)
            sinkConfig.get(KEY_TMANAGER), metricsCommunicator);

    // Start the tManagerLocationStarter
    startTManagerChecker();
  }

  // Start the TManagerCheck, which would check whether the TManagerLocation is changed
  // at an interval.
  // If so, restart the TManagerClientService with the new TManagerLocation
  private void startTManagerChecker() {
    final int checkIntervalSec =
        TypeUtils.getInteger(sinkConfig.get(KEY_TMANAGER_LOCATION_CHECK_INTERVAL_SEC));

    Runnable runnable = new Runnable() {
      @Override
      public void run() {
        TopologyManager.TManagerLocation location =
            (TopologyManager.TManagerLocation) SingletonRegistry.INSTANCE.getSingleton(
                 TMANAGER_LOCATION_BEAN_NAME);

        if (location != null) {
          if (currentTManagerLocation == null || !location.equals(currentTManagerLocation)) {
            LOG.info("Update current TManagerLocation to: " + location);
            currentTManagerLocation = location;
            tManagerClientService.updateTManagerLocation(currentTManagerLocation);
            tManagerClientService.startNewPrimaryClient();

            // Update Metrics
            sinkContext.exportCountMetric(TMANAGER_LOCATION_UPDATE_COUNT, 1);
          }
        }

        // Schedule itself in future
        tManagerLocationStarter.schedule(this, checkIntervalSec, TimeUnit.SECONDS);
      }
    };

    // First Entry
    tManagerLocationStarter.schedule(runnable, checkIntervalSec, TimeUnit.SECONDS);
    LOG.info("TManagerChecker started with interval: " + checkIntervalSec);
  }

  @Override
  public void processRecord(MetricsRecord record) {
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
      TopologyManager.TmanagerExceptionLog exceptionLog =
          TopologyManager.TmanagerExceptionLog.newBuilder()
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
    sinkContext.exportCountMetric(TMANAGER_RESTART_COUNT,
        tManagerClientService.startedAttempts.longValue());
  }

  @Override
  public void close() {
    tManagerClientService.close();
    metricsCommunicator.clear();
  }

  /////////////////////////////////////////////////////////
  // Following protected methods should be used only for unit testing
  /////////////////////////////////////////////////////////
  protected TManagerClientService getTManagerClientService() {
    return tManagerClientService;
  }

  protected void createSimpleTManagerClientService(Map<String, Object> serviceConfig) {
    tManagerClientService =
        new TManagerClientService(serviceConfig, metricsCommunicator);
  }

  protected TManagerClient getTManagerClient() {
    return tManagerClientService.getTManagerClient();
  }

  protected void startNewTManagerClient(TopologyManager.TManagerLocation location) {
    tManagerClientService.updateTManagerLocation(location);
    tManagerClientService.startNewPrimaryClient();
  }

  protected int getTManagerStartedAttempts() {
    return tManagerClientService.startedAttempts.get();
  }

  protected TopologyManager.TManagerLocation getCurrentTManagerLocation() {
    return currentTManagerLocation;
  }

  protected TopologyManager.TManagerLocation getCurrentTManagerLocationInService() {
    return tManagerClientService.getCurrentTManagerLocation();
  }

  /**
   * A long-live Service running TManagerClient
   * It would automatically restart the TManagerClient connecting and communicating to the latest
   * TManagerLocation if any uncaught exceptions throw.
   * <p>
   * It provides startNewPrimaryClient(TopologyManager.TManagerLocation location), which would also
   * update the currentTManagerLocation to the lastest location.
   * <p>
   * So a new TManagerClient would start in two cases:
   * 1. The old one threw exceptions and died.
   * 2. startNewPrimaryClient() is invoked externally with TManagerLocation.
   */
  private static final class TManagerClientService {
    private final AtomicInteger startedAttempts = new AtomicInteger(0);
    private final Map<String, Object> tmanagerClientConfig;
    private final Communicator<TopologyManager.PublishMetrics> metricsCommunicator;
    private final ExecutorService tmanagerClientExecutor =
        Executors.newSingleThreadExecutor(new TManagerClientThreadFactory());
    private volatile TManagerClient tManagerClient;
    // We need to cache TManagerLocation for failover case
    // This value is set in ScheduledExecutorService' thread while
    // it is used in TManagerClientService thread,
    // so we need to make it volatile to guarantee the visiability.
    private volatile TopologyManager.TManagerLocation currentTManagerLocation;

    private TManagerClientService(Map<String, Object> tmanagerClientConfig,
                                 Communicator<TopologyManager.PublishMetrics> metricsCommunicator) {
      this.tmanagerClientConfig = tmanagerClientConfig;
      this.metricsCommunicator = metricsCommunicator;
    }

    // Update the TManagerLocation to connect within the TManagerClient
    // This method is thread-safe, since
    // currentTManagerLocation is volatile and we just replace it.
    // In our scenario, it is only invoked when TManagerLocation is changed,
    // i.e. this method is only invoked in scheduled executor thread.
    public void updateTManagerLocation(TopologyManager.TManagerLocation location) {
      currentTManagerLocation = location;
    }

    // This method could be invoked by different threads
    // Make it synchronized to guarantee thread-safe
    public synchronized void startNewPrimaryClient() {

      // Exit any running tManagerClient if there is any to release
      // the thread in tmanagerClientExecutor
      if (tManagerClient != null) {
        tManagerClient.stop();
        tManagerClient.getNIOLooper().exitLoop();
      }

      // Construct the new TManagerClient
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
              TypeUtils.getByteAmount(tmanagerClientConfig.get(KEY_NETWORK_WRITE_BATCH_SIZE_BYTES)),
              TypeUtils.getDuration(
                  tmanagerClientConfig.get(KEY_NETWORK_WRITE_BATCH_TIME_MS), ChronoUnit.MILLIS),
              TypeUtils.getByteAmount(tmanagerClientConfig.get(KEY_NETWORK_READ_BATCH_SIZE_BYTES)),
              TypeUtils.getDuration(
                  tmanagerClientConfig.get(KEY_NETWORK_READ_BATCH_TIME_MS), ChronoUnit.MILLIS),
              TypeUtils.getByteAmount(tmanagerClientConfig.get(KEY_SOCKET_SEND_BUFFER_BYTES)),
              TypeUtils.getByteAmount(tmanagerClientConfig.get(KEY_SOCKET_RECEIVED_BUFFER_BYTES)),
              systemConfig.getMetricsMgrNetworkOptionsMaximumPacketSize());

      // Reset the Consumer
      metricsCommunicator.setConsumer(looper);

      tManagerClient =
          new TManagerClient(looper,
              currentTManagerLocation.getHost(),
              currentTManagerLocation.getServerPort(),
              socketOptions, metricsCommunicator,
              TypeUtils.getDuration(
                  tmanagerClientConfig.get(KEY_TMANAGER_RECONNECT_INTERVAL_SEC),
                  ChronoUnit.SECONDS));

      int attempts = startedAttempts.incrementAndGet();
      LOG.severe(String.format("Starting TManagerClient for the %d time.", attempts));
      tmanagerClientExecutor.execute(tManagerClient);
    }

    // This method could be invoked by different threads
    // Make it synchronized to guarantee thread-safe
    public synchronized void close() {
      tManagerClient.getNIOLooper().exitLoop();
      tmanagerClientExecutor.shutdownNow();
    }

    /////////////////////////////////////////////////////////
    // Following protected methods should be used only for unit testing
    /////////////////////////////////////////////////////////
    protected TManagerClient getTManagerClient() {
      return tManagerClient;
    }

    protected int getTManagerStartedAttempts() {
      return startedAttempts.get();
    }

    protected TopologyManager.TManagerLocation getCurrentTManagerLocation() {
      return currentTManagerLocation;
    }

    // An UncaughtExceptionHandler, which would restart TManagerLocation with
    // current TManagerLocation.
    private class TManagerClientThreadFactory implements ThreadFactory {
      @Override
      public Thread newThread(Runnable r) {
        final Thread thread = new Thread(r);
        thread.setUncaughtExceptionHandler(new TManagerClientExceptionHandler());
        return thread;
      }

      private class TManagerClientExceptionHandler implements Thread.UncaughtExceptionHandler {
        @Override
        public void uncaughtException(Thread t, Throwable e) {
          LOG.log(Level.SEVERE, "TManagerClient dies in thread: " + t, e);

          Duration reconnectInterval = TypeUtils.getDuration(
              tmanagerClientConfig.get(KEY_TMANAGER_RECONNECT_INTERVAL_SEC), ChronoUnit.SECONDS);
          SysUtils.sleep(reconnectInterval);
          LOG.info("Restarting TManagerClient");

          // We would use the TManagerLocation in cache, since
          // the new TManagerClient is started due to exception thrown,
          // rather than TManagerLocation changes
          startNewPrimaryClient();
        }
      }
    }
  }
}
