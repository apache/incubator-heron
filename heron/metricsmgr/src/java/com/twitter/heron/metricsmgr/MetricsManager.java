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

package com.twitter.heron.metricsmgr;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.twitter.heron.api.metric.MultiCountMetric;
import com.twitter.heron.common.basics.Communicator;
import com.twitter.heron.common.basics.Constants;
import com.twitter.heron.common.basics.NIOLooper;
import com.twitter.heron.common.basics.SingletonRegistry;
import com.twitter.heron.common.basics.SlaveLooper;
import com.twitter.heron.common.basics.SysUtils;
import com.twitter.heron.common.basics.TypeUtils;
import com.twitter.heron.common.config.SystemConfig;
import com.twitter.heron.common.network.HeronSocketOptions;
import com.twitter.heron.common.utils.logging.ErrorReportLoggingHandler;
import com.twitter.heron.common.utils.logging.LoggingHelper;
import com.twitter.heron.common.utils.metrics.JVMMetrics;
import com.twitter.heron.common.utils.metrics.MetricsCollector;
import com.twitter.heron.metricsmgr.executor.SinkExecutor;
import com.twitter.heron.metricsmgr.sink.SinkContextImpl;
import com.twitter.heron.proto.system.Metrics;
import com.twitter.heron.spi.metricsmgr.metrics.MetricsRecord;
import com.twitter.heron.spi.metricsmgr.sink.IMetricsSink;
import com.twitter.heron.spi.metricsmgr.sink.SinkContext;

/**
 * Main entry to start the Metrics Manager
 * 1. It would read and parse the sink-configs, and then instantiate new IMetricsSink
 * instance by reflection.
 * <p>
 * 2. Every IMetricsSink would be driven by an individual thread; and in fact it is
 * an executor running within a fixed-size thread's ExecutorService.
 * We would set the thread name of running executor driving the IMetricsSink, for restart when
 * uncaught exceptions of that IMetricsSink are caught.
 * <p>
 * 3. When one IMetricsSink throws uncaught exceptions, we would try to restart this sink unless
 * we have hit the # of retry attempts.
 * <p>
 * 4. When Metrics Manager internal exceptions are caught or we have restart IMetricsSink with
 * too many attempts, Metrics Manager would flush any remain log and exit.
 */
public class MetricsManager {
  private static final Logger LOG = Logger.getLogger(MetricsManager.class.getName());

  // Pre-defined value
  private static final String METRICS_MANAGER_HOST = "127.0.0.1";
  private static final String METRICS_MANAGER_COMPONENT_NAME = "__metricsmgr__";
  private static final int METRICS_MANAGER_INSTANCE_ID = -1;

  private final MetricsSinksConfig config;

  private final ExecutorService executors;

  // A ConcurrentHashMap from sinkId to SinkExecutor
  private final Map<String, SinkExecutor> sinkExecutors;

  private final MetricsManagerServer metricsManagerServer;

  // # of attempts for different IMetricsSink to restart when failure happens
  private final Map<String, Integer> sinksRetryAttempts;

  // The looper drives MetricsManagerServer
  private final NIOLooper metricsManagerServerLoop;

  private final JVMMetrics jvmMetrics;

  // MetricsCollector used to collect internal metrics of Metrics Manager
  private final MetricsCollector metricsCollector;
  // Communicator to be bind with MetricsCollector to collect metrics
  private final Communicator<Metrics.MetricPublisherPublishMessage> metricsQueue;
  private final Metrics.MetricPublisher metricsManagerPublisher;

  private final int heronMetricsExportIntervalSec;
  private final String topologyName;
  private final String metricsmgrId;

  private final long mainThreadId;

  /**
   * Metrics manager constructor
   */
  public MetricsManager(String topologyName, String serverHost,
                        int serverPort, String metricsmgrId,
                        SystemConfig systemConfig, MetricsSinksConfig config)
      throws IOException {
    this.topologyName = topologyName;
    this.metricsmgrId = metricsmgrId;
    this.config = config;
    this.metricsManagerServerLoop = new NIOLooper();

    // Init the Internal Metrics Export related stuff
    this.metricsManagerPublisher =
        Metrics.MetricPublisher.newBuilder().
            setHostname(getLocalHostName()).setPort(serverPort).
            setComponentName(METRICS_MANAGER_COMPONENT_NAME).
            setInstanceId(metricsmgrId).
            setInstanceIndex(METRICS_MANAGER_INSTANCE_ID).build();
    this.jvmMetrics = new JVMMetrics();
    this.metricsQueue =
        new Communicator<Metrics.MetricPublisherPublishMessage>(null,
            this.metricsManagerServerLoop);
    this.metricsCollector = new MetricsCollector(metricsManagerServerLoop, metricsQueue);
    this.heronMetricsExportIntervalSec = systemConfig.getHeronMetricsExportIntervalSec();

    this.mainThreadId = Thread.currentThread().getId();

    // Init the ErrorReportHandler
    ErrorReportLoggingHandler.init(
        metricsmgrId, metricsCollector, heronMetricsExportIntervalSec,
        systemConfig.getHeronMetricsMaxExceptionsPerMessageCount());

    // Set up the internal Metrics Export routine
    setupInternalMetricsExport();

    // Set up JVM metrics
    // TODO -- change the config name
    setupJVMMetrics(systemConfig.getInstanceMetricsSystemSampleIntervalSec());

    // Init the HeronSocketOptions
    HeronSocketOptions serverSocketOptions =
        new HeronSocketOptions(systemConfig.getMetricsMgrNetworkWriteBatchSizeBytes(),
            systemConfig.getMetricsMgrNetworkWriteBatchTimeMs(),
            systemConfig.getMetricsMgrNetworkReadBatchSizeBytes(),
            systemConfig.getMetricsMgrNetworkReadBatchTimeMs(),
            systemConfig.getMetricsMgrNetworkOptionsSocketSendBufferSizeBytes(),
            systemConfig.getMetricsMgrNetworkOptionsSocketReceivedBufferSizeBytes());

    // Set the MultiCountMetric for MetricsManagerServer
    MultiCountMetric serverCounters = new MultiCountMetric();
    metricsCollector.registerMetric(
        METRICS_MANAGER_COMPONENT_NAME, serverCounters, heronMetricsExportIntervalSec);

    // Construct the MetricsManagerServer
    metricsManagerServer = new MetricsManagerServer(metricsManagerServerLoop, serverHost,
        serverPort, serverSocketOptions, serverCounters);

    executors = Executors.newFixedThreadPool(config.getNumberOfSinks());
    sinkExecutors = new ConcurrentHashMap<>(config.getNumberOfSinks());
    sinksRetryAttempts = new ConcurrentHashMap<>(config.getNumberOfSinks());
    // Add exception handler for any uncaught exception here.
    Thread.setDefaultUncaughtExceptionHandler(new DefaultExceptionHandler());

    // Init the Sinks
    for (String sinkId : config.getSinkIds()) {
      // Instantiate a new instance by using reflection
      SinkExecutor sinkExecutor = initSinkExecutor(sinkId);

      // Update the SinkExecutor in sinkExecutors
      sinkExecutors.put(sinkId, sinkExecutor);

      // Set the retry attempts
      Object restartAttempts = config.getConfigForSink(sinkId).
          get(MetricsSinksConfig.CONFIG_KEY_SINK_RESTART_ATTEMPTS);
      // Supply with default value is config is null
      sinksRetryAttempts.put(sinkId,
          restartAttempts == null
              ? MetricsSinksConfig.DEFAULT_SINK_RESTART_ATTEMPTS
              : TypeUtils.getInteger(restartAttempts));

      // Update the list of Communicator in Metrics Manager Server
      metricsManagerServer.addSinkCommunicator(sinkExecutor.getCommunicator());
    }
  }

  private static String getLocalHostName() {
    String hostName;
    try {
      hostName = InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException e) {
      LOG.severe("Unknown host.");
      hostName = METRICS_MANAGER_HOST;
    }

    return hostName;
  }

  public static void main(String[] args) throws IOException {
    if (args.length != 6) {
      throw new RuntimeException(
          "Invalid arguments; Usage: java com.twitter.heron.metricsmgr.MetricsManager "
              + "<id> <port> <topname> <topid> <heron_internals_config_filename> "
              + "<metrics_sinks_config_filename>");
    }

    String metricsmgrId = args[0];
    int metricsPort = Integer.parseInt(args[1]);
    String topologyName = args[2];
    String topologyId = args[3];
    String systemConfigFilename = args[4];
    String metricsSinksConfigFilename = args[5];

    SystemConfig systemConfig = new SystemConfig(systemConfigFilename, true);
    // Add the SystemConfig into SingletonRegistry
    SingletonRegistry.INSTANCE.registerSingleton(SystemConfig.HERON_SYSTEM_CONFIG, systemConfig);

    // Init the logging setting and redirect the stdout and stderr to logging
    // For now we just set the logging level as INFO; later we may accept an argument to set it.
    Level loggingLevel = Level.INFO;
    String loggingDir = systemConfig.getHeronLoggingDirectory();

    // Log to file and TMaster
    LoggingHelper.loggerInit(loggingLevel, true);
    LoggingHelper.addLoggingHandler(
        LoggingHelper.getFileHandler(metricsmgrId, loggingDir, true,
            systemConfig.getHeronLoggingMaximumSizeMb() * Constants.MB_TO_BYTES,
            systemConfig.getHeronLoggingMaximumFiles()));
    LoggingHelper.addLoggingHandler(new ErrorReportLoggingHandler());

    LOG.info(String.format("Starting Metrics Manager for topology %s with topologyId %s with "
            + "Metrics Manager Id %s, Merics Manager Port: %d.",
        topologyName, topologyId, metricsmgrId, metricsPort));

    LOG.info("System Config: " + systemConfig);

    // Populate the config
    MetricsSinksConfig sinksConfig = new MetricsSinksConfig(metricsSinksConfigFilename);

    LOG.info("Sinks Config:" + sinksConfig.toString());

    MetricsManager metricsManager = new MetricsManager(
        topologyName, METRICS_MANAGER_HOST, metricsPort, metricsmgrId, systemConfig, sinksConfig);
    metricsManager.start();

    LOG.info("Loops terminated. Metrics Manager exits.");
  }

  private void setupJVMMetrics(int systemMetricsSampleIntervalSec) {
    this.jvmMetrics.registerMetrics(metricsCollector);

    // Attach sample Runnable to gatewayMetricsCollector
    this.metricsCollector.registerMetricSampleRunnable(jvmMetrics.getJVMSampleRunnable(),
        systemMetricsSampleIntervalSec);
  }

  private void setupInternalMetricsExport() {
    Runnable gatherInternalMetrics = new Runnable() {
      @Override
      public void run() {
        while (!metricsQueue.isEmpty()) {
          Metrics.MetricPublisherPublishMessage message = metricsQueue.poll();
          metricsManagerServer.onInternalMessage(metricsManagerPublisher, message);
        }

        // It schedules itself in future
        metricsManagerServerLoop.registerTimerEventInSeconds(heronMetricsExportIntervalSec,
            this);
      }
    };

    metricsManagerServerLoop.registerTimerEventInSeconds(heronMetricsExportIntervalSec,
        gatherInternalMetrics);
  }

  private SinkExecutor initSinkExecutor(String sinkId) {
    IMetricsSink sink;
    String classname =
        (String) config.getConfigForSink(sinkId).get(MetricsSinksConfig.CONFIG_KEY_CLASSNAME);
    try {
      sink = (IMetricsSink) Class.forName(classname).newInstance();
    } catch (InstantiationException e) {
      throw new RuntimeException(e + " IMetricsSink class must have a no-arg constructor.");
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e + " IMetricsSink class must be concrete.");
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e + " IMetricsSink class must be a class path.");
    }
    SlaveLooper sinkExecutorLoop = new SlaveLooper();
    Communicator<MetricsRecord> executorInMetricsQueue =
        new Communicator<MetricsRecord>(null, sinkExecutorLoop);

    // Since MetricsCollector is not thread-safe,
    // we need to specify individual MetricsCollector and MultiCountMetric
    // for different SinkExecutor
    MetricsCollector sinkMetricsCollector = new MetricsCollector(sinkExecutorLoop, metricsQueue);
    MultiCountMetric internalCounters = new MultiCountMetric();
    sinkMetricsCollector.registerMetric(sinkId, internalCounters, heronMetricsExportIntervalSec);

    // Set up the SinkContext
    SinkContext sinkContext =
        new SinkContextImpl(topologyName, metricsmgrId, sinkId, internalCounters);

    SinkExecutor sinkExecutor =
        new SinkExecutor(sinkId, sink, sinkExecutorLoop, executorInMetricsQueue, sinkContext);
    sinkExecutor.setPropertyMap(config.getConfigForSink(sinkId));

    return sinkExecutor;
  }

  public void start() {
    LOG.info("Starting the Executors.");
    // Execute the SinkExecutor in separate threads
    for (SinkExecutor executor : sinkExecutors.values()) {
      executors.execute(executor);
    }

    // The MetricsManagerServer would run in the main thread
    // We do it in the final step since it would await the main thread
    LOG.info("Starting Metrics Manager Server");
    metricsManagerServer.start();
    metricsManagerServerLoop.loop();
  }

  /**
   * Handler for catching exceptions thrown by any threads (owned either by topology or heron
   * infrastructure).
   * When one IMetricsSink throws uncaught exceptions, we would try to restart this sink unless
   * we have hit the # of retry attempts.
   * When Metrics Manager internal exceptions are caught or we have restart IMetricsSink with
   * too many attempts, Metrics Manager would flush any remain logs and exit.
   */
  public class DefaultExceptionHandler implements Thread.UncaughtExceptionHandler {

    /**
     * Handler for uncaughtException
     */
    public void uncaughtException(Thread thread, Throwable exception) {
      // Add try and catch block to prevent new exceptions stop the handling thread
      try {
        // Delegate to the actual one
        handleException(thread, exception);

        // SUPPRESS CHECKSTYLE IllegalCatch
      } catch (Throwable t) {
        LOG.log(Level.SEVERE, "Failed to handle exception. Process halting", t);
        Runtime.getRuntime().halt(1);
      }
    }

    // The actual uncaught exceptions handing logic
    private void handleException(Thread thread, Throwable exception) {
      // We would fail fast when errors occur
      if (exception instanceof Error) {
        LOG.log(Level.SEVERE,
            "Error caught in thread: " + thread.getName()
                + " with thread id: " + thread.getId() + ". Process halting...",
            exception);
        Runtime.getRuntime().halt(1);
      }

      // We would fail fast when exceptions happen in main thread
      if (thread.getId() == mainThreadId) {
        LOG.log(Level.SEVERE,
            "Exception caught in main thread. Process halting...",
            exception);
        Runtime.getRuntime().halt(1);
      }

      LOG.log(Level.SEVERE,
          "Exception caught in thread: " + thread.getName()
              + " with thread id: " + thread.getId(),
          exception);

      String sinkId = null;
      Integer thisSinkRetryAttempts = 0;

      // We enforced the name of thread running particular IMetricsSink equal to its sink-id
      // If the thread name is a key of SinkExecutors, then it is a thread running IMetricsSink
      if (sinkExecutors.containsKey(thread.getName())) {
        sinkId = thread.getName();
        // Remove the old sink executor
        SinkExecutor oldSinkExecutor = sinkExecutors.remove(sinkId);
        // Remove the unneeded Communicator bind with Metrics Manager Server
        metricsManagerServer.removeSinkCommunicator(oldSinkExecutor.getCommunicator());

        // Close the sink
        SysUtils.closeIgnoringExceptions(oldSinkExecutor);

        thisSinkRetryAttempts = sinksRetryAttempts.remove(sinkId);
      }

      if (sinkId != null && thisSinkRetryAttempts != 0) {
        LOG.info(String.format("Restarting IMetricsSink: %s with %d available retries",
            sinkId, thisSinkRetryAttempts));

        // That means it was a sinkExecutor throwing exceptions and threadName is sinkId
        SinkExecutor newSinkExecutor = initSinkExecutor(sinkId);

        // Update the SinkExecutor in sinkExecutors
        sinkExecutors.put(sinkId, newSinkExecutor);

        // Update the retry attempts if it is > 0
        if (thisSinkRetryAttempts > 0) {
          thisSinkRetryAttempts--;
        }
        sinksRetryAttempts.put(sinkId, thisSinkRetryAttempts);

        // Update the list of Communicator in Metrics Manager Server
        metricsManagerServer.addSinkCommunicator(newSinkExecutor.getCommunicator());

        // Restart it
        executors.execute(newSinkExecutor);
      } else if (sinkId != null
          && thisSinkRetryAttempts == 0
          && sinkExecutors.size() > 0) {
        // If the dead executor is the only one executor and it is removed,
        // e.g. sinkExecutors.size() == 0, we would exit the process directly

        LOG.severe("Failed to recover from exceptions for IMetricsSink: " + sinkId);
        LOG.info(sinkId + " would close and keep running rest sinks: " + sinkExecutors.keySet());
      } else {
        // It is not recoverable (retried too many times, or not an exception from IMetricsSink)
        // So we would do basic cleaning and exit
        LOG.info("Failed to recover from exceptions; Metrics Manager Exiting");
        for (Handler handler : java.util.logging.Logger.getLogger("").getHandlers()) {
          handler.close();
        }
        // Attempts to shutdown all the thread in threadsPool. This will send Interrupt to every
        // thread in the pool. Threads may implement a clean Interrupt logic.
        executors.shutdownNow();

        // (including threads not owned by HeronInstance). To be safe, not sending these
        // interrupts.
        Runtime.getRuntime().halt(1);
      }
    }
  }
}
