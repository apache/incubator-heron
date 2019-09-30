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

package org.apache.heron.metricsmgr;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.heron.api.metric.MultiCountMetric;
import org.apache.heron.common.basics.Communicator;
import org.apache.heron.common.basics.NIOLooper;
import org.apache.heron.common.basics.SingletonRegistry;
import org.apache.heron.common.basics.SlaveLooper;
import org.apache.heron.common.basics.SysUtils;
import org.apache.heron.common.basics.TypeUtils;
import org.apache.heron.common.config.SystemConfig;
import org.apache.heron.common.network.HeronSocketOptions;
import org.apache.heron.common.utils.logging.ErrorReportLoggingHandler;
import org.apache.heron.common.utils.logging.LoggingHelper;
import org.apache.heron.common.utils.metrics.JVMMetrics;
import org.apache.heron.common.utils.metrics.MetricsCollector;
import org.apache.heron.metricsmgr.executor.SinkExecutor;
import org.apache.heron.metricsmgr.sink.SinkContextImpl;
import org.apache.heron.proto.system.Metrics;
import org.apache.heron.spi.metricsmgr.metrics.MetricsRecord;
import org.apache.heron.spi.metricsmgr.sink.IMetricsSink;
import org.apache.heron.spi.metricsmgr.sink.SinkContext;

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

  private final Duration heronMetricsExportInterval;
  private final String topologyName;
  private final String cluster;
  private final String role;
  private final String environment;
  private final String metricsmgrId;

  private final long mainThreadId;

  /**
   * Metrics manager constructor
   */
  public MetricsManager(String topologyName, String cluster, String role, String environment,
                        String serverHost, int serverPort, String metricsmgrId,
                        SystemConfig systemConfig, MetricsSinksConfig config)
      throws IOException {
    this.topologyName = topologyName;
    this.cluster = cluster;
    this.role = role;
    this.environment = environment;
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
    this.heronMetricsExportInterval = systemConfig.getHeronMetricsExportInterval();

    this.mainThreadId = Thread.currentThread().getId();

    // Init the ErrorReportHandler
    ErrorReportLoggingHandler.init(metricsCollector, heronMetricsExportInterval,
        systemConfig.getHeronMetricsMaxExceptionsPerMessageCount());

    // Set up the internal Metrics Export routine
    setupInternalMetricsExport();

    // Set up JVM metrics
    // TODO -- change the config name
    setupJVMMetrics(systemConfig.getInstanceMetricsSystemSampleInterval());

    // Init the HeronSocketOptions
    HeronSocketOptions serverSocketOptions =
        new HeronSocketOptions(systemConfig.getMetricsMgrNetworkWriteBatchSize(),
            systemConfig.getMetricsMgrNetworkWriteBatchTime(),
            systemConfig.getMetricsMgrNetworkReadBatchSize(),
            systemConfig.getMetricsMgrNetworkReadBatchTime(),
            systemConfig.getMetricsMgrNetworkOptionsSocketSendBufferSize(),
            systemConfig.getMetricsMgrNetworkOptionsSocketReceivedBufferSize(),
            systemConfig.getMetricsMgrNetworkOptionsMaximumPacketSize());

    // Set the MultiCountMetric for MetricsManagerServer
    MultiCountMetric serverCounters = new MultiCountMetric();
    metricsCollector.registerMetric(METRICS_MANAGER_COMPONENT_NAME,
        serverCounters, (int) heronMetricsExportInterval.getSeconds());

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
      metricsManagerServer.addSinkCommunicator(sinkId, sinkExecutor.getCommunicator());
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

  // Construct all required command line options
  private static Options constructOptions() {
    Option id = Option.builder()
        .desc("Metrics manager id")
        .longOpt("id")
        .hasArgs()
        .argName("id")
        .required()
        .build();

    Option port = Option.builder()
        .desc("Metrics manager port")
        .longOpt("port")
        .hasArgs()
        .argName("port")
        .required()
        .build();

    Option topology = Option.builder()
        .desc("The name of the topology to collect metrics from")
        .longOpt("topology")
        .hasArgs()
        .argName("topology")
        .required()
        .build();

    Option topologyId = Option.builder()
        .desc("The name of the topology to collect metrics from")
        .longOpt("topology-id")
        .hasArgs()
        .argName("topologyId")
        .required()
        .build();

    Option cluster = Option.builder()
        .desc("The name of the topology to collect metrics from")
        .longOpt("cluster")
        .hasArgs()
        .argName("cluster")
        .required()
        .build();

    Option role = Option.builder()
        .desc("The name of the topology to collect metrics from")
        .longOpt("role")
        .hasArgs()
        .argName("role")
        .required()
        .build();

    Option environment = Option.builder()
        .desc("The name of the topology to collect metrics from")
        .longOpt("environment")
        .hasArgs()
        .argName("environment")
        .required()
        .build();

    Option sinkConfig = Option.builder()
        .desc("The name of the topology to collect metrics from")
        .longOpt("sink-config-file")
        .hasArgs()
        .argName("sink config file")
        .required()
        .build();

    Option systemConfig = Option.builder()
        .desc("The name of the topology to collect metrics from")
        .longOpt("system-config-file")
        .hasArgs()
        .argName("system config file")
        .required()
        .build();

    Option overrideConfig = Option.builder()
        .desc("The name of the topology to collect metrics from")
        .longOpt("override-config-file")
        .hasArgs()
        .argName("override config file")
        .required()
        .build();

    return new Options()
        .addOption(id)
        .addOption(port)
        .addOption(topology)
        .addOption(topologyId)
        .addOption(cluster)
        .addOption(role)
        .addOption(environment)
        .addOption(systemConfig)
        .addOption(overrideConfig)
        .addOption(sinkConfig);
  }

  // construct command line help options
  private static Options constructHelpOptions() {
    Options options = new Options();
    Option help = Option.builder("h")
        .desc("List all options and their description")
        .longOpt("help")
        .build();

    options.addOption(help);
    return options;
  }

  // Print usage options
  private static void usage(Options options) {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp("MetricsManager", options);
  }

  public static void main(String[] args) throws Exception {
    final Options options = constructOptions();
    final Options helpOptions = constructHelpOptions();

    final CommandLineParser parser = new DefaultParser();

    // parse the help options first.
    CommandLine cmd = parser.parse(helpOptions, args, true);
    if (cmd.hasOption("h")) {
      usage(options);
      return;
    }

    try {
      // Now parse the required options
      cmd = parser.parse(options, args);
    } catch (ParseException pe) {
      usage(options);
      throw new RuntimeException("Error parsing command line options: ", pe);
    }

    String metricsmgrId = cmd.getOptionValue("id");
    int metricsPort = Integer.parseInt(cmd.getOptionValue("port"));
    String topologyName = cmd.getOptionValue("topology");
    String topologyId = cmd.getOptionValue("topology-id");
    String systemConfigFilename = cmd.getOptionValue("system-config-file");
    String overrideConfigFilename = cmd.getOptionValue("override-config-file");
    String metricsSinksConfigFilename = cmd.getOptionValue("sink-config-file");
    String cluster = cmd.getOptionValue("cluster");
    String role = cmd.getOptionValue("role");
    String environment = cmd.getOptionValue("environment");

    SystemConfig systemConfig = SystemConfig.newBuilder(true)
        .putAll(systemConfigFilename, true)
        .putAll(overrideConfigFilename, true)
        .build();

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
            systemConfig.getHeronLoggingMaximumSize(),
            systemConfig.getHeronLoggingMaximumFiles()));
    LoggingHelper.addLoggingHandler(new ErrorReportLoggingHandler());

    LOG.info(String.format("Starting Metrics Manager for topology %s with topologyId %s with "
            + "Metrics Manager Id %s, Metrics Manager Port: %d, for cluster/role/env %s.",
        topologyName, topologyId, metricsmgrId, metricsPort,
        String.format("%s/%s/%s", cluster, role, environment)));

    LOG.info("System Config: " + systemConfig);

    // Populate the config
    MetricsSinksConfig sinksConfig = new MetricsSinksConfig(metricsSinksConfigFilename,
                                                            overrideConfigFilename);

    LOG.info("Sinks Config:" + sinksConfig.toString());

    MetricsManager metricsManager =
        new MetricsManager(topologyName, cluster, role, environment,
            METRICS_MANAGER_HOST, metricsPort, metricsmgrId, systemConfig, sinksConfig);
    metricsManager.start();

    LOG.info("Loops terminated. Metrics Manager exits.");
  }

  private void setupJVMMetrics(Duration systemMetricsSampleInterval) {
    this.jvmMetrics.registerMetrics(metricsCollector);

    // Attach sample Runnable to gatewayMetricsCollector
    this.metricsCollector.registerMetricSampleRunnable(jvmMetrics.getJVMSampleRunnable(),
        systemMetricsSampleInterval);
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
        metricsManagerServerLoop.registerTimerEvent(heronMetricsExportInterval,
            this);
      }
    };

    metricsManagerServerLoop.registerTimerEvent(heronMetricsExportInterval,
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
    sinkMetricsCollector
        .registerMetric(sinkId, internalCounters, (int) heronMetricsExportInterval.getSeconds());

    // Set up the SinkContext
    SinkContext sinkContext =
        new SinkContextImpl(topologyName, cluster, role, environment,
            metricsmgrId, sinkId, internalCounters);

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
        // Remove the unneeded Communicator bind with Metrics Manager Server
        metricsManagerServer.removeSinkCommunicator(sinkId);

        // Remove the old sink executor and close the sink
        SinkExecutor oldSinkExecutor = sinkExecutors.remove(sinkId);
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
        metricsManagerServer.addSinkCommunicator(sinkId, newSinkExecutor.getCommunicator());

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
