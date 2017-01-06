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


package com.twitter.heron.metricscachemgr;

import java.io.IOException;
import java.net.InetAddress;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.twitter.heron.common.basics.Constants;
import com.twitter.heron.common.basics.NIOLooper;
import com.twitter.heron.common.basics.SingletonRegistry;
import com.twitter.heron.common.basics.SysUtils;
import com.twitter.heron.common.config.SystemConfig;
import com.twitter.heron.common.network.HeronSocketOptions;
import com.twitter.heron.common.utils.logging.ErrorReportLoggingHandler;
import com.twitter.heron.common.utils.logging.LoggingHelper;
import com.twitter.heron.metricscachemgr.metricscache.MetricsCache;
import com.twitter.heron.metricsmgr.MetricsSinksConfig;
import com.twitter.heron.proto.tmaster.TopologyMaster;
import com.twitter.heron.spi.common.ClusterConfig;
import com.twitter.heron.spi.common.ClusterDefaults;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Context;
import com.twitter.heron.spi.common.Defaults;
import com.twitter.heron.spi.common.Keys;
import com.twitter.heron.spi.metricsmgr.metrics.MetricsFilter;
import com.twitter.heron.spi.statemgr.IStateManager;
import com.twitter.heron.spi.utils.ReflectionUtils;

//import com.twitter.heron.spi.common.ConfigDefaults;

//import com.twitter.heron.scheduler.TopologyRuntimeManagementException;

/**
 * main entry for metrics cache manager
 */
public class MetricsCacheManager {
  public static final String METRICS_SINKS_TMASTER_SINK = "tmaster-sink";
  public static final String METRICS_SINKS_TMASTER_METRICS = "tmaster-metrics-type";
  private static final Logger LOG = Logger.getLogger(MetricsCacheManager.class.getName());
  // Pre-defined value
  private static final String METRICS_CACHE_HOST = "0.0.0.0";
  private static final String METRICS_CACHE_COMPONENT_NAME = "__metricscachemgr__";
  private static final int METRICS_CACHE_INSTANCE_ID = -1;

  private final MetricsCacheManagerServer metricsCacheManagerServer;
  // The looper drives MetricsManagerServer
  private final NIOLooper metricsCacheManagerServerLoop;

  private final String topologyName;
  private final String metricsmgrId;

  private final MetricsSinksConfig msConfig;

  // map from metric prefix to its aggregation form
  private MetricsFilter metricsfilter = null;

  private MetricsCache metricsCache;
  private Config config;                        // holds all the config read
  private TopologyMaster.MetricsCacheLocation metricscacheLocation;

  /**
   * constructor
   */
  public MetricsCacheManager(String topologyName,
                             String serverHost, int masterPort, int statsPort,
                             String metricsCacheMgrId,
                             SystemConfig systemConfig, MetricsSinksConfig msconfig,
                             Config configExpand,
                             TopologyMaster.MetricsCacheLocation metricscacheLocation)
      throws IOException {
    this.topologyName = topologyName;
    this.metricsmgrId = metricsCacheMgrId;
    this.msConfig = msconfig;
    this.config = configExpand;
    this.metricsCacheManagerServerLoop = new NIOLooper();
    this.metricscacheLocation = metricscacheLocation;

    metricsCache = new MetricsCache(
        systemConfig.getTmasterMetricsCollectorMaximumIntervalMin() * 60,
        systemConfig.getTmasterMetricsCollectorPurgeIntervalSec(),
        msconfig);

    // Init the HeronSocketOptions
    HeronSocketOptions serverSocketOptions =
        new HeronSocketOptions(systemConfig.getMetricsMgrNetworkWriteBatchSizeBytes(),
            systemConfig.getMetricsMgrNetworkWriteBatchTimeMs(),
            systemConfig.getMetricsMgrNetworkReadBatchSizeBytes(),
            systemConfig.getMetricsMgrNetworkReadBatchTimeMs(),
            systemConfig.getMetricsMgrNetworkOptionsSocketSendBufferSizeBytes(),
            systemConfig.getMetricsMgrNetworkOptionsSocketReceivedBufferSizeBytes());

    // Construct the MetricsManagerServer
    metricsCacheManagerServer = new MetricsCacheManagerServer(metricsCacheManagerServerLoop,
        serverHost, masterPort, serverSocketOptions, metricsCache);

    metricsCacheManagerServer.registerOnMessage(TopologyMaster.PublishMetrics.newBuilder());
    metricsCacheManagerServer.registerOnRequest(TopologyMaster.MetricRequest.newBuilder());
    metricsCacheManagerServer.registerOnRequest(TopologyMaster.ExceptionLogRequest.newBuilder());
  }

  public static void main(String[] args) throws Exception {
    if (args.length != 10) {
      throw new RuntimeException(
          "Invalid arguments; Usage: java com.twitter.heron.metricscachemgr.MetricsCacheManager "
              + "<id> <master-port> <stats-port> <topname> <topid> "
              + "<heron_internals_config_filename> <metrics_sinks_config_filename> "
              + "<cluster> <role> <environ>");
    }

    String metricsCacheMgrId = args[0];
    int masterPort = Integer.parseInt(args[1]);
    int statsPort = Integer.parseInt(args[2]);
    String topologyName = args[3];
    String topologyId = args[4];
    String systemConfigFilename = args[5];
    String metricsSinksConfigFilename = args[6];
    String cluster = args[7];
    String role = args[8];
    String environ = args[9];
//    String heronHome = args[10];
//    String heronConfigPath = args[11];
//    String heronReleaseFile = args[12];

    //-----------------
    SystemConfig systemConfig = new SystemConfig(systemConfigFilename, true);
    // Add the SystemConfig into SingletonRegistry
    SingletonRegistry.INSTANCE.registerSingleton(SystemConfig.HERON_SYSTEM_CONFIG, systemConfig);

    // Init the logging setting and redirect the stdout and stderr to logging
    // For now we just set the logging level as INFO; later we may accept an argument to set it.
    Level loggingLevel = Level.ALL; // for prototype debug
    String loggingDir = systemConfig.getHeronLoggingDirectory();

    // Log to file and TMaster
    LoggingHelper.loggerInit(loggingLevel, true);
    LoggingHelper.addLoggingHandler(
        LoggingHelper.getFileHandler(metricsCacheMgrId, loggingDir, true,
            systemConfig.getHeronLoggingMaximumSizeMb() * Constants.MB_TO_BYTES,
            systemConfig.getHeronLoggingMaximumFiles()));
    LoggingHelper.addLoggingHandler(new ErrorReportLoggingHandler());

    LOG.info(String.format("Starting MetricsCache for topology %s with topologyId %s with "
            + "MetricsCache Id %s, MericsCache Port: %d.",
        topologyName, topologyId, metricsCacheMgrId, masterPort));

    LOG.info("System Config: " + systemConfig);

    //-----------------
    // Populate the msConfig
    MetricsSinksConfig sinksConfig = new MetricsSinksConfig(metricsSinksConfigFilename);

    LOG.info("Sinks Config: " + sinksConfig.toString());

    //----------------
    Config.Builder config = Config.newBuilder()
        .putAll(ClusterDefaults.getDefaults())
        .putAll(ClusterDefaults.getSandboxDefaults())
        .putAll(ClusterConfig.loadConfig(Defaults.heronSandboxHome(),
            Defaults.heronSandboxConf(), "./release.yaml"))
        .put(Keys.cluster(), cluster)
        .put(Keys.role(), role)
        .put(Keys.environ(), environ)
        .put(Keys.topologyName(), topologyName)
        .put(Keys.topologyId(), topologyId);
    LOG.info("Config: " + config.build().toString());

    Config configExpand = Config.expand(config.build());
    LOG.info("Config: " + config.toString());

    //-----------------
    TopologyMaster.MetricsCacheLocation metricscacheLocation =
        TopologyMaster.MetricsCacheLocation.newBuilder()
            .setTopologyName(topologyName)
            .setTopologyId(topologyId)
            .setHost(InetAddress.getLocalHost().getHostName())
            .setControllerPort(-1) // not used for metricscache
            .setMasterPort(masterPort)
            .setStatsPort(statsPort)
            .build();

    MetricsCacheManager metricsCacheManager = new MetricsCacheManager(
        topologyName, METRICS_CACHE_HOST, masterPort, statsPort, metricsCacheMgrId,
        systemConfig, sinksConfig, configExpand, metricscacheLocation);
    metricsCacheManager.start();

    LOG.info("Loops terminated. MetricsCache Manager exits.");
  }

  public void start() throws Exception {
    // 1. Do prepare work
    // create an instance of state manager
    String statemgrClass = Context.stateManagerClass(config);
    LOG.info("Context.stateManagerClass " + statemgrClass);
    IStateManager statemgr;
    try {
      statemgr = ReflectionUtils.newInstance(statemgrClass);
    } catch (IllegalAccessException | InstantiationException | ClassNotFoundException e) {
      throw new Exception(String.format(
          "Failed to instantiate state manager class '%s'",
          statemgrClass), e);
    }

    // Put it in a try block so that we can always clean resources
    try {
      // initialize the statemgr
      statemgr.initialize(config);

      statemgr.setMetricsCacheLocation(metricscacheLocation, topologyName);
      LOG.info("metricscacheLocation " + metricscacheLocation.toString());
      LOG.info("topologyName " + topologyName.toString());

      // The MetricsCacheServer would run in the main thread
      // We do it in the final step since it would await the main thread
      LOG.info("Starting Metrics Cache Server");
      metricsCacheManagerServer.start();
      metricsCacheManagerServerLoop.loop();
    } finally {
      // 3. Do post work basing on the result
      // Currently nothing to do here

      // 4. Close the resources
      SysUtils.closeIgnoringExceptions(statemgr);
    }


  }
}
