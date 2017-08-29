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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.twitter.heron.common.basics.NIOLooper;
import com.twitter.heron.common.basics.SysUtils;
import com.twitter.heron.common.config.SystemConfig;
import com.twitter.heron.common.network.HeronSocketOptions;
import com.twitter.heron.common.utils.logging.ErrorReportLoggingHandler;
import com.twitter.heron.common.utils.logging.LoggingHelper;
import com.twitter.heron.metricscachemgr.metricscache.MetricsCache;
import com.twitter.heron.metricsmgr.MetricsSinksConfig;
import com.twitter.heron.proto.tmaster.TopologyMaster;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.ConfigLoader;
import com.twitter.heron.spi.common.Context;
import com.twitter.heron.spi.common.Key;
import com.twitter.heron.spi.statemgr.IStateManager;
import com.twitter.heron.spi.utils.ReflectionUtils;

/**
 * main entry for metrics cache manager
 * MetricsCacheManager holds three major objects:
 * 1.MetricsCache metricsCache: it is the store where metrics and exceptions are cached.
 * 2.MetricsCacheManagerServer metricsCacheManagerServer: it accepts the metric message from sink.
 * 3.MetricsCacheManagerHttpServer metricsCacheManagerHttpServer: it responds to queries.
 */
public class MetricsCacheManager {
  private static final Logger LOG = Logger.getLogger(MetricsCacheManager.class.getName());

  private static final String METRICS_CACHE_HOST = "0.0.0.0";
  private static final String METRICS_CACHE_COMPONENT_NAME = "__metricscachemgr__";
  private static final int METRICS_CACHE_INSTANCE_ID = -1;

  // accepts messages from sinks
  private MetricsCacheManagerServer metricsCacheManagerServer;
  // The looper drives MetricsManagerServer
  private NIOLooper metricsCacheManagerServerLoop;

  // respond to query requests
  private MetricsCacheManagerHttpServer metricsCacheManagerHttpServer;

  private String topologyName;

  private MetricsCache metricsCache;

  private Config config;

  private TopologyMaster.MetricsCacheLocation metricsCacheLocation;

  /**
   * Constructor: MetricsCacheManager needs 4 type information:
   * 1. Servers: host and 2 ports
   * 2. Topology: name
   * 3. Config: heron config, sink config, and cli config
   * 4. State: location node to be put in the state-mgr
   *
   * @param topologyName topology name
   * @param serverHost server host
   * @param masterPort port to accept message from sink
   * @param statsPort port to respond to query request
   * @param systemConfig heron config
   * @param metricsSinkConfig sink config
   * @param configExpand other config
   * @param metricsCacheLocation location for state mgr
   */
  public MetricsCacheManager(String topologyName,
                             String serverHost, int masterPort, int statsPort,
                             SystemConfig systemConfig, MetricsSinksConfig metricsSinkConfig,
                             Config configExpand,
                             TopologyMaster.MetricsCacheLocation metricsCacheLocation)
      throws IOException {
    this.topologyName = topologyName;
    this.config = configExpand;
    this.metricsCacheLocation = metricsCacheLocation;

    metricsCacheManagerServerLoop = new NIOLooper();

    // initialize cache and hook to the shared nio-looper
    metricsCache = new MetricsCache(systemConfig, metricsSinkConfig, metricsCacheManagerServerLoop);

    // Init the HeronSocketOptions
    HeronSocketOptions serverSocketOptions =
        new HeronSocketOptions(systemConfig.getMetricsMgrNetworkWriteBatchSize(),
            systemConfig.getMetricsMgrNetworkWriteBatchTime(),
            systemConfig.getMetricsMgrNetworkReadBatchSize(),
            systemConfig.getMetricsMgrNetworkReadBatchTime(),
            systemConfig.getMetricsMgrNetworkOptionsSocketSendBufferSize(),
            systemConfig.getMetricsMgrNetworkOptionsSocketReceivedBufferSize(),
            systemConfig.getMetricsMgrNetworkOptionsMaximumPacketSize());

    // Construct the server to accepts messages from sinks
    metricsCacheManagerServer = new MetricsCacheManagerServer(metricsCacheManagerServerLoop,
        serverHost, masterPort, serverSocketOptions, metricsCache);

    metricsCacheManagerServer.registerOnMessage(TopologyMaster.PublishMetrics.newBuilder());
    metricsCacheManagerServer.registerOnRequest(TopologyMaster.MetricRequest.newBuilder());
    metricsCacheManagerServer.registerOnRequest(TopologyMaster.ExceptionLogRequest.newBuilder());

    // Construct the server to respond to query request
    metricsCacheManagerHttpServer = new MetricsCacheManagerHttpServer(metricsCache, statsPort);
  }

  // Construct all required command line options
  private static Options constructOptions() {
    Options options = new Options();

    Option cluster = Option.builder("c")
        .desc("Cluster name in which the topology needs to run on")
        .longOpt("cluster")
        .hasArgs()
        .argName("cluster")
        .required()
        .build();

    Option role = Option.builder("r")
        .desc("Role under which the topology needs to run")
        .longOpt("role")
        .hasArgs()
        .argName("role")
        .required()
        .build();

    Option environment = Option.builder("e")
        .desc("Environment under which the topology needs to run")
        .longOpt("environment")
        .hasArgs()
        .argName("environment")
        .required()
        .build();

    Option masterPort = Option.builder("m")
        .desc("Master port to accept the metric/exception messages from sinks")
        .longOpt("master_port")
        .hasArgs()
        .argName("master port")
        .required()
        .build();

    Option statsPort = Option.builder("s")
        .desc("Stats port to respond the queries on metrics/exceptions")
        .longOpt("stats_port")
        .hasArgs()
        .argName("stats port")
        .required()
        .build();

    Option systemConfig = Option.builder("y")
        .desc("System configuration file path")
        .longOpt("system_config_file")
        .hasArgs()
        .argName("system config file")
        .build();

    Option sinkConfig = Option.builder("i")
        .desc("Sink configuration file path")
        .longOpt("sink_config_file")
        .hasArgs()
        .argName("sink config file")
        .build();

    Option topologyName = Option.builder("n")
        .desc("The topology name")
        .longOpt("topology_name")
        .hasArgs()
        .argName("topology name")
        .required()
        .build();

    Option topologyId = Option.builder("t")
        .desc("The topology id")
        .longOpt("topology_id")
        .hasArgs()
        .argName("topology id")
        .required()
        .build();

    Option metricsCacheId = Option.builder("r")
        .desc("The MetricsCache id")
        .longOpt("metricscache_id")
        .hasArgs()
        .argName("metricscache id")
        .required()
        .build();

    Option verbose = Option.builder("v")
        .desc("Enable debug logs")
        .longOpt("verbose")
        .build();

    options.addOption(cluster);
    options.addOption(role);
    options.addOption(environment);
    options.addOption(masterPort);
    options.addOption(statsPort);
    options.addOption(systemConfig);
    options.addOption(sinkConfig);
    options.addOption(topologyName);
    options.addOption(topologyId);
    options.addOption(metricsCacheId);
    options.addOption(verbose);

    return options;
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
    formatter.printHelp("MetricsCache", options);
  }

  public static void main(String[] args) throws Exception {
    Options options = constructOptions();
    Options helpOptions = constructHelpOptions();

    CommandLineParser parser = new DefaultParser();

    // parse the help options first.
    CommandLine cmd = parser.parse(helpOptions, args, true);
    if (cmd.hasOption("h")) {
      usage(options);
      return;
    }

    try {
      // Now parse the required options
      cmd = parser.parse(options, args);
    } catch (ParseException e) {
      usage(options);
      throw new RuntimeException("Error parsing command line options: ", e);
    }

    Level logLevel = Level.INFO;
    if (cmd.hasOption("v")) {
      logLevel = Level.ALL;
    }

    String cluster = cmd.getOptionValue("cluster");
    String role = cmd.getOptionValue("role");
    String environ = cmd.getOptionValue("environment");
    int masterPort = Integer.valueOf(cmd.getOptionValue("master_port"));
    int statsPort = Integer.valueOf(cmd.getOptionValue("stats_port"));
    String systemConfigFilename = cmd.getOptionValue("system_config_file");
    String metricsSinksConfigFilename = cmd.getOptionValue("sink_config_file");
    String topologyName = cmd.getOptionValue("topology_name");
    String topologyId = cmd.getOptionValue("topology_id");
    String metricsCacheMgrId = cmd.getOptionValue("metricscache_id");

    // read heron internal config file
    SystemConfig systemConfig = SystemConfig.newBuilder(true)
        .putAll(systemConfigFilename, true)
        .build();

    // Log to file and sink(exception)
    LoggingHelper.loggerInit(logLevel, true);
    LoggingHelper.addLoggingHandler(LoggingHelper.getFileHandler(metricsCacheMgrId,
        systemConfig.getHeronLoggingDirectory(), true,
        systemConfig.getHeronLoggingMaximumSize(),
        systemConfig.getHeronLoggingMaximumFiles()));
    LoggingHelper.addLoggingHandler(new ErrorReportLoggingHandler());

    LOG.info(String.format("Starting MetricsCache for topology %s with topologyId %s with "
            + "MetricsCache Id %s, master port: %d.",
        topologyName, topologyId, metricsCacheMgrId, masterPort));

    LOG.info("System Config: " + systemConfig);

    // read sink config file
    MetricsSinksConfig sinksConfig = new MetricsSinksConfig(metricsSinksConfigFilename);
    LOG.info("Sinks Config: " + sinksConfig.toString());

    // build config from cli
    Config config = Config.toClusterMode(Config.newBuilder()
        .putAll(ConfigLoader.loadClusterConfig())
        .putAll(Config.newBuilder()
            .put(Key.CLUSTER, cluster).put(Key.ROLE, role).put(Key.ENVIRON, environ)
            .build())
        .putAll(Config.newBuilder()
            .put(Key.TOPOLOGY_NAME, topologyName).put(Key.TOPOLOGY_ID, topologyId)
            .build())
        .build());
    LOG.info("Cli Config: " + config.toString());

    // build metricsCache location
    TopologyMaster.MetricsCacheLocation metricsCacheLocation =
        TopologyMaster.MetricsCacheLocation.newBuilder()
            .setTopologyName(topologyName)
            .setTopologyId(topologyId)
            .setHost(InetAddress.getLocalHost().getHostName())
            .setControllerPort(-1) // not used for metricscache
            .setMasterPort(masterPort)
            .setStatsPort(statsPort)
            .build();

    MetricsCacheManager metricsCacheManager = new MetricsCacheManager(
        topologyName, METRICS_CACHE_HOST, masterPort, statsPort,
        systemConfig, sinksConfig, config, metricsCacheLocation);
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

      statemgr.setMetricsCacheLocation(metricsCacheLocation, topologyName);
      LOG.info("metricsCacheLocation " + metricsCacheLocation.toString());
      LOG.info("topologyName " + topologyName.toString());

      LOG.info("Starting Metrics Cache HTTP Server");
      metricsCacheManagerHttpServer.start();

      // 2. The MetricsCacheServer would run in the main thread
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
