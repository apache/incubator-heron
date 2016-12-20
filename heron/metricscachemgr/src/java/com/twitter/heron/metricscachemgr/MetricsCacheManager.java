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
import java.util.logging.Level;
import java.util.logging.Logger;

import com.twitter.heron.common.basics.Constants;
import com.twitter.heron.common.basics.SingletonRegistry;
import com.twitter.heron.common.config.SystemConfig;
import com.twitter.heron.common.utils.logging.ErrorReportLoggingHandler;
import com.twitter.heron.common.utils.logging.LoggingHelper;
import com.twitter.heron.metricsmgr.MetricsManager;
import com.twitter.heron.metricsmgr.MetricsSinksConfig;
import com.twitter.heron.spi.metricsmgr.metrics.MetricsFilter;

/**
 * main entry for metrics cache manager
 */
public class MetricsCacheManager {
  private static final Logger LOG = Logger.getLogger(MetricsCacheManager.class.getName());

  // Pre-defined value
  private static final String METRICS_CACHE_HOST = "127.0.0.1";
  private static final String METRICS_CACHE_COMPONENT_NAME = "__metricscachemgr__";

  public static final String METRICS_SINKS_TMASTER_SINK = "tmaster-sink";
  public static final String METRICS_SINKS_TMASTER_METRICS = "tmaster-metrics-type";

  // map from metric prefix to its aggregation form
  private MetricsFilter metricsfilter = null;

  public static void main(String[] args) throws IOException {
    if (args.length != 6) {
      throw new RuntimeException(
          "Invalid arguments; Usage: java com.twitter.heron.metricscachemgr.MetricsCacheManager "
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
        topologyName, METRICS_CACHE_HOST, metricsPort, metricsmgrId, systemConfig, sinksConfig);
    metricsManager.start();

    LOG.info("Loops terminated. Metrics Manager exits.");
  }
}
