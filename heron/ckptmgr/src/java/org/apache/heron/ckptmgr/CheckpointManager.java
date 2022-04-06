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

package org.apache.heron.ckptmgr;

import java.io.IOException;
import java.util.Collections;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.heron.api.utils.Slf4jUtils;
import org.apache.heron.common.basics.NIOLooper;
import org.apache.heron.common.basics.SingletonRegistry;
import org.apache.heron.common.config.SystemConfig;
import org.apache.heron.common.network.HeronSocketOptions;
import org.apache.heron.common.utils.logging.ErrorReportLoggingHandler;
import org.apache.heron.common.utils.logging.LoggingHelper;
import org.apache.heron.spi.statefulstorage.IStatefulStorage;
import org.apache.heron.spi.statefulstorage.StatefulStorageException;

/**
 * Main class of CheckpointManager.
 */
public class CheckpointManager {
  private static final Logger LOG = Logger.getLogger(CheckpointManager.class.getName());

  // Pre-defined value
  private static final String CHECKPOINT_MANAGER_HOST = "127.0.0.1";

  // The looper drives CheckpointManagerServer
  private NIOLooper checkpointManagerServerLoop;
  private CheckpointManagerServer checkpointManagerServer;

  // Print usage options
  private static void usage(Options options) {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp("CheckpointManager", options);
  }

  // Construct all required command line options
  private static Options constructOptions() {
    Options options = new Options();

    Option topName = Option.builder("t")
        .desc("Name of the topology")
        .longOpt("topologyname")
        .hasArgs()
        .argName("topologyname")
        .required()
        .build();

    Option topId = Option.builder("i")
        .desc("Id of the topology")
        .longOpt("topologyid")
        .hasArgs()
        .argName("topologyid")
        .required()
        .build();

    Option ckptMgrId = Option.builder("c")
        .desc("Id of the checkpoint manager")
        .longOpt("ckptmgrid")
        .hasArgs()
        .argName("ckptmgrid")
        .required()
        .build();

    Option ckptMgrPort = Option.builder("p")
        .desc("Port of the checkpoint manager")
        .longOpt("ckptmgrport")
        .hasArgs()
        .argName("ckptmgrport")
        .required()
        .build();

    Option ckptMgrConfig = Option.builder("f")
        .desc("Config name of the checkpoint manager")
        .longOpt("ckptmgrconfig")
        .hasArgs()
        .argName("ckptmgrconfig")
        .required()
        .build();

    Option ckptMgrOverridenConfig = Option.builder("o")
        .desc("Config name of the checkpoint manager")
        .longOpt("ckptMgrOverridenConfig")
        .hasArgs()
        .argName("ckptMgrOverridenConfig")
        .build();

    Option heronInternalConfig = Option.builder("g")
        .desc("Heron internal config filename")
        .longOpt("heroninternalconfig")
        .hasArgs()
        .argName("heroninternalconfig")
        .required()
        .build();

    options.addOption(topName);
    options.addOption(topId);
    options.addOption(ckptMgrId);
    options.addOption(ckptMgrPort);
    options.addOption(ckptMgrConfig);
    options.addOption(ckptMgrOverridenConfig);
    options.addOption(heronInternalConfig);

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

  public CheckpointManager() {
  }

  public void init(
      String topologyName,
      String topologyId,
      String checkpointMgrId,
      String serverHost,
      int serverPort,
      SystemConfig systemConfig,
      CheckpointManagerConfig checkpointManagerConfig)
      throws IOException, CheckpointManagerException {

    LOG.info("Initializing CheckpointManager");
    checkpointManagerServerLoop = new NIOLooper();
    HeronSocketOptions serverSocketOptions =
        new HeronSocketOptions(
            checkpointManagerConfig.getWriteBatchSize(),
            checkpointManagerConfig.getWriteBatchTime(),
            checkpointManagerConfig.getReadBatchSize(),
            checkpointManagerConfig.getReadBatchTime(),
            checkpointManagerConfig.getSocketSendSize(),
            checkpointManagerConfig.getSocketReceiveSize(),
            checkpointManagerConfig.getMaximumPacketSize());

    // Setup the IStatefulStorage
    IStatefulStorage statefulStorage = setupStatefulStorage(topologyName, checkpointManagerConfig);

    // Start the server
    this.checkpointManagerServer = new CheckpointManagerServer(
        topologyName, topologyId, checkpointMgrId, statefulStorage,
        checkpointManagerServerLoop, serverHost, serverPort, serverSocketOptions);
  }

  public void startAndLoop() {
    // The CheckpointManagerServer would run in the main thread
    // We do it in the final step since it would await the main thread
    LOG.info("Starting CheckpointManager Server");
    checkpointManagerServer.start();
    checkpointManagerServerLoop.loop();
  }

  private static IStatefulStorage setupStatefulStorage(
      String topologyName,
      CheckpointManagerConfig checkpointManagerConfig) throws CheckpointManagerException {

    IStatefulStorage statefulStorage;
    String classname = checkpointManagerConfig.getStorageClassname();

    try {
      statefulStorage = (IStatefulStorage) Class.forName(classname).newInstance();
    } catch (InstantiationException e) {
      throw new CheckpointManagerException(classname + " class must have a no-arg constructor.", e);
    } catch (IllegalAccessException e) {
      throw new CheckpointManagerException(classname + " class must be concrete.", e);
    } catch (ClassNotFoundException e) {
      throw new CheckpointManagerException(classname + " class must be a class path.", e);
    }

    try {
      statefulStorage.init(topologyName,
          Collections.unmodifiableMap(checkpointManagerConfig.getStatefulStorageConfig()));
    } catch (StatefulStorageException e) {
      throw new CheckpointManagerException(classname + " init threw exception", e);
    }

    return statefulStorage;
  }

  public static void main(String[] args) throws IOException,
                                       ParseException, CheckpointManagerException {
    Slf4jUtils.installSLF4JBridge();
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
      throw new RuntimeException("Error parsing command line options ", e);
    }

    String topologyName = cmd.getOptionValue("topologyname");
    String topologyId = cmd.getOptionValue("topologyid");
    String ckptmgrId = cmd.getOptionValue("ckptmgrid");
    int port = Integer.parseInt(cmd.getOptionValue("ckptmgrport"));
    String stateConfigFilename = cmd.getOptionValue("ckptmgrconfig");
    String overriddenConfigFilename = cmd.getOptionValue("ckptMgrOverridenConfig");
    String heronInternalConfig = cmd.getOptionValue("heroninternalconfig");
    SystemConfig systemConfig = SystemConfig.newBuilder(true).putAll(heronInternalConfig,
                                                                     true).build();
    CheckpointManagerConfig ckptmgrConfig = CheckpointManagerConfig
        .newBuilder(true)
        .putAll(stateConfigFilename, true)
        .override(overriddenConfigFilename)
        .build();

    // Add the SystemConfig into SingletonRegistry
    SingletonRegistry.INSTANCE.registerSingleton(SystemConfig.HERON_SYSTEM_CONFIG, systemConfig);

    // Init the logging setting and redirect the stdout and stderr to logging
    // For now we just set the logging level as INFO; later we may accept an argument to set it.
    Level loggingLevel = Level.INFO;
    String loggingDir = systemConfig.getHeronLoggingDirectory();

    // Log to file and TManager
    LoggingHelper.loggerInit(loggingLevel, true);
    LoggingHelper.addLoggingHandler(
        LoggingHelper.getFileHandler(ckptmgrId, loggingDir, true,
            systemConfig.getHeronLoggingMaximumSize(),
            systemConfig.getHeronLoggingMaximumFiles()));
    LoggingHelper.addLoggingHandler(new ErrorReportLoggingHandler());

    // Start the actual things
    LOG.info(String.format("Starting topology %s with topologyId %s with "
            + "Checkpoint Manager Id %s, Port: %d.",
        topologyName, topologyId, ckptmgrId, port));

    LOG.info("System Config: " + systemConfig);
    LOG.info(() -> "Checkpoint Manager Config: " + ckptmgrConfig);

    CheckpointManager checkpointManager = new CheckpointManager();
    checkpointManager.init(topologyName, topologyId, ckptmgrId,
        CHECKPOINT_MANAGER_HOST, port, systemConfig, ckptmgrConfig);
    checkpointManager.startAndLoop();

    LOG.info("Loops terminated. Exiting.");
  }
}
