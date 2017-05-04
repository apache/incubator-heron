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

package com.twitter.heron.ckptmgr;

import java.io.IOException;
import java.util.Collections;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.twitter.heron.common.basics.Constants;
import com.twitter.heron.common.basics.NIOLooper;
import com.twitter.heron.common.basics.SingletonRegistry;
import com.twitter.heron.common.config.SystemConfig;
import com.twitter.heron.common.network.HeronSocketOptions;
import com.twitter.heron.common.utils.logging.ErrorReportLoggingHandler;
import com.twitter.heron.common.utils.logging.LoggingHelper;
import com.twitter.heron.spi.statefulstorage.IStatefulStorage;

public class CheckpointManager {
  private static final Logger LOG = Logger.getLogger(CheckpointManager.class.getName());

  // Pre-defined value
  private static final String CHECKPOINT_MANAGER_HOST = "127.0.0.1";

  // The looper drives CheckpointManagerServer
  private final NIOLooper checkpointManagerServerLoop;
  private final CheckpointManagerServer checkpointManagerServer;

  public CheckpointManager(
      String topologyName, String topologyId, String checkpointMgrId,
      String serverHost, int serverPort,
      SystemConfig systemConfig, CheckpointManagerConfig checkpointManagerConfig)
      throws IOException {

    this.checkpointManagerServerLoop = new NIOLooper();

    // TODO(mfu): Read from config
    HeronSocketOptions serverSocketOptions =
        new HeronSocketOptions(32768,
            16,
            32768,
            16,
            655360,
            655360);

    // Setup the IStatefulStorage
    // TODO(mfu): This should be done in an executor driven by another thread, kind of async
    IStatefulStorage checkpointsBackend;
    String classname =
        (String) checkpointManagerConfig.get(CheckpointManagerConfig.CONFIG_KEY_CLASSNAME);
    try {
      checkpointsBackend = (IStatefulStorage) Class.forName(classname).newInstance();
    } catch (InstantiationException e) {
      throw new RuntimeException(e + " class must have a no-arg constructor.");
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e + " class must be concrete.");
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e + " class must be a class path.");
    }
    checkpointsBackend.init(
        Collections.unmodifiableMap(checkpointManagerConfig.getBackendConfig()));

    // Start the server
    this.checkpointManagerServer = new CheckpointManagerServer(
        topologyName, topologyId, checkpointMgrId, checkpointsBackend,
        checkpointManagerServerLoop, serverHost, serverPort, serverSocketOptions);
  }

  public void start() {
    // The CheckpointManagerServer would run in the main thread
    // We do it in the final step since it would await the main thread
    LOG.info("Starting CheckpointManager Server");
    checkpointManagerServer.start();
    checkpointManagerServerLoop.loop();
  }

  public static void main(String[] args) throws IOException {
    if (args.length != 9) {
      throw new RuntimeException(
          "Invalid arguments; Usage: java com.twitter.heron.ckptmgr.CheckpointManager "
              + "<topname> <topid> <ckptmgr_id> <myport> <stateful_config_filename> "
              + "<cluster> <role> <environ> <heron_internals_config_filename>");
    }

    // Handling these arguments

    String topologyName = args[0];
    String topologyId = args[1];
    String ckptmgrId = args[2];
    int port = Integer.parseInt(args[3]);
    String stateConfigFilename = args[4];
    // TODO(mfu): Remove following 3 arguments?
    String cluster = args[5];
    String role = args[6];
    String environ = args[7];

    SystemConfig systemConfig = SystemConfig.newBuilder(true).putAll(args[8], true).build();
    CheckpointManagerConfig ckptmgrConfig = new CheckpointManagerConfig(stateConfigFilename);

    // Add the SystemConfig into SingletonRegistry
    SingletonRegistry.INSTANCE.registerSingleton(SystemConfig.HERON_SYSTEM_CONFIG, systemConfig);

    // Init the logging setting and redirect the stdout and stderr to logging
    // For now we just set the logging level as INFO; later we may accept an argument to set it.
    Level loggingLevel = Level.INFO;
    String loggingDir = systemConfig.getHeronLoggingDirectory();

    // Log to file and TMaster
    LoggingHelper.loggerInit(loggingLevel, true);
    LoggingHelper.addLoggingHandler(
        LoggingHelper.getFileHandler(ckptmgrId, loggingDir, true,
            systemConfig.getHeronLoggingMaximumSizeMb() * Constants.MB_TO_BYTES,
            systemConfig.getHeronLoggingMaximumFiles()));
    LoggingHelper.addLoggingHandler(new ErrorReportLoggingHandler());

    // Start the actual things
    LOG.info(String.format("Starting for topology %s with topologyId %s with "
            + "Id %s, Port: %d.",
        topologyName, topologyId, ckptmgrId, port));

    LOG.info("System Config: " + systemConfig);

    CheckpointManager checkpointManager =
        new CheckpointManager(topologyName, topologyId, ckptmgrId,
            CHECKPOINT_MANAGER_HOST, port, systemConfig, ckptmgrConfig);
    checkpointManager.start();

    LOG.info("Loops terminated. Exiting.");
  }
}
