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

package com.twitter.heron.scheduler;

import java.util.concurrent.Callable;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.proto.scheduler.Scheduler;
import com.twitter.heron.scheduler.client.HttpServiceSchedulerClient;
import com.twitter.heron.scheduler.client.ISchedulerClient;
import com.twitter.heron.scheduler.client.LibrarySchedulerClient;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Context;
import com.twitter.heron.spi.scheduler.Command;
import com.twitter.heron.spi.scheduler.IScheduler;
import com.twitter.heron.spi.statemgr.SchedulerStateManagerAdaptor;
import com.twitter.heron.spi.utils.Runtime;
import com.twitter.heron.spi.utils.SchedulerUtils;
import com.twitter.heron.spi.utils.TMasterUtils;

public class RuntimeManagerRunner implements Callable<Boolean> {
  private static final Logger LOG = Logger.getLogger(RuntimeManagerRunner.class.getName());

  private final Config config;
  private final Config runtime;
  private final Command command;
  private final ISchedulerClient schedulerClient;

  public RuntimeManagerRunner(Config config, Config runtime,
                              Command command) throws
      ClassNotFoundException, InstantiationException, IllegalAccessException {

    this.config = config;
    this.runtime = runtime;
    this.command = command;

    this.schedulerClient = getSchedulerClient();
    if (this.schedulerClient == null) {
      throw new RuntimeException("Failed to initialize scheduler client");
    }
  }

  @Override
  public Boolean call() {
    // execute the appropriate command
    String topologyName = Context.topologyName(config);
    boolean result = false;
    switch (command) {
      case ACTIVATE:
        result = activateTopologyHandler(topologyName);
        break;
      case DEACTIVATE:
        result = deactivateTopologyHandler(topologyName);
        break;
      case RESTART:
        result = restartTopologyHandler(topologyName);
        break;
      case KILL:
        result = killTopologyHandler(topologyName);
        break;
      default:
        LOG.severe("Unknown command for topology: " + command);
    }

    return result;
  }

  /**
   * Handler to activate a topology
   */
  protected boolean activateTopologyHandler(String topologyName) {
    return TMasterUtils.controlTopologyState(
        topologyName, "activate", Runtime.schedulerStateManagerAdaptor(runtime),
        TopologyAPI.TopologyState.PAUSED, TopologyAPI.TopologyState.RUNNING);
  }

  /**
   * Handler to deactivate a topology
   */
  protected boolean deactivateTopologyHandler(String topologyName) {
    return TMasterUtils.controlTopologyState(
        topologyName, "deactivate", Runtime.schedulerStateManagerAdaptor(runtime),
        TopologyAPI.TopologyState.RUNNING, TopologyAPI.TopologyState.PAUSED);
  }

  /**
   * Handler to restart a topology
   */
  protected boolean restartTopologyHandler(String topologyName) {
    Integer containerId = Context.topologyContainerId(config);
    Scheduler.RestartTopologyRequest restartTopologyRequest = Scheduler.RestartTopologyRequest.newBuilder()
        .setTopologyName(topologyName)
        .setContainerIndex(containerId)
        .build();

    if (!schedulerClient.restartTopology(restartTopologyRequest)) {
      LOG.log(Level.SEVERE, "Failed to restart with Scheduler: ");
      return false;
    }
    // Clean the connection when we are done.
    LOG.info("Scheduler restarted topology successfully.");
    return true;
  }

  /**
   * Handler to kill a topology
   */
  protected boolean killTopologyHandler(String topologyName) {
    Scheduler.KillTopologyRequest killTopologyRequest = Scheduler.KillTopologyRequest.newBuilder()
        .setTopologyName(topologyName).build();

    if (!schedulerClient.killTopology(killTopologyRequest)) {
      LOG.log(Level.SEVERE, "Failed to kill with Scheduler.");
      return false;
    }

    // clean up the state of the topology in state manager
    if (!SchedulerUtils.cleanState(topologyName, Runtime.schedulerStateManagerAdaptor(runtime))) {
      LOG.severe("Failed to clean state");
      return false;
    }

    // Clean the connection when we are done.
    LOG.info("Scheduler killed topology successfully.");
    return true;
  }

  protected Scheduler.SchedulerLocation getSchedulerLocation() {
    // TODO(mfu): Add Proxy support, rather than to connect scheduler directly

    // get the instance of the state manager
    SchedulerStateManagerAdaptor statemgr = com.twitter.heron.spi.utils.Runtime.schedulerStateManagerAdaptor(runtime);

    // fetch scheduler location from state manager
    LOG.log(Level.INFO, "Fetching scheduler location from state manager to {0} topology", command);

    Scheduler.SchedulerLocation schedulerLocation =
        statemgr.getSchedulerLocation(Runtime.topologyName(runtime));

    return schedulerLocation;
  }

  // TODO(mfu): Make it into Factory pattern if needed
  protected ISchedulerClient getSchedulerClient()
      throws ClassNotFoundException, IllegalAccessException, InstantiationException {
    ISchedulerClient schedulerClient;

    if (Context.schedulerService(config)) {
      Scheduler.SchedulerLocation schedulerLocation = getSchedulerLocation();
      if (schedulerLocation == null) {
        LOG.log(Level.SEVERE, "Failed to get scheduler location to {0} topology", command);
        return null;
      }

      schedulerClient = new HttpServiceSchedulerClient(config, runtime, schedulerLocation);
    } else {
      // create an instance of scheduler
      final String schedulerClass = Context.schedulerClass(config);
      final IScheduler scheduler = (IScheduler) Class.forName(schedulerClass).newInstance();
      schedulerClient = new LibrarySchedulerClient(config, runtime, scheduler);
    }

    return schedulerClient;
  }
}
