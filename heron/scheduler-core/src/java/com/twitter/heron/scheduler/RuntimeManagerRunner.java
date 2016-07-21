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
import com.twitter.heron.scheduler.client.ISchedulerClient;
import com.twitter.heron.spi.common.Command;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Context;
import com.twitter.heron.spi.statemgr.SchedulerStateManagerAdaptor;
import com.twitter.heron.spi.utils.Runtime;
import com.twitter.heron.spi.utils.TMasterUtils;

public class RuntimeManagerRunner implements Callable<Boolean> {
  private static final Logger LOG = Logger.getLogger(RuntimeManagerRunner.class.getName());

  private final Config config;
  private final Config runtime;
  private final Command command;
  private final ISchedulerClient schedulerClient;

  public RuntimeManagerRunner(Config config, Config runtime,
                              Command command, ISchedulerClient schedulerClient) {

    this.config = config;
    this.runtime = runtime;
    this.command = command;

    this.schedulerClient = schedulerClient;
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
    return TMasterUtils.transitionTopologyState(
        topologyName, "activate", Runtime.schedulerStateManagerAdaptor(runtime),
        TopologyAPI.TopologyState.PAUSED, TopologyAPI.TopologyState.RUNNING);
  }

  /**
   * Handler to deactivate a topology
   */
  protected boolean deactivateTopologyHandler(String topologyName) {
    return TMasterUtils.transitionTopologyState(
        topologyName, "deactivate", Runtime.schedulerStateManagerAdaptor(runtime),
        TopologyAPI.TopologyState.RUNNING, TopologyAPI.TopologyState.PAUSED);
  }

  /**
   * Handler to restart a topology
   */
  protected boolean restartTopologyHandler(String topologyName) {
    Integer containerId = Context.topologyContainerId(config);
    Scheduler.RestartTopologyRequest restartTopologyRequest =
        Scheduler.RestartTopologyRequest.newBuilder()
            .setTopologyName(topologyName)
            .setContainerIndex(containerId)
            .build();

    // If we restart the container including TMaster, wee need to clean TMasterLocation,
    // since when starting up, TMaster expects no other existing TMaster,
    // i.e. TMasterLocation does not exist
    if (containerId == -1 || containerId == 0) {
      // get the instance of state manager to clean state
      SchedulerStateManagerAdaptor stateManager = Runtime.schedulerStateManagerAdaptor(runtime);

      Boolean result = stateManager.deleteTMasterLocation(topologyName);
      if (result == null || !result) {
        // We would not return false since it is possible that TMaster didn't write physical plan
        LOG.severe("Failed to clear TMaster location. Check whether TMaster set it correctly.");
        return false;
      }
    }

    if (!schedulerClient.restartTopology(restartTopologyRequest)) {
      LOG.log(Level.SEVERE, "Failed to restart with Scheduler: ");
      return false;
    }
    // Clean the connection when we are done.
    LOG.fine("Scheduler restarted topology successfully.");
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
    SchedulerStateManagerAdaptor statemgr = Runtime.schedulerStateManagerAdaptor(runtime);
    if (!statemgr.cleanState(topologyName)) {
      LOG.severe("Failed to clean topology state");
      return false;
    }

    // Clean the connection when we are done.
    LOG.fine("Scheduler killed topology successfully.");
    return true;
  }
}
