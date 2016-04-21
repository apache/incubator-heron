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

package com.twitter.heron.scheduler.aurora;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.proto.system.ExecutionEnvironment;
import com.twitter.heron.proto.tmaster.TopologyMaster;
import com.twitter.heron.scheduler.util.ShellUtility;
import com.twitter.heron.scheduler.util.TopologyUtility;
import com.twitter.heron.spi.common.Constants;
import com.twitter.heron.spi.scheduler.SchedulerStateManagerAdaptor;
import com.twitter.heron.spi.scheduler.context.RuntimeManagerContext;

/**
 * Handles Runtime tasks like kill/restart/activate/deactivate for heron topology launched
 * in aurora.
 */
public class AuroraTopologyRuntimeManager implements IRuntimeManager {
  private static final Logger LOG = Logger.getLogger(AuroraTopologyRuntimeManager.class.getName());
  private static final String HERON_AURORA_BATCH_SIZE = "heron.aurora.batch.size";
  private static final String HERON_CONTROLLER_AURORA = "heron_controller.aurora";
  private RuntimeManagerContext context;
  private String topologyName;
  private String cluster;
  private String role;
  private String environ;

  private String getHeronControllerAuroraPath() {
    String configPath = context.getConfig().get(Constants.HERON_CONFIG_PATH).toString();
    return Paths.get(configPath, HERON_CONTROLLER_AURORA).toString();
  }

  protected boolean launchAuroraJob(Map<String, String> args) {
    ArrayList<String> auroraCmd = new ArrayList<>(Arrays.asList(
        "aurora", "job", "create", "--wait-until", "FINISHED"));
    for (String binding : args.keySet()) {
      auroraCmd.add("--bind");
      auroraCmd.add(String.format("%s=%s", binding, args.get(binding)));
    }
    auroraCmd.add(String.format("%s/%s/%s/%s",
        cluster, role, "devel", args.get("JOB_NAME")));
    auroraCmd.add(getHeronControllerAuroraPath());
    if (context.isVerbose()) {
      auroraCmd.add("--verbose");
    }
    if (0 != ShellUtility.runProcess(
        true, auroraCmd.toArray(new String[auroraCmd.size()]),
        new StringBuilder(), new StringBuilder())) {
      LOG.severe("Launching Aurora controller failed");
      return false;
    }
    return true;
  }

  protected boolean verifyState(boolean activate,
                                ExecutionEnvironment.ExecutionState executionState,
                                TopologyAPI.Topology topology,
                                TopologyMaster.TMasterLocation tMasterLocation) {
    // Verify if current state change can be performed.
    if (tMasterLocation == null || executionState == null || topology == null) {
      LOG.severe("Failed to read state. Check if topology is running");
      return false;
    }
    if (!executionState.getRole().equals(role)) {
      LOG.severe("Role not valid. Expected role: "
          + executionState.getRole());
      return false;
    }
    if (activate) {
      if (!topology.getState().equals(TopologyAPI.TopologyState.PAUSED)) {
        LOG.info("Topology not deactivated. State: " + topology.getState());
        return false;
      }
    } else {
      if (!topology.getState().equals(TopologyAPI.TopologyState.RUNNING)) {
        LOG.info("Topology not active. State: " + topology.getState());
        return false;
      }
    }
    return true;
  }

  protected boolean controlTopology(boolean activate) {
    TopologyAPI.Topology topology = null;
    ExecutionEnvironment.ExecutionState executionState = null;
    TopologyMaster.TMasterLocation tMasterLocation = null;
    try {
      SchedulerStateManagerAdaptor manager = context.getStateManagerAdaptor();
      topology = manager.getTopology().get();
      executionState = manager.getExecutionState().get();
      tMasterLocation = manager.getTMasterLocation().get();
    } catch (InterruptedException | ExecutionException e) {
      LOG.log(Level.SEVERE, "Fetch topology or execution state failed." +
          " Confirm that topology is running", e);
      return false;
    }

    if (!verifyState(activate, executionState, topology, tMasterLocation)) {
      LOG.severe("Check topology is running and you have permission to change it's state");
    }

    String controlJobName = "heron-controller-" + topology.getName();
    Map<String, String> args = new HashMap<>();
    args.put("HERON_CONTROLLER_BINARY", "heron-controller");
    args.put("TMASTER_HOSTNAME", tMasterLocation.getHost());
    args.put("TMASTER_CONTROLLER_PORT", "" + tMasterLocation.getControllerPort());
    args.put("TOPOLOGY_ID", topology.getId());
    args.put("JOB_NAME", controlJobName);
    args.put("CLUSTER", cluster);
    args.put("RUN_ROLE", role);
    args.put("HERON_PACKAGE",
        context.getProperty(Constants.HERON_RELEASE_PACKAGE_NAME, "heron-core-package"));
    args.put("RELEASE_ROLE",
        context.getProperty(Constants.HERON_RELEASE_PACKAGE_ROLE, "heron"));
    args.put("VERSION",
        context.getProperty(Constants.HERON_RELEASE_PACKAGE_VERSION, "live"));
    if (activate) {
      args.put("CONTROLLERCMD", "/activate");
    } else {
      args.put("CONTROLLERCMD", "/deactivate");
    }

    return launchAuroraJob(args);
  }

  protected boolean verifyControllerSuccess(boolean activate) {
    // Verify controller succeeded.
    TopologyAPI.Topology topology = null;
    try {
      SchedulerStateManagerAdaptor manager = context.getStateManagerAdaptor();
      topology = manager.getTopology().get();
    } catch (InterruptedException | ExecutionException e) {
      LOG.log(Level.SEVERE, "Fetch topology failed." +
          " Confirm that topology is running", e);
      return false;
    }
    if (activate) {
      LOG.info("Topology End state: " + topology.getState());
      return topology.getState().equals(TopologyAPI.TopologyState.RUNNING);
    } else {
      LOG.info("Topology End state: " + topology.getState());
      return topology.getState().equals(TopologyAPI.TopologyState.PAUSED);
    }
  }

  @Override
  public void initialize(RuntimeManagerContext context) {
    this.context = context;
    this.topologyName = context.getTopologyName();
    this.cluster = this.context.getPropertyWithException(Constants.CLUSTER);
    this.role = this.context.getPropertyWithException(Constants.ROLE);
    this.environ = this.context.getPropertyWithException(Constants.ENVIRON);
  }

  @Override
  public void close() {
  }

  @Override
  public boolean prepareRestart(int containerIndex) {
    return true;
  }

  @Override
  public boolean postRestart(int restartShardId) {
    TopologyAPI.Topology topology = null;
    try {
      topology = context.getStateManagerAdaptor().getTopology().get();
    } catch (InterruptedException | ExecutionException e) {
      LOG.log(Level.SEVERE, "Fetch topology failed. Check if topology is running", e);
    }
    String batchSize = context.getProperty(HERON_AURORA_BATCH_SIZE, (1 + TopologyUtility.getNumContainer(topology)) + "");

    ArrayList<String> auroraCmd = new ArrayList<>(Arrays.asList(
        "aurora", "job", "restart", "--batch-size", batchSize,
        restartShardId == -1 ? String.format("%s/%s/%s/%s", cluster, role, environ, topologyName)
            : String.format("%s/%s/%s/%s/%s", cluster, role, environ, topologyName, restartShardId)));
    if (context.isVerbose()) {
      auroraCmd.add("--verbose");
    }

    return 0 == ShellUtility.runProcess(context.isVerbose(), auroraCmd.toArray(new String[auroraCmd.size()]), null, null);
  }

  @Override
  public boolean prepareKill() {
    TopologyAPI.Topology topology = null;
    try {
      topology = context.getStateManagerAdaptor().getTopology().get();
    } catch (InterruptedException | ExecutionException e) {
      LOG.log(Level.SEVERE, "Fetch topology failed. Check if topology is running", e);
      return false;
    }
    String batchSize = context.getProperty(HERON_AURORA_BATCH_SIZE, (1 + TopologyUtility.getNumContainer(topology)) + "");

    ArrayList<String> auroraCmd = new ArrayList<>(Arrays.asList(
        "aurora", "job", "killall", "--batch-size", batchSize,
        String.format("%s/%s/%s/%s", cluster, role, environ, topologyName)));
    if (context.isVerbose()) {
      auroraCmd.add("--verbose");
    }

    LOG.info("auroraCmd=" + auroraCmd);

    if (0 == ShellUtility.runProcess(context.isVerbose(), auroraCmd.toArray(new String[auroraCmd.size()]), null, null)) {
      LOG.info("Killed topology");
      return true;
    } else {
      LOG.severe("Failed to kill topology.");
      return false;
    }
  }

  @Override
  public boolean postKill() {
    return true;
  }

  @Override
  public boolean prepareDeactivate() {
    return controlTopology(false) && verifyControllerSuccess(false);
  }

  @Override
  public boolean postDeactivate() {
    return true;
  }

  @Override
  public boolean prepareActivate() {
    return controlTopology(true) && verifyControllerSuccess(true);
  }

  @Override
  public boolean postActivate() {
    return true;
  }
}
