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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

import com.google.common.annotations.VisibleForTesting;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.common.basics.SysUtils;
import com.twitter.heron.proto.scheduler.Scheduler;
import com.twitter.heron.proto.system.PackingPlans;
import com.twitter.heron.scheduler.client.ISchedulerClient;
import com.twitter.heron.scheduler.dryrun.UpdateDryRunResponse;
import com.twitter.heron.scheduler.utils.Runtime;
import com.twitter.heron.spi.common.Command;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Context;
import com.twitter.heron.spi.packing.IRepacking;
import com.twitter.heron.spi.packing.PackingException;
import com.twitter.heron.spi.packing.PackingPlan;
import com.twitter.heron.spi.packing.PackingPlanProtoDeserializer;
import com.twitter.heron.spi.packing.PackingPlanProtoSerializer;
import com.twitter.heron.spi.statemgr.SchedulerStateManagerAdaptor;
import com.twitter.heron.spi.utils.NetworkUtils;
import com.twitter.heron.spi.utils.ReflectionUtils;
import com.twitter.heron.spi.utils.TMasterException;
import com.twitter.heron.spi.utils.TMasterUtils;

public class RuntimeManagerRunner {
  static final String NEW_COMPONENT_PARALLELISM_KEY = "NEW_COMPONENT_PARALLELISM";
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

  public void call()
      throws TMasterException, TopologyRuntimeManagementException,
      PackingException, UpdateDryRunResponse {
    // execute the appropriate command
    String topologyName = Context.topologyName(config);
    switch (command) {
      case ACTIVATE:
        activateTopologyHandler(topologyName);
        break;
      case DEACTIVATE:
        deactivateTopologyHandler(topologyName);
        break;
      case RESTART:
        restartTopologyHandler(topologyName);
        break;
      case KILL:
        killTopologyHandler(topologyName);
        break;
      case UPDATE:
        updateTopologyHandler(topologyName,
            config.getStringValue(NEW_COMPONENT_PARALLELISM_KEY));
        break;
      default:
        LOG.severe("Unknown command for topology: " + command);
    }
  }

  /**
   * Handler to activate a topology
   */
  private void activateTopologyHandler(String topologyName) throws TMasterException {
    NetworkUtils.TunnelConfig tunnelConfig =
        NetworkUtils.TunnelConfig.build(config, NetworkUtils.HeronSystem.SCHEDULER);
    TMasterUtils.transitionTopologyState(topologyName,
        TMasterUtils.TMasterCommand.ACTIVATE, Runtime.schedulerStateManagerAdaptor(runtime),
        TopologyAPI.TopologyState.PAUSED, TopologyAPI.TopologyState.RUNNING, tunnelConfig);
  }

  /**
   * Handler to deactivate a topology
   */
  private void deactivateTopologyHandler(String topologyName) throws TMasterException {
    NetworkUtils.TunnelConfig tunnelConfig =
        NetworkUtils.TunnelConfig.build(config, NetworkUtils.HeronSystem.SCHEDULER);
    TMasterUtils.transitionTopologyState(topologyName,
        TMasterUtils.TMasterCommand.DEACTIVATE, Runtime.schedulerStateManagerAdaptor(runtime),
        TopologyAPI.TopologyState.RUNNING, TopologyAPI.TopologyState.PAUSED, tunnelConfig);
  }

  /**
   * Handler to restart a topology
   */
  @VisibleForTesting
  void restartTopologyHandler(String topologyName) throws TopologyRuntimeManagementException {
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
        throw new TopologyRuntimeManagementException(
            "Failed to clear TMaster location. Check whether TMaster set it correctly.");
      }
    }

    if (!schedulerClient.restartTopology(restartTopologyRequest)) {
      throw new TopologyRuntimeManagementException(String.format(
          "Failed to restart topology '%s'", topologyName));
    }
    // Clean the connection when we are done.
    LOG.fine("Scheduler restarted topology successfully.");
  }

  /**
   * Handler to kill a topology
   */
  @VisibleForTesting
  void killTopologyHandler(String topologyName) throws TopologyRuntimeManagementException {
    Scheduler.KillTopologyRequest killTopologyRequest = Scheduler.KillTopologyRequest.newBuilder()
        .setTopologyName(topologyName).build();

    if (!schedulerClient.killTopology(killTopologyRequest)) {
      throw new TopologyRuntimeManagementException(
          String.format("Failed to kill topology '%s' with scheduler", topologyName));
    }

    // clean up the state of the topology in state manager
    cleanState(topologyName, Runtime.schedulerStateManagerAdaptor(runtime));

    // Clean the connection when we are done.
    LOG.fine("Scheduler killed topology successfully.");
  }

  /**
   * Handler to update a topology
   */
  @VisibleForTesting
  void updateTopologyHandler(String topologyName, String newParallelism)
      throws TopologyRuntimeManagementException, PackingException, UpdateDryRunResponse {
    LOG.fine(String.format("updateTopologyHandler called for %s with %s",
        topologyName, newParallelism));
    SchedulerStateManagerAdaptor manager = Runtime.schedulerStateManagerAdaptor(runtime);
    TopologyAPI.Topology topology = manager.getTopology(topologyName);
    Map<String, Integer> changeRequests = parseNewParallelismParam(newParallelism);
    PackingPlans.PackingPlan currentPlan = manager.getPackingPlan(topologyName);

    if (!changeDetected(currentPlan, changeRequests)) {
      throw new TopologyRuntimeManagementException(
          String.format("The component parallelism request (%s) is the same as the "
          + "current topology parallelism. Not taking action.", newParallelism));
    }

    PackingPlans.PackingPlan proposedPlan = buildNewPackingPlan(currentPlan, changeRequests,
        topology);

    if (Context.dryRun(config)) {
      PackingPlanProtoDeserializer deserializer = new PackingPlanProtoDeserializer();
      PackingPlan oldPlan = deserializer.fromProto(currentPlan);
      PackingPlan newPlan = deserializer.fromProto(proposedPlan);
      throw new UpdateDryRunResponse(topology, config, newPlan, oldPlan, changeRequests);
    }

    Scheduler.UpdateTopologyRequest updateTopologyRequest =
        Scheduler.UpdateTopologyRequest.newBuilder()
            .setCurrentPackingPlan(currentPlan)
            .setProposedPackingPlan(proposedPlan)
            .build();

    LOG.fine("Sending Updating topology request: " + updateTopologyRequest);
    if (!schedulerClient.updateTopology(updateTopologyRequest)) {
      throw new TopologyRuntimeManagementException(String.format(
          "Failed to update topology with Scheduler, updateTopologyRequest="
          + updateTopologyRequest));
    }

    // Clean the connection when we are done.
    LOG.fine("Scheduler updated topology successfully.");
  }

  /**
   * Clean all states of a heron topology
   * 1. Topology def and ExecutionState are required to exist to delete
   * 2. TMasterLocation, SchedulerLocation and PhysicalPlan may not exist to delete
   */
  protected void cleanState(
      String topologyName,
      SchedulerStateManagerAdaptor statemgr) throws TopologyRuntimeManagementException {
    LOG.fine("Cleaning up topology state");

    Boolean result;

    // It is possible that TMasterLocation, PackingPlan, PhysicalPlan and SchedulerLocation are not
    // set. Just log but don't consider it a failure and don't return false
    result = statemgr.deleteTMasterLocation(topologyName);
    if (result == null || !result) {
      throw new TopologyRuntimeManagementException(
          "Failed to clear TMaster location. Check whether TMaster set it correctly.");
    }

    result = statemgr.deletePackingPlan(topologyName);
    if (result == null || !result) {
      throw new TopologyRuntimeManagementException(
          "Failed to clear packing plan. Check whether Launcher set it correctly.");
    }

    result = statemgr.deletePhysicalPlan(topologyName);
    if (result == null || !result) {
      throw new TopologyRuntimeManagementException(
        "Failed to clear physical plan. Check whether TMaster set it correctly.");
    }

    result = statemgr.deleteSchedulerLocation(topologyName);
    if (result == null || !result) {
      throw new TopologyRuntimeManagementException(
        "Failed to clear scheduler location. Check whether Scheduler set it correctly.");
    }

    result = statemgr.deleteLocks(topologyName);
    if (result == null || !result) {
      throw new TopologyRuntimeManagementException(
        "Failed to delete locks. It's possible that the topology never created any.");
    }

    result = statemgr.deleteExecutionState(topologyName);
    if (result == null || !result) {
      throw new TopologyRuntimeManagementException(
        "Failed to clear execution state");
    }

    // Set topology def at last since we determine whether a topology is running
    // by checking the existence of topology def
    result = statemgr.deleteTopology(topologyName);
    if (result == null || !result) {
      throw new TopologyRuntimeManagementException(
        "Failed to clear topology definition");
    }

    LOG.fine("Cleaned up topology state");
  }

  @VisibleForTesting
  PackingPlans.PackingPlan buildNewPackingPlan(PackingPlans.PackingPlan currentProtoPlan,
                                               Map<String, Integer> changeRequests,
                                               TopologyAPI.Topology topology)
      throws PackingException {
    PackingPlanProtoDeserializer deserializer = new PackingPlanProtoDeserializer();
    PackingPlanProtoSerializer serializer = new PackingPlanProtoSerializer();
    PackingPlan currentPackingPlan = deserializer.fromProto(currentProtoPlan);

    Map<String, Integer> componentCounts = currentPackingPlan.getComponentCounts();
    Map<String, Integer> componentChanges = parallelismDelta(componentCounts, changeRequests);

    // Create an instance of the packing class
    String repackingClass = Context.repackingClass(config);
    IRepacking packing;
    try {
      // create an instance of the packing class
      packing = ReflectionUtils.newInstance(repackingClass);
    } catch (IllegalAccessException | InstantiationException | ClassNotFoundException e) {
      throw new IllegalArgumentException(
          "Failed to instantiate packing instance: " + repackingClass, e);
    }

    LOG.info("Updating packing plan using " + repackingClass);
    try {
      packing.initialize(config, topology);
      PackingPlan packedPlan = packing.repack(currentPackingPlan, componentChanges);
      return serializer.toProto(packedPlan);
    } finally {
      SysUtils.closeIgnoringExceptions(packing);
    }
  }

  @VisibleForTesting
  Map<String, Integer> parallelismDelta(Map<String, Integer> componentCounts,
                                        Map<String, Integer> changeRequests) {
    Map<String, Integer> componentDeltas = new HashMap<>();
    for (String component : changeRequests.keySet()) {
      if (!componentCounts.containsKey(component)) {
        throw new IllegalArgumentException(String.format(
            "Invalid component name in update request: %s. Valid components include: %s",
            component, Arrays.toString(
                componentCounts.keySet().toArray(new String[componentCounts.keySet().size()]))));
      }
      Integer newValue = changeRequests.get(component);
      Integer delta = newValue - componentCounts.get(component);
      if (delta != 0) {
        componentDeltas.put(component, delta);
      }
    }
    return componentDeltas;
  }

  @VisibleForTesting
  Map<String, Integer> parseNewParallelismParam(String newParallelism) {
    Map<String, Integer> changes = new HashMap<>();
    try {
      for (String componentValuePair : newParallelism.split(",")) {
        if (componentValuePair.length() == 0) {
          continue;
        }
        String[] kvp = componentValuePair.split(":", 2);
        changes.put(kvp[0], Integer.parseInt(kvp[1]));
      }
    } catch (NumberFormatException | ArrayIndexOutOfBoundsException e) {
      throw new IllegalArgumentException("Invalid parallelism parameter found. Expected: "
          + "<component>:<parallelism>[,<component>:<parallelism>], Found: " + newParallelism);
    }
    return changes;
  }

  private static boolean changeDetected(PackingPlans.PackingPlan currentProtoPlan,
                                 Map<String, Integer> changeRequests) {
    PackingPlanProtoDeserializer deserializer = new PackingPlanProtoDeserializer();
    PackingPlan currentPlan = deserializer.fromProto(currentProtoPlan);
    for (String component : changeRequests.keySet()) {
      if (changeRequests.get(component) != currentPlan.getComponentCounts().get(component)) {
        return true;
      }
    }
    return false;
  }
}
