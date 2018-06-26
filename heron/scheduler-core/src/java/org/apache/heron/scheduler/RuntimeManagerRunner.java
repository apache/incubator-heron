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

package org.apache.heron.scheduler;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.annotations.VisibleForTesting;

import org.apache.heron.api.generated.TopologyAPI;
import org.apache.heron.common.basics.SysUtils;
import org.apache.heron.proto.scheduler.Scheduler;
import org.apache.heron.proto.system.PackingPlans;
import org.apache.heron.scheduler.client.ISchedulerClient;
import org.apache.heron.scheduler.dryrun.UpdateDryRunResponse;
import org.apache.heron.scheduler.utils.Runtime;
import org.apache.heron.spi.common.Config;
import org.apache.heron.spi.common.Context;
import org.apache.heron.spi.packing.IRepacking;
import org.apache.heron.spi.packing.PackingException;
import org.apache.heron.spi.packing.PackingPlan;
import org.apache.heron.spi.packing.PackingPlanProtoDeserializer;
import org.apache.heron.spi.packing.PackingPlanProtoSerializer;
import org.apache.heron.spi.statemgr.SchedulerStateManagerAdaptor;
import org.apache.heron.spi.utils.NetworkUtils;
import org.apache.heron.spi.utils.ReflectionUtils;
import org.apache.heron.spi.utils.TMasterException;
import org.apache.heron.spi.utils.TMasterUtils;

public class RuntimeManagerRunner {
  // Internal config keys. They are used internally only to pass command line arguments
  // into handlers.
  public static final String RUNTIME_MANAGER_COMPONENT_PARALLELISM_KEY =
      "RUNTIME_MANAGER_COMPONENT_PARALLELISM_KEY";
  public static final String RUNTIME_MANAGER_RUNTIME_CONFIG_KEY =
      "RUNTIME_MANAGER_RUNTIME_CONFIG_KEY";

  private static final Logger LOG = Logger.getLogger(RuntimeManagerRunner.class.getName());
  private final Config config;
  private final Config runtime;
  private final Command command;
  private final ISchedulerClient schedulerClient;
  private final boolean potentialStaleExecutionData;

  public RuntimeManagerRunner(Config config,
                              Config runtime,
                              Command command,
                              ISchedulerClient schedulerClient,
                              boolean potentialStaleExecutionData) {

    this.config = config;
    this.runtime = runtime;
    this.command = command;
    this.potentialStaleExecutionData = potentialStaleExecutionData;

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
        updateTopologyHandler(topologyName, config);
        break;
      default:
        LOG.severe("Unknown command for topology: " + command);
    }
  }

  /**
   * Handler to activate a topology
   */
  private void activateTopologyHandler(String topologyName) throws TMasterException {
    assert !potentialStaleExecutionData;
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
    assert !potentialStaleExecutionData;
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
    assert !potentialStaleExecutionData;
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
          String.format("Failed to kill topology '%s' with scheduler, "
              + "please re-try the kill command later", topologyName));
    }

    // clean up the state of the topology in state manager
    cleanState(topologyName, Runtime.schedulerStateManagerAdaptor(runtime));

    if (potentialStaleExecutionData) {
      LOG.warning(String.format("Topology %s does not exist. Cleaned up potential stale state.",
          topologyName));
    } else {
      LOG.fine(String.format("Scheduler killed topology %s successfully.", topologyName));
    }
  }

  /**
   * Handler to update a topology
   */
  @VisibleForTesting
  void updateTopologyHandler(String topologyName, Config updateConfig)
      throws TopologyRuntimeManagementException, PackingException, UpdateDryRunResponse {
    assert !potentialStaleExecutionData;
    String newParallelism = updateConfig.getStringValue(RUNTIME_MANAGER_COMPONENT_PARALLELISM_KEY);
    String userRuntimeConfig = updateConfig.getStringValue(RUNTIME_MANAGER_RUNTIME_CONFIG_KEY);

    // parallelism and runtime config can not be updated at the same time.
    if (newParallelism != null && !newParallelism.isEmpty()
        && userRuntimeConfig != null && !userRuntimeConfig.isEmpty()) {
      throw new TopologyRuntimeManagementException(
          "parallelism and runtime config can not be updated at the same time.");
    }

    if (newParallelism != null && !newParallelism.isEmpty()) {
      // Update parallelism if newParallelism parameter is available
      updateTopologyComponentParallelism(topologyName, newParallelism);
    } else if (userRuntimeConfig != null && !userRuntimeConfig.isEmpty()) {
      // Update user runtime config if userRuntimeConfig parameter is available
      updateTopologyUserRuntimeConfig(topologyName, userRuntimeConfig);
    } else {
      throw new TopologyRuntimeManagementException("Missing arguments. Not taking action.");
    }
    // Clean the connection when we are done.
    LOG.fine("Scheduler updated topology successfully.");
  }

  @VisibleForTesting
  void updateTopologyComponentParallelism(String topologyName, String  newParallelism)
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
      throw new TopologyRuntimeManagementException(
          "Failed to update topology with Scheduler, updateTopologyRequest="
              + updateTopologyRequest + "The topology can be in a strange stage. "
                  + "Please check carefully or redeploy the topology !!");
    }
  }

  @VisibleForTesting
  void updateTopologyUserRuntimeConfig(String topologyName, String userRuntimeConfig)
      throws TopologyRuntimeManagementException, TMasterException {
    String[] runtimeConfigs = parseUserRuntimeConfigParam(userRuntimeConfig);
    if (runtimeConfigs.length == 0) {
      throw new TopologyRuntimeManagementException("No user config is found");
    }

    // Send user runtime config to TMaster
    NetworkUtils.TunnelConfig tunnelConfig =
        NetworkUtils.TunnelConfig.build(config, NetworkUtils.HeronSystem.SCHEDULER);
    TMasterUtils.sendRuntimeConfig(topologyName,
                                   TMasterUtils.TMasterCommand.RUNTIME_CONFIG_UPDATE,
                                   Runtime.schedulerStateManagerAdaptor(runtime),
                                   runtimeConfigs,
                                   tunnelConfig);
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

    result = statemgr.deleteTMasterLocation(topologyName);
    if (result == null || !result) {
      throw new TopologyRuntimeManagementException(
          "Failed to clear TMaster location. Check whether TMaster set it correctly.");
    }

    result = statemgr.deleteMetricsCacheLocation(topologyName);
    if (result == null || !result) {
      throw new TopologyRuntimeManagementException(
          "Failed to clear MetricsCache location. Check whether MetricsCache set it correctly.");
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
  Map<String, Integer> parseNewParallelismParam(String newParallelism)
      throws IllegalArgumentException {
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

  @VisibleForTesting
  String[] parseUserRuntimeConfigParam(String userRuntimeConfig)
      throws IllegalArgumentException {
    // Regex for "[component_name:]<config>:<value>[,[component_name>:]<config>:<value>]"
    final Pattern pattern = Pattern.compile("^([\\w\\.-]+:){1,2}[\\w\\.-]+$");
    if (userRuntimeConfig.isEmpty()) {
      return new String[0];
    }

    String[] configs = userRuntimeConfig.split(",");
    for (String configValuePair: configs) {
      Matcher matcher = pattern.matcher(configValuePair);
      if (!matcher.find()) {
        throw new IllegalArgumentException("Invalid user config found. Expected: "
            + "[component_name:]<config>:<value>"
            + "[,[component_name>:]<config>:<value>], Found: " + userRuntimeConfig);
      }
    }

    return configs;
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
