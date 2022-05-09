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
import org.apache.heron.spi.utils.TManagerException;
import org.apache.heron.spi.utils.TManagerUtils;

public class RuntimeManagerRunner {
  // Internal config keys. They are used internally only to pass command line arguments
  // into handlers.
  public static final String RUNTIME_MANAGER_COMPONENT_PARALLELISM_KEY =
      "RUNTIME_MANAGER_COMPONENT_PARALLELISM_KEY";
  public static final String RUNTIME_MANAGER_CONTAINER_NUMBER_KEY =
      "RUNTIME_MANAGER_CONTAINER_NUMBER_KEY";
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
      throws TManagerException, TopologyRuntimeManagementException,
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
  private void activateTopologyHandler(String topologyName) throws TManagerException {
    assert !potentialStaleExecutionData;
    NetworkUtils.TunnelConfig tunnelConfig =
        NetworkUtils.TunnelConfig.build(config, NetworkUtils.HeronSystem.SCHEDULER);
    TManagerUtils.transitionTopologyState(topologyName,
        TManagerUtils.TManagerCommand.ACTIVATE, Runtime.schedulerStateManagerAdaptor(runtime),
        TopologyAPI.TopologyState.PAUSED, TopologyAPI.TopologyState.RUNNING, tunnelConfig);
  }

  /**
   * Handler to deactivate a topology
   */
  private void deactivateTopologyHandler(String topologyName) throws TManagerException {
    assert !potentialStaleExecutionData;
    NetworkUtils.TunnelConfig tunnelConfig =
        NetworkUtils.TunnelConfig.build(config, NetworkUtils.HeronSystem.SCHEDULER);
    TManagerUtils.transitionTopologyState(topologyName,
        TManagerUtils.TManagerCommand.DEACTIVATE, Runtime.schedulerStateManagerAdaptor(runtime),
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
    // If we restart the container including TManager, wee need to clean TManagerLocation,
    // since when starting up, TManager expects no other existing TManager,
    // i.e. TManagerLocation does not exist
    if (containerId == -1 || containerId == 0) {
      // get the instance of state manager to clean state
      SchedulerStateManagerAdaptor stateManager = Runtime.schedulerStateManagerAdaptor(runtime);
      Boolean result = stateManager.deleteTManagerLocation(topologyName);
      if (result == null || !result) {
        throw new TopologyRuntimeManagementException(
            "Failed to clear TManager location. Check whether TManager set it correctly.");
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
    String newContainerNumber = updateConfig.getStringValue(RUNTIME_MANAGER_CONTAINER_NUMBER_KEY);
    String userRuntimeConfig = updateConfig.getStringValue(RUNTIME_MANAGER_RUNTIME_CONFIG_KEY);
    LOG.info("userRuntimeConfig " + userRuntimeConfig
        + "; newParallelism " + newParallelism
        + "; newContainerNumber " + newContainerNumber);

    // parallelism and runtime config can not be updated at the same time.
    if (((newParallelism != null && !newParallelism.isEmpty())
        || (newContainerNumber != null && !newContainerNumber.isEmpty()))
        && userRuntimeConfig != null && !userRuntimeConfig.isEmpty()) {
      throw new TopologyRuntimeManagementException(
          "parallelism or container number "
              + "and runtime config can not be updated at the same time.");
    }
    if (userRuntimeConfig != null && !userRuntimeConfig.isEmpty()) {
      // Update user runtime config if userRuntimeConfig parameter is available
      updateTopologyUserRuntimeConfig(topologyName, userRuntimeConfig);
    } else if (newContainerNumber != null && !newContainerNumber.isEmpty()) {
      // Update container count if newContainerNumber parameter is available
      updateTopologyContainerCount(topologyName, newContainerNumber, newParallelism);
    } else if (newParallelism != null && !newParallelism.isEmpty()) {
      // Update parallelism if newParallelism parameter is available
      updateTopologyComponentParallelism(topologyName, newParallelism);
    } else {
      throw new TopologyRuntimeManagementException("Missing arguments. Not taking action.");
    }
    // Clean the connection when we are done.
    LOG.fine("Scheduler updated topology successfully.");
  }

  private int getCurrentContainerNumber(String topologyName) {
    SchedulerStateManagerAdaptor manager = Runtime.schedulerStateManagerAdaptor(runtime);
    PackingPlans.PackingPlan currentPlan = manager.getPackingPlan(topologyName);
    PackingPlanProtoDeserializer deserializer = new PackingPlanProtoDeserializer();
    PackingPlan cPlan = deserializer.fromProto(currentPlan);
    return cPlan.getContainers().size();
  }

  void sendUpdateRequest(TopologyAPI.Topology topology,
                         Map<String, Integer> changeRequests,
                         PackingPlans.PackingPlan currentPlan,
                         PackingPlans.PackingPlan proposedPlan) {
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
  void updateTopologyComponentParallelism(String topologyName, String  newParallelism)
      throws TopologyRuntimeManagementException, PackingException, UpdateDryRunResponse {
    LOG.fine(String.format("updateTopologyHandler called for %s with %s",
        topologyName, newParallelism));
    Map<String, Integer> changeRequests = parseNewParallelismParam(newParallelism);

    SchedulerStateManagerAdaptor manager = Runtime.schedulerStateManagerAdaptor(runtime);
    TopologyAPI.Topology topology = manager.getTopology(topologyName);
    PackingPlans.PackingPlan currentPlan = manager.getPackingPlan(topologyName);

    if (!parallelismChangeDetected(currentPlan, changeRequests)) {
      throw new TopologyRuntimeManagementException(
          String.format("The component parallelism request (%s) is the same as the "
              + "current topology parallelism. Not taking action.", newParallelism));
    }

    PackingPlans.PackingPlan proposedPlan = buildNewPackingPlan(currentPlan, changeRequests,
        null, topology);

    sendUpdateRequest(topology, changeRequests, currentPlan, proposedPlan);
  }

  @VisibleForTesting
  void updateTopologyContainerCount(String topologyName,
                                    String newContainerNumber,
                                    String  newParallelism)
      throws PackingException, UpdateDryRunResponse {
    LOG.fine(String.format("updateTopologyHandler called for %s with %s and %s",
        topologyName, newContainerNumber, newParallelism));
    Integer containerNum = Integer.parseInt(newContainerNumber);
    Map<String, Integer> changeRequests = new HashMap<String, Integer>();
    if (newParallelism != null && !newParallelism.isEmpty()) {
      changeRequests = parseNewParallelismParam(newParallelism);
    }

    SchedulerStateManagerAdaptor manager = Runtime.schedulerStateManagerAdaptor(runtime);
    TopologyAPI.Topology topology = manager.getTopology(topologyName);
    PackingPlans.PackingPlan currentPlan = manager.getPackingPlan(topologyName);

    if (!containersNumChangeDetected(currentPlan, containerNum)
        && !parallelismChangeDetected(currentPlan, changeRequests)) {
      throw new TopologyRuntimeManagementException(
          String.format("Both component parallelism request and container number are the "
              + "same as in the running topology."));
    }

    PackingPlans.PackingPlan proposedPlan = buildNewPackingPlan(currentPlan, changeRequests,
        containerNum, topology);

    sendUpdateRequest(topology, changeRequests, currentPlan, proposedPlan);
  }

  @VisibleForTesting
  void updateTopologyUserRuntimeConfig(String topologyName, String userRuntimeConfig)
      throws TopologyRuntimeManagementException, TManagerException {
    String[] runtimeConfigs = parseUserRuntimeConfigParam(userRuntimeConfig);
    if (runtimeConfigs.length == 0) {
      throw new TopologyRuntimeManagementException("No user config is found");
    }

    // Send user runtime config to TManager
    NetworkUtils.TunnelConfig tunnelConfig =
        NetworkUtils.TunnelConfig.build(config, NetworkUtils.HeronSystem.SCHEDULER);
    TManagerUtils.sendRuntimeConfig(topologyName,
                                   TManagerUtils.TManagerCommand.RUNTIME_CONFIG_UPDATE,
                                   Runtime.schedulerStateManagerAdaptor(runtime),
                                   runtimeConfigs,
                                   tunnelConfig);
  }

  /**
   * Clean all states of a heron topology
   * 1. Topology def and ExecutionState are required to exist to delete
   * 2. TManagerLocation, SchedulerLocation and PhysicalPlan may not exist to delete
   */
  protected void cleanState(
      String topologyName,
      SchedulerStateManagerAdaptor statemgr) throws TopologyRuntimeManagementException {
    LOG.fine("Cleaning up topology state");

    Boolean result;

    result = statemgr.deleteTManagerLocation(topologyName);
    if (result == null || !result) {
      throw new TopologyRuntimeManagementException(
          "Failed to clear TManager location. Check whether TManager set it correctly.");
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
          "Failed to clear physical plan. Check whether TManager set it correctly.");
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
                                               Integer containerNum,
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
      PackingPlan packedPlan = null;
      if (containerNum == null) {
        packedPlan = packing.repack(currentPackingPlan, componentChanges);
      } else {
        packedPlan = packing.repack(currentPackingPlan, containerNum, componentChanges);
      }
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

  private static boolean containersNumChangeDetected(PackingPlans.PackingPlan currentProtoPlan,
                                                     int numContainers) {
    PackingPlanProtoDeserializer deserializer = new PackingPlanProtoDeserializer();
    PackingPlan currentPlan = deserializer.fromProto(currentProtoPlan);
    return currentPlan.getContainers().size() != numContainers;
  }

  private static boolean parallelismChangeDetected(PackingPlans.PackingPlan currentProtoPlan,
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
