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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.proto.scheduler.Scheduler;
import com.twitter.heron.proto.system.PackingPlans;
import com.twitter.heron.scheduler.client.ISchedulerClient;
import com.twitter.heron.spi.common.Command;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Context;
import com.twitter.heron.spi.statemgr.SchedulerStateManagerAdaptor;
import com.twitter.heron.spi.utils.Runtime;
import com.twitter.heron.spi.utils.TMasterUtils;

public class RuntimeManagerRunner implements Callable<Boolean> {
  private static final Logger LOG = Logger.getLogger(RuntimeManagerRunner.class.getName());

  static final String NEW_COMPONENT_PARALLELISM_KEY = "NEW_COMPONENT_PARALLELISM";

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
      case UPDATE:
        result = updateTopologyHandler(topologyName,
            config.getStringValue(NEW_COMPONENT_PARALLELISM_KEY));
        break;
      default:
        LOG.severe("Unknown command for topology: " + command);
    }

    return result;
  }

  /**
   * Handler to activate a topology
   */
  private boolean activateTopologyHandler(String topologyName) {
    return TMasterUtils.transitionTopologyState(
        topologyName, "activate", Runtime.schedulerStateManagerAdaptor(runtime),
        TopologyAPI.TopologyState.PAUSED, TopologyAPI.TopologyState.RUNNING);
  }

  /**
   * Handler to deactivate a topology
   */
  private boolean deactivateTopologyHandler(String topologyName) {
    return TMasterUtils.transitionTopologyState(
        topologyName, "deactivate", Runtime.schedulerStateManagerAdaptor(runtime),
        TopologyAPI.TopologyState.RUNNING, TopologyAPI.TopologyState.PAUSED);
  }

  /**
   * Handler to restart a topology
   */
  private boolean restartTopologyHandler(String topologyName) {
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
  private boolean killTopologyHandler(String topologyName) {
    Scheduler.KillTopologyRequest killTopologyRequest = Scheduler.KillTopologyRequest.newBuilder()
        .setTopologyName(topologyName).build();

    if (!schedulerClient.killTopology(killTopologyRequest)) {
      LOG.log(Level.SEVERE, "Failed to kill with Scheduler.");
      return false;
    }

    // clean up the state of the topology in state manager
    if (!cleanState(topologyName, Runtime.schedulerStateManagerAdaptor(runtime))) {
      LOG.severe("Failed to clean topology state");
      return false;
    }

    // Clean the connection when we are done.
    LOG.fine("Scheduler killed topology successfully.");
    return true;
  }

  /**
   * Handler to update a topology
   */
  private boolean updateTopologyHandler(String topologyName, String newParallelism) {
    LOG.fine(String.format("updateTopologyHandler called for %s with %s",
        topologyName, newParallelism));
    SchedulerStateManagerAdaptor manager = Runtime.schedulerStateManagerAdaptor(runtime);

    PackingPlans.PackingPlan currentPlan = manager.getPackingPlan(topologyName);
    PackingPlans.PackingPlan proposedPlan = buildNewPackingPlan(currentPlan, newParallelism);

    Scheduler.UpdateTopologyRequest updateTopologyRequest =
        Scheduler.UpdateTopologyRequest.newBuilder()
            .setCurrentPackingPlan(currentPlan)
            .setProposedPackingPlan(proposedPlan)
            .build();

    LOG.info("Sending Updating topology request: " + updateTopologyRequest);
    if (!schedulerClient.updateTopology(updateTopologyRequest)) {
      LOG.log(Level.SEVERE, "Failed to update topology with Scheduler, updateTopologyRequest="
          + updateTopologyRequest);
      return false;
    }

    // Clean the connection when we are done.
    LOG.fine("Scheduler updated topology successfully.");
    return true;
  }

  /**
   * Clean all states of a heron topology
   * 1. Topology def and ExecutionState are required to exist to delete
   * 2. TMasterLocation, SchedulerLocation and PhysicalPlan may not exist to delete
   */
  protected boolean cleanState(
      String topologyName,
      SchedulerStateManagerAdaptor statemgr) {
    LOG.fine("Cleaning up topology state");

    Boolean result;

    // It is possible that TMasterLocation, PackingPlan, PhysicalPlan and SchedulerLocation are nots
    // set. Just log but don't consider it a failure and don't return false
    result = statemgr.deleteTMasterLocation(topologyName);
    if (result == null || !result) {
      LOG.warning("Failed to clear TMaster location. Check whether TMaster set it correctly.");
    }

    result = statemgr.deletePackingPlan(topologyName);
    if (result == null || !result) {
      LOG.warning("Failed to clear packing plan. Check whether TMaster set it correctly.");
    }

    result = statemgr.deletePhysicalPlan(topologyName);
    if (result == null || !result) {
      LOG.warning("Failed to clear physical plan. Check whether TMaster set it correctly.");
    }

    result = statemgr.deleteSchedulerLocation(topologyName);
    if (result == null || !result) {
      LOG.warning("Failed to clear scheduler location. Check whether Scheduler set it correctly.");
    }

    result = statemgr.deleteExecutionState(topologyName);
    if (result == null || !result) {
      LOG.severe("Failed to clear execution state");
      return false;
    }

    // Set topology def at last since we determine whether a topology is running
    // by checking the existence of topology def
    result = statemgr.deleteTopology(topologyName);
    if (result == null || !result) {
      LOG.severe("Failed to clear topology definition");
      return false;
    }

    LOG.fine("Cleaned up topology state");
    return true;
  }

  // *** all below should be replaced with the proper way to compute a new packing plan ***
  private PackingPlans.PackingPlan buildNewPackingPlan(PackingPlans.PackingPlan currentPlan,
                                                       String newParallelism) {
    Map<String, Integer> componentCounts =
        parseInstanceDistribution(currentPlan.getInstanceDistribution());
    Integer totalInstances = 0;
    for (Integer count : componentCounts.values()) {
      totalInstances += count;
    }

    Map<String, Integer> componentChanges = parallelismDelta(componentCounts, newParallelism);

    // just add instances to the last container
    Integer nextInstanceId = totalInstances + 1; // TODO: don't assume they go from 1 -> N
    StringBuilder newInstanceDist = new StringBuilder(currentPlan.getInstanceDistribution());
    boolean addNewContainer = false;
    if(addNewContainer) {
      newInstanceDist.append(",");
      newInstanceDist.append(totalInstances + 1);
      newInstanceDist.append(":");
    }

    for (String component : componentChanges.keySet()) {
      Integer delta = componentChanges.get(component);
      assertTrue(delta > 0,
          "Component reductions (%s) for %s not supported. Parallelism change request: %s",
          delta, component, newParallelism);
      for (int i = 0; i < delta; i++) {
        newInstanceDist.append(":");
        newInstanceDist.append(component);
        newInstanceDist.append(":");
        newInstanceDist.append(nextInstanceId++);
        newInstanceDist.append(":0");
      }
    }

    return createPackingPlan(newInstanceDist.toString(), currentPlan.getComponentRamDistribution());
  }

  private Map<String, Integer> parallelismDelta(Map<String, Integer> componentCounts,
                                               String newParallelism) {
    Map<String, Integer> changeRequests = parallelismChangeRequests(newParallelism);
    for (String component : changeRequests.keySet()) {
      if (!componentCounts.containsKey(component)) {
        throw new IllegalArgumentException(
            "Invalid component name in update request: " + component);
      }
      Integer newValue = changeRequests.get(component);
      Integer delta = newValue - componentCounts.get(component);
      if (delta == 0) {
        changeRequests.remove(component);
      } else {
        changeRequests.put(component, delta);
      }
    }
    return changeRequests;
  }

  // TODO: better error handling
  private Map<String, Integer> parallelismChangeRequests(String newParallelism) {
    Map<String, Integer> changes = new HashMap<>();
    for (String componentValuePair : newParallelism.split(",")) {
      String[] kvp = componentValuePair.split(":", 2);
      changes.put(kvp[0], Integer.parseInt(kvp[1]));
    }
    return changes;
  }

  // given a string like "1:word:3:0:exclaim1:2:0:exclaim1:1:0,2:exclaim1:4:0" returns a map of
  // { word -> 1, explain1 -> 3 }
  private Map<String, Integer> parseInstanceDistribution(String instanceDistribution) {
    Map<String, Integer> componentParallelism = new HashMap<>();
    String[] containers = parseInstanceDistributionContainers(instanceDistribution);
    for (String container : containers) {
      String[] tokens = container.split(":");
      assertTrue(tokens.length > 3 && (tokens.length - 1) % 3 == 0,
          "Invalid instance distribution format. Expected componentId "
              + "followed by instance triples: %s", instanceDistribution);
      Set<String> idsFound = new HashSet<>();
      for (int i = 1; i < tokens.length; i += 3) {
        String instanceName = tokens[i];
        String instanceId = tokens[i + 1];
        assertTrue(!idsFound.contains(instanceId),
            "Duplicate instanceId (%s) found in instance distribution %s for instance %s %s",
            instanceId, instanceDistribution, instanceName, i);
        idsFound.add(instanceId);
        Integer occurrences = componentParallelism.getOrDefault(instanceName, 0);
        componentParallelism.put(instanceName, occurrences + 1);
      }
    }
    return componentParallelism;
  }

  private static String[] parseInstanceDistributionContainers(String instanceDistribution) {
    return instanceDistribution.split(",");
  }

  private static PackingPlans.PackingPlan createPackingPlan(String instanceDistribution,
                                                            String componentRamDistribution) {
    PackingPlans.PackingPlan.Builder builder = PackingPlans.PackingPlan.newBuilder();
    builder.setInstanceDistribution(instanceDistribution);
    builder.setComponentRamDistribution(componentRamDistribution);
    return builder.build();
  }

  protected void assertTrue(boolean condition, String message, Object... values) {
    if (!condition) {
      throw new RuntimeException("ERROR: " + String.format(message, values));
    }
  }
}
