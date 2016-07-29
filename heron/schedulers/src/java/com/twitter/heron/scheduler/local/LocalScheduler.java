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

package com.twitter.heron.scheduler.local;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.protobuf.Descriptors;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.common.basics.SysUtils;
import com.twitter.heron.proto.scheduler.Scheduler;
import com.twitter.heron.proto.system.PackingPlans;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.packing.PackingPlan;
import com.twitter.heron.spi.scheduler.IScheduler;
import com.twitter.heron.spi.statemgr.SchedulerStateManagerAdaptor;
import com.twitter.heron.spi.utils.Runtime;
import com.twitter.heron.spi.utils.SchedulerUtils;
import com.twitter.heron.spi.utils.ShellUtils;

public class LocalScheduler implements IScheduler {
  private static final Logger LOG = Logger.getLogger(LocalScheduler.class.getName());
  // executor service for monitoring all the containers
  private final ExecutorService monitorService = Executors.newCachedThreadPool();
  // map to keep track of the process and the shard it is running
  private final Map<Process, Integer> processToContainer = new ConcurrentHashMap<>();
  private Config config;
  private Config runtime;
  // has the topology been killed?
  private volatile boolean isTopologyKilled = false;

  @Override
  public void initialize(Config mConfig, Config mRuntime) {
    this.config = mConfig;
    this.runtime = mRuntime;
  }

  @Override
  public void close() {
    // Shut down the ExecutorService for monitoring
    monitorService.shutdownNow();

    // Clear the map
    processToContainer.clear();
  }

  /**
   * Start executor process via running an async shell process
   */
  protected Process startExecutorProcess(int container) {
    return ShellUtils.runASyncProcess(true,
        getExecutorCommand(container), new File(LocalContext.workingDirectory(config)));
  }

  /**
   * Start the executor for the given container
   */
  protected void startExecutor(final int container) {
    LOG.info("Starting a new executor for container: " + container);

    // create a process with the executor command and topology working directory
    final Process containerExecutor = startExecutorProcess(container);

    // associate the process and its container id
    processToContainer.put(containerExecutor, container);
    LOG.info("Started the executor for container: " + container);

    // add the container for monitoring
    startExecutorMonitor(container, containerExecutor);
  }

  /**
   * Start the monitor of a given executor
   */
  protected void startExecutorMonitor(final int container, final Process containerExecutor) {
    // add the container for monitoring
    Runnable r = new Runnable() {
      @Override
      public void run() {
        try {
          LOG.info("Waiting for container " + container + " to finish.");
          containerExecutor.waitFor();

          LOG.log(Level.INFO,
              "Container {0} is completed. Exit status: {1}",
              new Object[]{container, containerExecutor.exitValue()});
          if (isTopologyKilled) {
            LOG.info("Topology is killed. Not to start new executors.");
            return;
          }
          LOG.log(Level.INFO, "Trying to restart container {0}", container);
          // restart the container
          startExecutor(processToContainer.remove(containerExecutor));
        } catch (InterruptedException e) {
          LOG.log(Level.SEVERE, "Process is interrupted: ", e);
        }
      }
    };

    monitorService.submit(r);
  }

  protected String[] getExecutorCommand(int container) {
    List<Integer> freePorts = new ArrayList<>(SchedulerUtils.PORTS_REQUIRED_FOR_EXECUTOR);
    for (int i = 0; i < SchedulerUtils.PORTS_REQUIRED_FOR_EXECUTOR; i++) {
      freePorts.add(SysUtils.getFreePort());
    }

    String[] executorCmd = SchedulerUtils.executorCommand(config, runtime, container, freePorts);

    LOG.info("Executor command line: " + Arrays.toString(executorCmd));
    return executorCmd;
  }

  /**
   * Schedule the provided packed plan
   */
  @Override
  public boolean onSchedule(PackingPlan packing) {
    long numContainers = Runtime.numContainers(runtime);

    LOG.info("Starting to deploy topology: " + LocalContext.topologyName(config));
    LOG.info("# of containers: " + numContainers);

    // for each container, run its own executor
    for (int i = 0; i < numContainers; i++) {
      startExecutor(i);
    }

    LOG.info("Executor for each container have been started.");

    return true;
  }

  @Override
  public List<String> getJobLinks() {
    return new ArrayList<>();
  }

  /**
   * Handler to kill topology
   */
  @Override
  public boolean onKill(Scheduler.KillTopologyRequest request) {

    // get the topology name
    String topologyName = LocalContext.topologyName(config);
    LOG.info("Command to kill topology: " + topologyName);

    // set the flag that the topology being killed
    isTopologyKilled = true;

    // destroy/kill the process for each container
    for (Process p : processToContainer.keySet()) {

      // get the container index for the process
      int index = processToContainer.get(p);
      LOG.info("Killing executor for container: " + index);

      // destroy the process
      p.destroy();
      LOG.info("Killed executor for container: " + index);
    }

    // clear the mapping between process and container ids
    processToContainer.clear();

    return true;
  }

  /**
   * Handler to restart topology
   */
  @Override
  public boolean onRestart(Scheduler.RestartTopologyRequest request) {
    // Containers would be restarted automatically once we destroy it
    int containerId = request.getContainerIndex();

    List<Process> processesToRestart = new LinkedList<>();

    if (containerId == -1) {
      LOG.info("Command to restart the entire topology: " + LocalContext.topologyName(config));
      processesToRestart.addAll(processToContainer.keySet());
    } else {
      // restart that particular container
      LOG.info("Command to restart a container of topology: " + LocalContext.topologyName(config));
      LOG.info("Restart container requested: " + containerId);

      // locate the container and destroy it
      for (Process p : processToContainer.keySet()) {
        if (containerId == processToContainer.get(p)) {
          processesToRestart.add(p);
        }
      }
    }

    if (processesToRestart.isEmpty()) {
      LOG.severe("Container not exist.");
      return false;
    }

    for (Process process : processesToRestart) {
      process.destroy();
    }

    return true;
  }

  @Override
  public boolean onUpdate(Scheduler.UpdateTopologyRequest request) {
    try {
      adjustTopology(request.getCurrentPackingPlan(), request.getProposedPackingPlan());
    } catch (ExecutionException|InterruptedException e) {
      LOG.log(Level.SEVERE, "Could not update topology for request: " + request, e);
      return false;
    }
    return true;
  }

  boolean isTopologyKilled() {
    return isTopologyKilled;
  }

  // This method shall be used only for unit test
  ExecutorService getMonitorService() {
    return monitorService;
  }

  // This method shall be used only for unit test
  Map<Process, Integer> getProcessToContainer() {
    return processToContainer;
  }


  // Handles scaling the cluster out or in based on the proposedInstanceDistribution
  private void adjustTopology(PackingPlans.PackingPlan existingPackingPlan,
                              PackingPlans.PackingPlan proposedPackingPlan)
      throws ExecutionException, InterruptedException {
    TopologyAPI.Topology topology = Runtime.topology(runtime);
    String topologyName = topology.getName();

    Map<String, Integer> proposedChanges = parallelismChanges(
        existingPackingPlan.getInstanceDistribution(),
        proposedPackingPlan.getInstanceDistribution());

    SchedulerStateManagerAdaptor stateManager = Runtime.schedulerStateManagerAdaptor(runtime);

    for (Map.Entry<String, Integer> proposedChange : proposedChanges.entrySet()) {
      String componentName = proposedChange.getKey();
      Integer parallelism = proposedChange.getValue();
      topology = mergeTopology(topology, componentName, parallelism);
    }

    int existingContainerCount = parseInstanceDistributionContainers(
        existingPackingPlan.getInstanceDistribution()).length;
    int proposedContainerCount = parseInstanceDistributionContainers(
        proposedPackingPlan.getInstanceDistribution()).length;
    Integer containerDelta = proposedContainerCount - existingContainerCount;

    assertTrue(proposedContainerCount > 0,
        "proposed instance distribution must have at least 1 container %s",
        proposedPackingPlan.getInstanceDistribution());

    // request new aurora resources if necessary. Once containers are allocated we must make the
    // changes to state manager quickly, otherwise aurora might penalize for thrashing on start-up
    if (containerDelta > 0) {
      addContainers(topologyName, containerDelta);
    }

    // assert found is same as existing.
    PackingPlans.PackingPlan foundPackingPlan = stateManager.getPackingPlan(topologyName);
    assertTrue(foundPackingPlan.equals(existingPackingPlan),
        "Existing packing plan received does not equal the packing plan found in the state "
        + "manager. Not updating topology. Received: %s, Found: %s",
        existingPackingPlan, foundPackingPlan);

    //update parallelism in topology since TMaster checks that Sum(parallelism) == Sum(instances)
    print("==> Deleted existing Topology: %s", stateManager.deleteTopology(topologyName));
    print("==> Set new Topology: %s", stateManager.setTopology(topology, topologyName));

    print("==> Deleted existing packing plan: %s", stateManager.deletePackingPlan(topologyName));
    print("==> Set new PackingPlan: %s",
        stateManager.setPackingPlan(proposedPackingPlan, topologyName));

    print("==> Deleted Physical Plan: %s", stateManager.deletePhysicalPlan(topologyName));

    if (containerDelta < 0) {
      removeContainers(topologyName, existingContainerCount, -containerDelta);
    }
  }

  void addContainers(String topologyName, Integer count) {
    throw new RuntimeException("addContainers not implemented by this scheduler.");
  }

  void removeContainers(String topologyName, Integer existingContainerCount, Integer count) {
    throw new RuntimeException("addContainers not implemented by this scheduler.");
  }

  // Given the existing and proposed instance distribution, verify the proposal and return only
  // the changes being requested.
  private Map<String, Integer> parallelismChanges(String existingInstanceDistribution,
                                                  String proposedInstanceDistribution) {
    Map<String, Integer> existingParallelism =
        parseInstanceDistribution(existingInstanceDistribution);
    Map<String, Integer> proposedParallelism =
        parseInstanceDistribution(proposedInstanceDistribution);
    Map<String, Integer> parallelismChanges = new HashMap<>();

    for (String componentName : proposedParallelism.keySet()) {
      Integer newParallelism = proposedParallelism.get(componentName);
      assertTrue(existingParallelism.containsKey(componentName),
          "key %s in proposed instance distribution %s not found in "
              + "current instance distribution %s",
          componentName, proposedInstanceDistribution, existingInstanceDistribution);
      assertTrue(newParallelism > 0,
          "Non-positive parallelism (%s) for component %s found in instance distribution %s",
          newParallelism, componentName, proposedInstanceDistribution);

      if (!newParallelism.equals(existingParallelism.get(componentName))) {
        parallelismChanges.put(componentName, newParallelism);
      }
    }
    return parallelismChanges;
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

  private static TopologyAPI.Topology mergeTopology(TopologyAPI.Topology topology,
                                                    String componentName,
                                                    int parallelism) {
    TopologyAPI.Topology.Builder builder = TopologyAPI.Topology.newBuilder().mergeFrom(topology);
    for (int i = 0; i < builder.getBoltsCount(); i++) {
      TopologyAPI.Bolt.Builder boltBuilder = builder.getBoltsBuilder(i);
      TopologyAPI.Component.Builder compBuilder = boltBuilder.getCompBuilder();
      for (Map.Entry<Descriptors.FieldDescriptor, Object> entry
          : compBuilder.getAllFields().entrySet()) {
        if (entry.getKey().getName().equals("name") && componentName.equals(entry.getValue())) {
          TopologyAPI.Config.Builder confBuilder = compBuilder.getConfigBuilder();
          boolean keyFound = false;
          for (TopologyAPI.Config.KeyValue.Builder kvBuilder : confBuilder.getKvsBuilderList()) {
            if (kvBuilder.getKey().equals(
                com.twitter.heron.api.Config.TOPOLOGY_COMPONENT_PARALLELISM)) {
              kvBuilder.setValue(Integer.toString(parallelism));
              keyFound = true;
              break;
            }
          }
          if (!keyFound) {
            TopologyAPI.Config.KeyValue.Builder kvBuilder =
                TopologyAPI.Config.KeyValue.newBuilder();
            kvBuilder.setKey(com.twitter.heron.api.Config.TOPOLOGY_COMPONENT_PARALLELISM);
            kvBuilder.setValue(Integer.toString(parallelism));
            confBuilder.addKvs(kvBuilder);
          }
        }
      }
    }
    return builder.build();
  }

  protected void assertTrue(boolean condition, String message, Object... values) {
    if (!condition) {
      throw new RuntimeException("ERROR: " + String.format(message, values));
    }
  }

  protected void print(String format, Object... values) {
    LOG.fine(String.format(format, values));
  }
}
