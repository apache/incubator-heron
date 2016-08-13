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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.common.base.Optional;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.common.basics.SysUtils;
import com.twitter.heron.proto.scheduler.Scheduler;
import com.twitter.heron.scheduler.ScalableScheduler;
import com.twitter.heron.scheduler.UpdateTopologyManager;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.packing.PackingPlan;
import com.twitter.heron.spi.scheduler.IScheduler;
import com.twitter.heron.spi.utils.Runtime;
import com.twitter.heron.spi.utils.SchedulerUtils;
import com.twitter.heron.spi.utils.ShellUtils;

public class LocalScheduler implements IScheduler, ScalableScheduler {
  private static final Logger LOG = Logger.getLogger(LocalScheduler.class.getName());
  // executor service for monitoring all the containers
  private final ExecutorService monitorService = Executors.newCachedThreadPool();
  // map to keep track of the process and the shard it is running
  private final Map<Process, Integer> processToContainer = new ConcurrentHashMap<>();
  private Config config;
  private Config runtime;
  private UpdateTopologyManager updateTopologyManager;
  // has the topology been killed?
  private volatile boolean isTopologyKilled = false;

  @Override
  public void initialize(Config mConfig, Config mRuntime) {
    this.config = mConfig;
    this.runtime = mRuntime;
    this.updateTopologyManager =
        new UpdateTopologyManager(runtime, Optional.<ScalableScheduler>of(this));
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
          } else if (!processToContainer.containsKey(containerExecutor)) {
            LOG.log(Level.INFO, "Container {0} is killed. No need to relaunch.", container);
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
      TopologyAPI.Topology topology = Runtime.topology(runtime);
      updateTopologyManager.updateTopology(
          request.getCurrentPackingPlan(), request.getProposedPackingPlan());
    } catch (ExecutionException | InterruptedException e) {
      LOG.log(Level.SEVERE, "Could not update topology for request: " + request, e);
      return false;
    }
    return true;
  }

  @Override
  public void addContainers(Integer count) {
    int activeContainerCount = processToContainer.size();

    for (int i = 0; i < count; i++) {
      // if number of active container is 2, then there is 1 TMaster container (id=0) and 1 worker
      // (id = 1). Then the next container to be added will have id = 2, same as current container
      // count
      startExecutor(activeContainerCount + i);
    }
  }

  @Override
  public void removeContainers(Integer existingContainerCount, Integer count) {
    LOG.log(Level.INFO, "Kill {0} of {1} containers", new Object[] {count, existingContainerCount});

    if (existingContainerCount != processToContainer.size()) {
      LOG.log(Level.SEVERE, "Container count mismatch: expected {0} != active {1}",
          new Object[]{existingContainerCount, processToContainer.size()});
      throw new RuntimeException("Container count mismatch");
    }

    Map<Integer, Process> containerToProcessMap = new HashMap<>();
    for (Map.Entry<Process, Integer> entry : processToContainer.entrySet()) {
      containerToProcessMap.put(entry.getValue(), entry.getKey());
    }

    for (int i = count, container = existingContainerCount - 1; i > 0; container--, i--) {
      Process process = containerToProcessMap.get(container);
      LOG.info("Killing executor for container: " + container);

      // remove the process so that it is not monitored and relaunched
      processToContainer.remove(process);
      process.destroy();
      LOG.info("Killed executor for container: " + container);
    }
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
}
