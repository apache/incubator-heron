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

package org.apache.heron.scheduler.local;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;

import org.apache.heron.api.utils.TopologyUtils;
import org.apache.heron.common.basics.SysUtils;
import org.apache.heron.proto.scheduler.Scheduler;
import org.apache.heron.scheduler.UpdateTopologyManager;
import org.apache.heron.scheduler.utils.Runtime;
import org.apache.heron.scheduler.utils.SchedulerUtils;
import org.apache.heron.scheduler.utils.SchedulerUtils.ExecutorPort;
import org.apache.heron.spi.common.Config;
import org.apache.heron.spi.packing.PackingPlan;
import org.apache.heron.spi.scheduler.IScalable;
import org.apache.heron.spi.scheduler.IScheduler;
import org.apache.heron.spi.utils.ShellUtils;

public class LocalScheduler implements IScheduler, IScalable {
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
        new UpdateTopologyManager(config, runtime, Optional.<IScalable>of(this));
  }

  @Override
  public void close() {
    // Shut down the ExecutorService for monitoring
    monitorService.shutdownNow();

    // Clear the map
    processToContainer.clear();

    if (updateTopologyManager != null) {
      updateTopologyManager.close();
    }
  }

  /**
   * Start executor process via running an async shell process
   */
  @VisibleForTesting
  protected Process startExecutorProcess(int container, Set<PackingPlan.InstancePlan> instances) {
    return ShellUtils.runASyncProcess(
        getExecutorCommand(container, instances),
        new File(LocalContext.workingDirectory(config)),
        Integer.toString(container));
  }

  /**
   * Start the executor for the given container
   */
  @VisibleForTesting
  protected void startExecutor(final int container, Set<PackingPlan.InstancePlan> instances) {
    LOG.info("Starting a new executor for container: " + container);

    // create a process with the executor command and topology working directory
    final Process containerExecutor = startExecutorProcess(container, instances);

    // associate the process and its container id
    processToContainer.put(containerExecutor, container);
    LOG.info("Started the executor for container: " + container);

    // add the container for monitoring
    startExecutorMonitor(container, containerExecutor, instances);
  }

  /**
   * Start the monitor of a given executor
   */
  @VisibleForTesting
  protected void startExecutorMonitor(final int container,
                                      final Process containerExecutor,
                                      Set<PackingPlan.InstancePlan> instances) {
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
          startExecutor(processToContainer.remove(containerExecutor), instances);
        } catch (InterruptedException e) {
          if (!isTopologyKilled) {
            LOG.log(Level.SEVERE, "Process is interrupted: ", e);
          }
        }
      }
    };

    monitorService.submit(r);
  }


  private String[] getExecutorCommand(int container,  Set<PackingPlan.InstancePlan> instances) {
    Map<ExecutorPort, String> ports = new HashMap<>();
    for (ExecutorPort executorPort : ExecutorPort.getRequiredPorts()) {
      int port = SysUtils.getFreePort();
      if (port == -1) {
        throw new RuntimeException("Failed to find available ports for executor");
      }
      ports.put(executorPort, String.valueOf(port));
    }

    if (TopologyUtils.getTopologyRemoteDebuggingEnabled(Runtime.topology(runtime))
        && instances != null) {
      List<String> remoteDebuggingPorts = new LinkedList<>();
      int portsForRemoteDebugging = instances.size();
      for (int i = 0; i < portsForRemoteDebugging; i++) {
        int port = SysUtils.getFreePort();
        if (port == -1) {
          throw new RuntimeException("Failed to find available ports for executor");
        }
        remoteDebuggingPorts.add(String.valueOf(port));
      }
      ports.put(ExecutorPort.JVM_REMOTE_DEBUGGER_PORTS,
          String.join(",", remoteDebuggingPorts));
    }

    String[] executorCmd = SchedulerUtils.getExecutorCommand(config, runtime, container, ports);
    LOG.info("Executor command line: " + Arrays.toString(executorCmd));
    return executorCmd;
  }

  /**
   * Schedule the provided packed plan
   */
  @Override
  public boolean onSchedule(PackingPlan packing) {
    LOG.info("Starting to deploy topology: " + LocalContext.topologyName(config));

    synchronized (processToContainer) {
      LOG.info("Starting executor for TManager");
      startExecutor(0, null);

      // for each container, run its own executor
      for (PackingPlan.ContainerPlan container : packing.getContainers()) {
        startExecutor(container.getId(), container.getInstances());
      }
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

    synchronized (processToContainer) {
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
    }

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
      updateTopologyManager.updateTopology(
          request.getCurrentPackingPlan(), request.getProposedPackingPlan());
    } catch (ExecutionException | InterruptedException e) {
      LOG.log(Level.SEVERE, "Could not update topology for request: " + request, e);
      return false;
    }
    return true;
  }

  @Override
  public Set<PackingPlan.ContainerPlan> addContainers(Set<PackingPlan.ContainerPlan> containers) {
    synchronized (processToContainer) {
      for (PackingPlan.ContainerPlan container : containers) {
        if (processToContainer.values().contains(container.getId())) {
          throw new RuntimeException(String.format("Found active container for %s, "
              + "cannot launch a duplicate container.", container.getId()));
        }
        startExecutor(container.getId(), container.getInstances());
      }
    }
    return containers;
  }

  @Override
  public void removeContainers(Set<PackingPlan.ContainerPlan> containersToRemove) {
    LOG.log(Level.INFO,
        "Kill {0} of {1} containers",
        new Object[]{containersToRemove.size(), processToContainer.size()});

    synchronized (processToContainer) {
      // Create a inverse map to be able to get process instance from container id
      Map<Integer, Process> containerToProcessMap = new HashMap<>();
      for (Map.Entry<Process, Integer> entry : processToContainer.entrySet()) {
        containerToProcessMap.put(entry.getValue(), entry.getKey());
      }

      for (PackingPlan.ContainerPlan containerToRemove : containersToRemove) {
        int containerId = containerToRemove.getId();
        Process process = containerToProcessMap.get(containerId);
        if (process == null) {
          LOG.log(Level.WARNING, "Container for id:{0} not found.", containerId);
          continue;
        }

        // remove the process so that it is not monitored and relaunched
        LOG.info("Killing executor for container: " + containerId);
        processToContainer.remove(process);
        process.destroy();
        LOG.info("Killed executor for container: " + containerId);
      }
    }
  }

  @VisibleForTesting
  boolean isTopologyKilled() {
    return isTopologyKilled;
  }

  @VisibleForTesting
  ExecutorService getMonitorService() {
    return monitorService;
  }

  @VisibleForTesting
  Map<Process, Integer> getProcessToContainer() {
    return processToContainer;
  }
}
