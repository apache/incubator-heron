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
import java.nio.charset.Charset;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.xml.bind.DatatypeConverter;

import org.apache.commons.io.FilenameUtils;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.proto.scheduler.Scheduler;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.PackingPlan;
import com.twitter.heron.spi.common.ShellUtils;
import com.twitter.heron.spi.scheduler.IScheduler;
import com.twitter.heron.spi.utils.NetworkUtils;
import com.twitter.heron.spi.utils.Runtime;
import com.twitter.heron.spi.utils.TopologyUtils;

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
   * Encode the JVM options
   *
   * @return encoded string
   */
  protected String formatJavaOpts(String javaOpts) {
    String javaOptsBase64 = DatatypeConverter.printBase64Binary(
        javaOpts.getBytes(Charset.forName("UTF-8")));

    return String.format("\"%s\"", javaOptsBase64.replace("=", "&equals;"));
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

  protected String getExecutorCommand(int container) {
    TopologyAPI.Topology topology = Runtime.topology(runtime);

    int port1 = NetworkUtils.getFreePort();
    int port2 = NetworkUtils.getFreePort();
    int port3 = NetworkUtils.getFreePort();
    int shellPort = NetworkUtils.getFreePort();
    int port4 = NetworkUtils.getFreePort();

    if (port1 == -1 || port2 == -1 || port3 == -1 || shellPort == -1 || port4 == -1) {
      throw new RuntimeException("Failed to find available ports to start topology");
    }

    String executorCmd = String.format(
        "%s %d %s %s %s %s %s %s %s %s %s %s %s %s %s %s "
            + "%s %s %s %s %s %s %d %s %s %d %s %s %s %s %s %s %d",
        LocalContext.executorSandboxBinary(config),
        container,
        topology.getName(),
        topology.getId(),
        FilenameUtils.getName(LocalContext.topologyDefinitionFile(config)),
        Runtime.instanceDistribution(runtime),
        LocalContext.stateManagerConnectionString(config),
        LocalContext.stateManagerRootPath(config),
        LocalContext.tmasterSandboxBinary(config),
        LocalContext.stmgrSandboxBinary(config),
        LocalContext.metricsManagerSandboxClassPath(config),
        formatJavaOpts(TopologyUtils.getInstanceJvmOptions(topology)),
        TopologyUtils.makeClassPath(topology, LocalContext.topologyJarFile(config)),
        port1,
        port2,
        port3,
        LocalContext.systemConfigSandboxFile(config),
        TopologyUtils.formatRamMap(
            TopologyUtils.getComponentRamMap(topology, LocalContext.instanceRam(config))),
        formatJavaOpts(TopologyUtils.getComponentJvmOptions(topology)),
        LocalContext.topologyPackageType(config),
        LocalContext.topologyJarFile(config),
        LocalContext.javaSandboxHome(config),
        shellPort,
        LocalContext.logSandboxDirectory(config),
        LocalContext.shellSandboxBinary(config),
        port4,
        LocalContext.cluster(config),
        LocalContext.role(config),
        LocalContext.environ(config),
        LocalContext.instanceSandboxClassPath(config),
        LocalContext.metricsSinksSandboxFile(config),
        "no_need_since_scheduler_is_started",
        0
    );

    LOG.info("Executor command line: " + executorCmd.toString());
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

  public boolean isTopologyKilled() {
    return isTopologyKilled;
  }

  // This method shall be used only for unit test
  protected ExecutorService getMonitorService() {
    return monitorService;
  }

  // This method shall be used only for unit test
  protected Map<Process, Integer> getProcessToContainer() {
    return processToContainer;
  }
}
