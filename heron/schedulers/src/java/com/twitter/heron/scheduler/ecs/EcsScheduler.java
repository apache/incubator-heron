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

package com.twitter.heron.scheduler.ecs;

//import java.io.FileNotFoundException;
//import java.io.FileWriter;
import java.io.File;
//import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
//import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermission;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;


//import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;

import org.apache.commons.io.IOUtils;

import com.twitter.heron.proto.scheduler.Scheduler;
import com.twitter.heron.scheduler.UpdateTopologyManager;
import com.twitter.heron.scheduler.utils.SchedulerUtils;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Context;
import com.twitter.heron.spi.packing.PackingPlan;
import com.twitter.heron.spi.scheduler.IScalable;
import com.twitter.heron.spi.scheduler.IScheduler;
import com.twitter.heron.spi.utils.ShellUtils;


/**
 * Created by ananth on 4/19/17.
 */
public class EcsScheduler implements IScheduler, IScalable {
  private static final Logger LOG = Logger.getLogger(EcsScheduler.class.getName());
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

  public void close() {
    // Shut down the ExecutorService for monitoring
    monitorService.shutdownNow();

    // Clear the map
    processToContainer.clear();

    if (updateTopologyManager != null) {
      updateTopologyManager.close();
    }
  }


  protected int startExecutorSyncProcess(int container) {
    String executingInShell = new String();
    executingInShell = getExecutorCommand(container)[0];
    System.out.println("executing in Shell: " + executingInShell);
    return ShellUtils.runProcess(executingInShell, null);
  }

  protected void startExecutor(final int container) {
    LOG.info("Starting a new executor for container: " + container);
    int shellOutput = startExecutorSyncProcess(container);
    LOG.info("output value for the executor container: "
        + container + String.valueOf(shellOutput));

  }

  private String[] getExecutorCommand(int container) {
    List<Integer> freePorts = new ArrayList<>(SchedulerUtils.PORTS_REQUIRED_FOR_EXECUTOR);
    String ecsComposeCommand = "/usr/local/bin/ecs-cli compose --project-name heron_ex";
    ecsComposeCommand = ecsComposeCommand + String.valueOf(container) + "  --file ";
    String ecsEnableUpdaload = " up";

    for (int i = 0; i < SchedulerUtils.PORTS_REQUIRED_FOR_EXECUTOR; i++) {
      //freePorts.add(SysUtils.getFreePort());
      freePorts.add(5000 + (i + (container * 10)));
    }
    String replaceTopologyBinFile = Context.topologyBinaryFile(config);
    String replaeRole = Context.role(config);
    String replaceJavaHome = Context.clusterJavaHome(config);
    System.out.println("Topology Bin file :" + Context.topologyBinaryFile(config));
    System.out.println("Role :" + Context.role(config));
    System.out.println("JAVA HOME :" + Context.clusterJavaHome(config));
    String[] executorCmd = SchedulerUtils.executorCommand(config, runtime, container, freePorts);
    System.out.println("Executor command line: " + Arrays.toString(executorCmd));

    String finalExecCommand = formHeronExecCommand(executorCmd);
    finalExecCommand = finalExecCommand.replace(replaceTopologyBinFile, "heron-examples.jar");
    //finalExecCommand = finalExecCommand.replace(replaeRole, "root");
    finalExecCommand = finalExecCommand.replace(replaceJavaHome, EcsContext.DESTINATION_JVM);
    finalExecCommand = finalExecCommand.replaceAll("\"", "'");
    System.out.println("heron exec command: " + finalExecCommand);
    String content = null;
    String finalCommand = ecsComposeCommand;
    FileOutputStream dockerFilestream = null;
    String dockerComposeFileName = EcsContext.COMPOSE_WORKING_DIR;
    try {
      System.out.println("reading file: docker_compose_template.yml ");

      content = EcsContext.PART1 + EcsContext.CMD;
      content = content + EcsContext.ECSNETWORK;
      content = replacePortNumbers(container, content);
      content = content.replaceAll("TOPOLOGY_NAME", EcsContext.topologyName(config));
      content = content.replaceAll("container_number",
          "executor" + String.valueOf(container));
      content = content.replace("heron_executor", finalExecCommand);
      System.out.println("content to build .yml file : " + content);
      dockerComposeFileName = dockerComposeFileName + "/docker_compose";
      dockerComposeFileName = dockerComposeFileName + String.valueOf(container) + ".yml";
      if (Files.exists(Paths.get(dockerComposeFileName))) {
        //its from an old submit so delete it
        Files.delete(Paths.get(dockerComposeFileName));
      }
      final File file = new File(dockerComposeFileName);
      file.setWritable(true);
      System.out.println("docker compose file " +  dockerComposeFileName);
      dockerFilestream = new FileOutputStream(dockerComposeFileName);
      //IOUtils.write(content, new FileOutputStream(dockerComposeFileName));
      IOUtils.write(content, dockerFilestream);
      IOUtils.closeQuietly(dockerFilestream);
      setPermissionsOnDockerfile(dockerComposeFileName);
      System.out.println("docker compose file permission granted " +  dockerComposeFileName);
    } catch (IOException  e) {
      e.printStackTrace();
    } finally {
      IOUtils.closeQuietly(dockerFilestream);
    }

    finalCommand = finalCommand + dockerComposeFileName + ecsEnableUpdaload;
    //return finalCommand.toArray(new String[finalCommand.size()]);
    System.out.println("final Ecs Task command " + finalCommand);
    return  new String[] {finalCommand};

  }

  public String replacePortNumbers(int container, String content) {
    int basePortnumber = 5000;
    String localContent = new String(content);
    for (int i = 0; i < SchedulerUtils.PORTS_REQUIRED_FOR_EXECUTOR; i++) {
      localContent = localContent.replace(String.valueOf(basePortnumber + i),
          String.valueOf(basePortnumber + (i + (container * 10))));
    }
    return localContent;
  }

  public void setPermissionsOnDockerfile(String fileName) throws IOException {
    Set<PosixFilePermission> perms = new HashSet<PosixFilePermission>();
    //add owners permission
    perms.add(PosixFilePermission.OWNER_READ);
    perms.add(PosixFilePermission.OWNER_WRITE);
    perms.add(PosixFilePermission.OWNER_EXECUTE);
    //add group permissions
    perms.add(PosixFilePermission.GROUP_READ);
    perms.add(PosixFilePermission.GROUP_WRITE);
    perms.add(PosixFilePermission.GROUP_EXECUTE);
    //add others permissions
    perms.add(PosixFilePermission.OTHERS_READ);
    perms.add(PosixFilePermission.OTHERS_WRITE);
    perms.add(PosixFilePermission.OTHERS_EXECUTE);

    Files.setPosixFilePermissions(Paths.get(fileName), perms);
  }
  public String formHeronExecCommand(String[] inStringArray) {
    StringBuilder builder = new StringBuilder();

    for (String string : inStringArray) {
      if (builder.length() > 0) {
        builder.append(" ");
      }
      builder.append(string);
    }

    String stringToReturn = builder.toString();
    return stringToReturn;
  }

  /**
   * Schedule the provided packed plan
   */
  @Override
  public boolean onSchedule(PackingPlan packing) {
    LOG.info("Starting to deploy topology: " + EcsContext.topologyName(config));

    synchronized (processToContainer) {
      LOG.info("Starting executor for TMaster");
      startExecutor(0);

      // for each container, run its own executor
      for (PackingPlan.ContainerPlan container : packing.getContainers()) {
        startExecutor(container.getId());
      }
    }

    LOG.info("Executor for each container have been started.");

    return true;
  }

  @Override
  public void addContainers(Set<PackingPlan.ContainerPlan> containers) {
    synchronized (processToContainer) {
      for (PackingPlan.ContainerPlan container : containers) {
        if (processToContainer.values().contains(container.getId())) {
          throw new RuntimeException(String.format("Found active container for %s, "
              + "cannot launch a duplicate container.", container.getId()));
        }
        startExecutor(container.getId());
      }
    }
  }
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

  public List<String> getJobLinks() {
    return null;
  }

  public boolean onKill(Scheduler.KillTopologyRequest request) {
    return false;
  }

  public boolean onRestart(Scheduler.RestartTopologyRequest request) {
    return false;
  }

  public boolean onUpdate(Scheduler.UpdateTopologyRequest request) {
    return false;
  }


}
