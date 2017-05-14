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


import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import com.google.common.annotations.VisibleForTesting;

import org.apache.commons.io.IOUtils;

import com.twitter.heron.common.basics.SysUtils;
import com.twitter.heron.proto.scheduler.Scheduler;
import com.twitter.heron.scheduler.utils.SchedulerUtils;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Context;
import com.twitter.heron.spi.packing.PackingPlan;
import com.twitter.heron.spi.scheduler.IScheduler;
import com.twitter.heron.spi.utils.ShellUtils;


public class EcsScheduler implements IScheduler {
  private static final Logger LOG = Logger.getLogger(EcsScheduler.class.getName());
  private Config config;
  private Config runtime;
  private StringBuilder nfreePorts;
  private volatile boolean isTopologyKilled = false;
  private File tempDockerFile = null;

  @Override
  public void initialize(Config mConfig, Config mRuntime) {
    this.config = Config.toClusterMode(mConfig);
    this.runtime = mRuntime;
  }

  @Override
  public void close() {
  }


  @VisibleForTesting
  protected int startExecutorSyncProcess(int container) {
    String executingInShell = new String();
    executingInShell = getExecutorCommand(container)[0];
    return ShellUtils.runProcess(executingInShell, null);
  }

  @VisibleForTesting
  protected void startExecutor(final int container) {
    LOG.info("Starting a new executor for container: " + container);
    int shellOutput = startExecutorSyncProcess(container);
    LOG.info("output value for the executor container: "
        + container + String.valueOf(shellOutput));
  }

  @VisibleForTesting
  private String[] getExecutorCommand(int container) {
    List<Integer> freePorts = new ArrayList<>(SchedulerUtils.PORTS_REQUIRED_FOR_EXECUTOR);
    Integer localFreePort = null;
    nfreePorts = new StringBuilder();
    for (int i = 0; i < SchedulerUtils.PORTS_REQUIRED_FOR_EXECUTOR; i++) {
      localFreePort = SysUtils.getFreePort();
      freePorts.add(localFreePort);
      nfreePorts.append("\n    - \"");
      nfreePorts.append(localFreePort);
      nfreePorts.append(":");
      nfreePorts.append(localFreePort);
      nfreePorts.append("\"");
    }
    String[] executorCmd = SchedulerUtils.executorCommand(config, runtime, container, freePorts);
    String finalExecCommand = setClusterValues(formHeronExecCommand(executorCmd));
    String ecsTaskProject =  EcsContext.topologyName(config) + "_" + container;
    FileOutputStream dockerFilestream = null;
    String content = null;
    try {
      tempDockerFile = File.createTempFile("docker", ".yml");
      content = getDockerFileContent(finalExecCommand, container);
      tempDockerFile.setWritable(true);
      dockerFilestream = new FileOutputStream(tempDockerFile);
      IOUtils.write(content, dockerFilestream);
      IOUtils.closeQuietly(dockerFilestream);
    } catch (IOException  e) {
      LOG.severe("Unable to create ecs task for container: " + container);
    } finally {
      IOUtils.closeQuietly(dockerFilestream);
    }
    String finalCommand = String.format("%s %s --file %s up",
                                         EcsContext.composeupCmd(config),
                                          ecsTaskProject, tempDockerFile);
    //LOG.info("final Ecs Task command " + finalCommand);
    tempDockerFile.deleteOnExit();
    return  new String[] {finalCommand};
  }

  private String setClusterValues(String localExecCommand) {
    String clusterExecCommand = localExecCommand.replace(Context.topologyBinaryFile(config),
                                                          EcsContext.ecsClusterBinary(config));
    clusterExecCommand = clusterExecCommand.replaceAll("\"", "'");
    return clusterExecCommand;
  }

  private String getDockerFileContent(String execCommand, int container) throws IOException {

    String commandBuiler = new String(Files.readAllBytes(
                                       Paths.get(EcsContext.ecsComposeTemplate(config))));
    commandBuiler = commandBuiler.replaceAll("TOPOLOGY_NAME",
                                              EcsContext.topologyName(config));
    commandBuiler = commandBuiler.replaceAll("CONTAINER_NUMBER",
                                              "executor" + String.valueOf(container));
    commandBuiler = commandBuiler.replace("HERON_EXECUTOR", execCommand);
    commandBuiler = commandBuiler.replace("FREEPORTS", nfreePorts);
    //System.out.println("commandBuiler  :\n" + commandBuiler);
    return commandBuiler;
  }

  private String formHeronExecCommand(String[] inStringArray) {
    StringBuilder builder = new StringBuilder();
    for (String string : inStringArray) {
      if (builder.length() > 0) {
        builder.append(" ");
      }
      builder.append(string);
    }
    builder.append(" ");
    builder.append(EcsContext.AmiInstanceUrl(config));
    String stringToReturn = builder.toString();
    return stringToReturn;
  }

  /**
   * Schedule the provided packed plan
   */
  @Override
  public boolean onSchedule(PackingPlan packing) {
    LOG.info("Starting to deploy topology: " + EcsContext.topologyName(config));
    LOG.info("Starting executor for TMaster");
    startExecutor(0);
      // for each container, run its own executor
    for (PackingPlan.ContainerPlan container : packing.getContainers()) {
      startExecutor(container.getId());
    }
    LOG.info("Executor for each container have been started.");
    return true;
  }

  public List<String> getJobLinks() {
    return null;
  }

  @Override
  public boolean onKill(Scheduler.KillTopologyRequest request) {
    ShellUtils.runProcess(EcsContext.composeStopCmd(config), null);
    return true;
  }

  @Override
  public boolean onRestart(Scheduler.RestartTopologyRequest request) {
    return false;
  }

  @Override
  public boolean onUpdate(Scheduler.UpdateTopologyRequest request) {
    return false;
  }

}
