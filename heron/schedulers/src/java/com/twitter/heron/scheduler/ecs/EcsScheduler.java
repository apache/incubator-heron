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
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import org.apache.commons.io.IOUtils;

import com.twitter.heron.proto.scheduler.Scheduler;
import com.twitter.heron.scheduler.utils.SchedulerUtils;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Context;
import com.twitter.heron.spi.packing.PackingPlan;
import com.twitter.heron.spi.scheduler.IScheduler;
import com.twitter.heron.spi.utils.ShellUtils;


/**
 * Created by ananth on 4/19/17.
 */
public class EcsScheduler implements IScheduler {
  private static final Logger LOG = Logger.getLogger(EcsScheduler.class.getName());
  private Config config;
  private Config runtime;

  private volatile boolean isTopologyKilled = false;
  private File tempDockerFile = null;
  @Override
  public void initialize(Config mConfig, Config mRuntime) {
    this.config = Config.toClusterMode(mConfig);
    this.runtime = mRuntime;

  }

  public void close() {

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
    for (int i = 0; i < SchedulerUtils.PORTS_REQUIRED_FOR_EXECUTOR; i++) {
      //freePorts.add(SysUtils.getFreePort());
      freePorts.add(5000 + (i + (container * 10)));
    }
    String[] executorCmd = SchedulerUtils.executorCommand(config, runtime, container, freePorts);
    String finalExecCommand = setClusterValues(formHeronExecCommand(executorCmd));
    String ecsTaskProject =  EcsContext.topologyName(config) + "_" + String.valueOf(container);
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
      e.printStackTrace();
    } finally {
      IOUtils.closeQuietly(dockerFilestream);
    }
    String dockerComposeFileName = " --file " + tempDockerFile;
    String finalCommand = EcsContext.COMPOSE_CMD + ecsTaskProject + dockerComposeFileName;
    finalCommand = finalCommand + EcsContext.UP;
    System.out.println("final Ecs Task command " + finalCommand);
    tempDockerFile.deleteOnExit();
    return  new String[] {finalCommand};

  }

  public String setClusterValues(String localExecCommand) {
    String clusterExecCommand = localExecCommand.replace(Context.topologyBinaryFile(config),
                                                           EcsContext.ECS_CLUSTER_BINARY);
    // line below can be removed once the Cluster JVM TODO is resolved
    clusterExecCommand = clusterExecCommand.replace(Context.clusterJavaHome(config),
                                                     EcsContext.DESTINATION_JVM);
    clusterExecCommand = clusterExecCommand.replaceAll("\"", "'");
    return clusterExecCommand;
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

  public String getDockerFileContent(String execCommand, int container) {
    String commandBuiler = EcsContext.PART1 + EcsContext.CMD;
    commandBuiler = commandBuiler + EcsContext.ECSNETWORK;
    commandBuiler = replacePortNumbers(container, commandBuiler);
    commandBuiler = commandBuiler.replaceAll("TOPOLOGY_NAME",
                                              EcsContext.topologyName(config));
    commandBuiler = commandBuiler.replaceAll("container_number",
                                              "executor" + String.valueOf(container));
    commandBuiler = commandBuiler.replace("heron_executor", execCommand);

    return commandBuiler;
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
