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
//import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

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


  protected int startExecutorSyncProcess(int container) {
    String executingInShell = new String();
    executingInShell = getExecutorCommand(container)[0];
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
    LOG.info("Listing each of tasks started.");
   // List<String> jobLinks = new ArrayList<String>();
    //jobLinks = getJobLinks();
    return true;
  }

  @Override
  public List<String> getJobLinks() {
    List<String> list = new ArrayList<String>();
    StringBuilder familyListcmd = new StringBuilder();
    //String tempStr = "aws ecs list-task-definition-families --family-prefix ecscompose-";
    familyListcmd.append(EcsContext.composeFamilyName(config));
    familyListcmd.append(EcsContext.topologyName(config));
    //tempStr = tempStr + EcsContext.topologyName(config);
    LOG.info("final list cmd:" + familyListcmd);
    StringBuilder stdout = new StringBuilder();
    StringBuilder stderr = new StringBuilder();
    List<String> familyString;
    int status = ShellUtils.runProcess(familyListcmd.toString(), stdout);
    if (status != 0) {
      LOG.severe(String.format(
          "Failed to run process. Command=%s, STDOUT=%s, STDERR=%s",
            familyListcmd.toString(), stdout, stderr));
    } else {
      String listjsonString = stdout.toString();
      try {
        familyString = parseJsonName(listjsonString, EcsContext.composeListby(config));
        for (String familyName : familyString) {
          StringBuilder listout = new StringBuilder();
          StringBuilder listerr = new StringBuilder();
          StringBuilder taskListcmd = new StringBuilder();
          taskListcmd.append(EcsContext.composeListCmd(config));
          taskListcmd.append("  ");
          taskListcmd.append(familyName);
          int jobListstatus = ShellUtils.runProcess(taskListcmd.toString(), listout);
          if (jobListstatus != 0) {
            LOG.severe(String.format(
                "Failed to run process. Command=%s, STDOUT=%s, STDERR=%s",
                taskListcmd.toString(), listout, listerr));
          } else {
            List<String>  taskString = parseJsonName(listout.toString(),
                                                      EcsContext.composeTaskTag(config));
            for (String taskId : taskString) {
              list.add(taskId);
            }
          }
        }
      } catch (JsonParseException e) {
        LOG.severe("Unable to get list due to Parsing issues");
      } catch (IOException ioe) {
        LOG.severe("Unable to get list due to IO issues");
      }
    }
    return list;
  }

  private List<String> parseJsonName(String jString, String jName) throws JsonParseException,
      IOException {
    ObjectMapper mapper = new ObjectMapper();
    JsonNode actualObj = mapper.readTree(jString);
    //JsonNode jsonNode1 = actualObj.get("families");
    List<String> jsonList = new ArrayList<String>();
    final JsonNode arrNode = new ObjectMapper().readTree(jString).get(jName);
    if (arrNode.isArray()) {
      for (final JsonNode objNode : arrNode) {
        String taskDefn = objNode.asText();
        jsonList.add(taskDefn.toString());
      }
    } else {
      jsonList.add(arrNode.textValue());
    }
    return jsonList;
  }

  @Override
  public boolean onKill(Scheduler.KillTopologyRequest request) {
    ShellUtils.runProcess(EcsContext.composeStopCmd(config), null);
    return true;
  }

  @Override
  public boolean onRestart(Scheduler.RestartTopologyRequest request) {
    // TODO(ananthgs): Need to see if re-starting each task is good.
    LOG.severe("Topology onRestart not implemented by this scheduler Please use kill & start.");
    return false;
  }

  @Override
  public boolean onUpdate(Scheduler.UpdateTopologyRequest request) {
    // TODO(ananthgs): Need to decide how to get pplans and  update
    LOG.severe("Topology onUpdate not implemented by this scheduler.");
    return false;
  }

}
