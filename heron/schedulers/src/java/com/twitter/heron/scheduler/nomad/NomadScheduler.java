//  Copyright 2017 Twitter. All rights reserved.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
package com.twitter.heron.scheduler.nomad;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import com.google.common.base.Optional;
import com.hashicorp.nomad.apimodel.Job;
import com.hashicorp.nomad.apimodel.JobListStub;
import com.hashicorp.nomad.apimodel.NetworkResource;
import com.hashicorp.nomad.apimodel.Port;
import com.hashicorp.nomad.apimodel.Resources;
import com.hashicorp.nomad.apimodel.Task;
import com.hashicorp.nomad.apimodel.TaskGroup;
import com.hashicorp.nomad.apimodel.Template;
import com.hashicorp.nomad.javasdk.EvaluationResponse;
import com.hashicorp.nomad.javasdk.NomadApiClient;
import com.hashicorp.nomad.javasdk.NomadApiConfiguration;
import com.hashicorp.nomad.javasdk.NomadException;
import com.hashicorp.nomad.javasdk.ServerQueryResponse;

import com.twitter.heron.proto.scheduler.Scheduler;
import com.twitter.heron.scheduler.UpdateTopologyManager;
import com.twitter.heron.scheduler.utils.Runtime;
import com.twitter.heron.scheduler.utils.SchedulerUtils;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Context;
import com.twitter.heron.spi.packing.PackingPlan;
import com.twitter.heron.spi.scheduler.IScheduler;

@SuppressWarnings("IllegalCatch")
public class NomadScheduler implements IScheduler {

  private static final Logger LOG = Logger.getLogger(NomadScheduler.class.getName());

  private Config clusterConfig;
  private Config localConfig;
  private Config runtimeConfig;
  private UpdateTopologyManager updateTopologyManager;

  @Override
  public void initialize(Config config, Config runtime) {
    this.localConfig = config;
    this.clusterConfig = Config.toClusterMode(config);
    this.runtimeConfig = runtime;
    this.updateTopologyManager =
        new UpdateTopologyManager(config, runtime, Optional.absent());
  }

  @Override
  public void close() {
    // don't need to do anything
  }

  @Override
  public boolean onSchedule(PackingPlan packing) {
    if (packing == null || packing.getContainers().isEmpty()) {
      LOG.severe("No container requested. Can't schedule");
      return false;
    }

    NomadApiClient apiClient = getApiClient(NomadContext.getSchedulerURI(this.clusterConfig));
    try {
      List<Job> jobs = getJobs(packing);
      startJobs(apiClient, jobs.toArray(new Job[jobs.size()]));
    } catch (RuntimeException e) {
      LOG.log(Level.SEVERE, "Failed to deploy topology "
          + Runtime.topologyName(this.runtimeConfig) + " with error: " + e.getMessage(), e);
      return false;
    } finally {
      closeClient(apiClient);
    }
    return true;
  }

  @Override
  public List<String> getJobLinks() {
    List<String> jobLinks = new LinkedList<>();
    String schedulerUri = NomadContext.getSchedulerURI(this.clusterConfig);
    jobLinks.add(schedulerUri + NomadConstants.JOB_LINK);
    return jobLinks;
  }

  @Override
  public boolean onKill(Scheduler.KillTopologyRequest request) {
    String topologyName = request.getTopologyName();

    LOG.fine("Killing Topology " + topologyName);
    NomadApiClient apiClient = getApiClient(NomadContext.getSchedulerURI(this.clusterConfig));

    try {
      List<Job> jobs = getTopologyJobs(apiClient, topologyName);
      killJobs(apiClient, jobs.toArray(new Job[jobs.size()]));
    } catch (RuntimeException e) {
      LOG.log(Level.SEVERE, "Failed to kill topology " + topologyName
          + " with error: " + e.getMessage(), e);
      return false;
    } finally {
      closeClient(apiClient);
    }
    return true;
  }

  @Override
  public boolean onRestart(Scheduler.RestartTopologyRequest request) {
    LOG.fine("Restarting Topology " + request.getTopologyName()
        + " container " + request.getContainerIndex());
    NomadApiClient apiClient = getApiClient(NomadContext.getSchedulerURI(this.clusterConfig));

    String topologyName = request.getTopologyName();
    int containerIndex = request.getContainerIndex();

    try {
      if (containerIndex == -1) {
        // restarting whole topology
        List<Job> jobs = getTopologyJobs(apiClient, topologyName);
        restartJobs(apiClient, jobs.toArray(new Job[jobs.size()]));
      } else {
        // restarting single container
        Job job = getTopologyContainerJob(apiClient, topologyName, containerIndex);
        restartJobs(apiClient, job);
      }
    } catch (RuntimeException e) {
      LOG.log(Level.SEVERE, "Failed to restart topology "
          + topologyName + " with error: " + e.getMessage(), e);
      return false;
    } finally {
      closeClient(apiClient);
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

  /**
   * HELPER FUNCTIONS
   **/

  static NomadApiClient getApiClient(String uri) {
    return new NomadApiClient(
        new NomadApiConfiguration.Builder()
            .setAddress(uri)
            .build());
  }

  static void closeClient(NomadApiClient apiClient) {
    try {
      apiClient.close();
    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Problem closing client: " + e.getMessage(), e);
    }
  }

  List<Job> getJobs(PackingPlan packing) {
    List<Job> ret = new LinkedList<>();
    for (int i = 0; i < Runtime.numContainers(this.runtimeConfig); i++) {
      Optional<PackingPlan.ContainerPlan> containerPlan = packing.getContainer(i);
      ret.add(getJob(i, containerPlan));
    }
    return ret;
  }

  Job getJob(int containerIndex, Optional<PackingPlan.ContainerPlan> containerPlan) {

    String topologyName = Runtime.topologyName(this.runtimeConfig);
    String topologyId = Runtime.topologyId(this.runtimeConfig);
    Job job = new Job();
    job.setId(getJobId(topologyId, containerIndex));
    job.setName(getJobId(topologyName, containerIndex));
    job.addTaskGroups(getTaskGroup(getJobId(topologyName, containerIndex),
        containerIndex, containerPlan));
    job.setDatacenters(Arrays.asList(NomadConstants.NOMAD_DEFAULT_DATACENTER));
    job.setMeta(getMetaData(this.runtimeConfig, containerPlan));
    return job;
  }

  TaskGroup getTaskGroup(String groupName, int containerIndex,
                         Optional<PackingPlan.ContainerPlan> containerPlan) {
    TaskGroup taskGroup = new TaskGroup();
    taskGroup.setCount(1);
    taskGroup.setName(groupName);
    taskGroup.addTasks(getTask(groupName, containerIndex, containerPlan));
    return taskGroup;
  }

  Task getTask(String taskName, int containerIndex,
               Optional<PackingPlan.ContainerPlan> containerPlan) {

    String executorBinary = Context.executorBinary(this.clusterConfig);
    // get arguments for heron executor command
    String[] executorArgs = SchedulerUtils.executorCommandArgs(
        this.clusterConfig, this.runtimeConfig, NomadConstants.EXECUTOR_PORTS,
        String.valueOf(containerIndex));
    // get complete heron executor command
    String executorCmd = executorBinary + " " + String.join(" ", executorArgs);
    // get heron_downloader command for downloading topology package
    String topologyDownloadCmd = getFetchCommand(this.clusterConfig, this.runtimeConfig);
    // read nomad heron executor start up script from file
    String heronNomadScript = getHeronNomadScript(this.localConfig);

    Task task = new Task();
    task.setName(taskName);
    task.setDriver(NomadConstants.NOMAD_RAW_EXEC);
    // call nomad heron start up script
    task.addConfig(NomadConstants.NOMAD_TASK_COMMAND, NomadConstants.SHELL_CMD);
    String[] args = {NomadConstants.NOMAD_HERON_SCRIPT_NAME};
    task.addConfig(NomadConstants.NOMAD_TASK_COMMAND_ARGS, args);
    Template template = new Template();
    template.setEmbeddedTmpl(heronNomadScript);
    template.setDestPath(NomadConstants.NOMAD_HERON_SCRIPT_NAME);
    task.addTemplates(template);

    Resources resourceReqs = new Resources();
    // configure nomad to allocate dynamic ports
    Port[] ports = new Port[NomadConstants.EXECUTOR_PORTS.size()];
    int i = 0;
    for (SchedulerUtils.ExecutorPort port : NomadConstants.EXECUTOR_PORTS.keySet()) {
      ports[i] = new Port().setLabel(port.getName().replace("-", "_"));
      i++;
    }

    resourceReqs.addNetworks(new NetworkResource().addDynamicPorts(ports));
    // set resources requests
    if (containerPlan.isPresent()) {
      // set memory requirements
      long memoryReqMb = containerPlan.get().getRequiredResource().getRam().asMegabytes();
      resourceReqs.setMemoryMb(longToInt(memoryReqMb));

      // set cpu requirements
      double coresReq = containerPlan.get().getRequiredResource().getCpu();
      double coresReqFreq = NomadContext.getCoreFreqMapping(this.clusterConfig) * coresReq;
      resourceReqs.setCpu(Integer.valueOf((int) Math.round(coresReqFreq)));

      // set disk requirements
      long diskReqMb = containerPlan.get().getRequiredResource().getDisk().asMegabytes();
      resourceReqs.setDiskMb(longToInt(diskReqMb));
    }

    task.setResources(resourceReqs);

    // set enviroment variables used int the heron nomad start up script
    Map<String, String> envVars = new HashMap<>();
    envVars.put(NomadConstants.HERON_NOMAD_WORKING_DIR,
        NomadContext.workingDirectory(this.clusterConfig) + "/container-"
            + String.valueOf(containerIndex));
    envVars.put(NomadConstants.HERON_CORE_PACKAGE_URI,
        NomadContext.corePackageUri(this.localConfig));
    envVars.put(NomadConstants.HERON_TOPOLOGY_DOWNLOAD_CMD, topologyDownloadCmd);
    envVars.put(NomadConstants.HERON_EXECUTOR_CMD, executorCmd);
    task.setEnv(envVars);

    return task;
  }

  static List<JobListStub> getJobList(NomadApiClient apiClient) {
    ServerQueryResponse<List<JobListStub>> response;
    try {
      response = apiClient.getJobsApi().list();
    } catch (IOException | NomadException e) {
      LOG.log(Level.SEVERE, "Error when attempting to fetch job list", e);
      throw new RuntimeException(e);
    }
    return response.getValue();
  }

  static List<Job> getTopologyJobs(NomadApiClient apiClient, String topologyName) {
    List<JobListStub> jobs = getJobList(apiClient);
    List<Job> ret = new LinkedList<>();
    for (JobListStub job : jobs) {
      Job jobActual;
      try {
        jobActual = apiClient.getJobsApi().info(job.getId()).getValue();
      } catch (IOException | NomadException e) {
        String msg = "Failed to retrieve job info for job " + job.getId()
            + " part of topology " + topologyName;
        LOG.log(Level.SEVERE, msg, e);
        throw new RuntimeException(msg, e);
      }
      Map<String, String> metaData = jobActual.getMeta();
      if (metaData.get(NomadConstants.NOMAD_TOPOLOGY_NAME).equals(topologyName)) {
        ret.add(jobActual);
      }
    }
    return ret;
  }

  static Job getTopologyContainerJob(NomadApiClient apiClient, String topologyName,
                                     int containerIndex) {
    List<Job> jobs = getTopologyJobs(apiClient, topologyName);
    for (Job job : jobs) {
      if (job.getMeta().get(NomadConstants.NOMAD_TOPOLOGY_CONTAINER_INDEX)
          .equals(String.valueOf(containerIndex))) {
        return job;
      }
    }
    throw new RuntimeException("Container " + containerIndex
        + " does not exist for topology " + topologyName);
  }

  static void killJobs(NomadApiClient apiClient, Job... jobs) {
    for (Job job : jobs) {
      LOG.fine("Killing job " + job.getId());
      try {
        apiClient.getJobsApi().deregister(job.getId());
      } catch (IOException | NomadException e) {
        String errorMsg = "Failed to kill job " + job.getId();
        LOG.log(Level.SEVERE, errorMsg, e);
        throw new RuntimeException(errorMsg, e);
      }
    }
  }

  static void restartJobs(NomadApiClient apiClient, Job... jobs) {
    LOG.fine("Restarting jobs: " + Arrays.asList(jobs)
        .stream().map(a -> a.getId()).collect(Collectors.toList()));
    killJobs(apiClient, jobs);
    startJobs(apiClient, jobs);
  }

  static void startJobs(NomadApiClient apiClient, Job... jobs) {

    for (Job job : jobs) {
      LOG.fine("Starting job " + job.getId());
      try {
        EvaluationResponse response = apiClient.getJobsApi().register(job);
        LOG.info("response: " + response);
      } catch (IOException | NomadException e) {
        String msg = "Failed to submit job "
            + job.getId() + " with error: " + e.getMessage()
            + " with job spec: " + job.toString();
        LOG.log(Level.SEVERE, msg, e);
        throw new RuntimeException(msg, e);
      }
    }
  }

  public static Map<String, String> getMetaData(
      Config runtimeConfig,
      Optional<PackingPlan.ContainerPlan> containerPlan) {
    String topologyName = Runtime.topologyName(runtimeConfig);
    String topologyId = Runtime.topologyId(runtimeConfig);
    Map<String, String> metaData = new HashMap<>();
    metaData.put(NomadConstants.NOMAD_TOPOLOGY_NAME, topologyName);
    metaData.put(NomadConstants.NOMAD_TOPOLOGY_ID, topologyId);
    if (containerPlan.isPresent()) {
      metaData.put(NomadConstants.NOMAD_TOPOLOGY_CONTAINER_INDEX,
          String.valueOf(containerPlan.get().getId()));
    }
    return metaData;
  }

  static String getJobId(String topologyName, int containerIndex) {
    return String.format("%s-%d", topologyName, containerIndex);
  }

  static String getHeronNomadScript(Config config) {
    try {
      return new String(Files.readAllBytes(Paths.get(
          NomadContext.getHeronNomadPath(config))), StandardCharsets.UTF_8);
    } catch (IOException e) {

      String msg = "Failed to read heron nomad script from "
          + NomadContext.getHeronNomadPath(config) + " . Please check file path!";

      LOG.log(Level.SEVERE, msg, e);
      throw new RuntimeException(msg, e);
    }
  }

  /**
   * Get the command that will be used to retrieve the topology JAR
   */
  static String getFetchCommand(Config config, Config runtime) {
    return String.format("%s %s .", Context.downloaderBinary(config),
        Runtime.topologyPackageUri(runtime).toString());
  }

  static int longToInt(long val) {
    if (val < Integer.MIN_VALUE || val > Integer.MAX_VALUE) {
      throw new IllegalArgumentException(val + " is too large and cannot be cast to an Integer");
    }
    return (int) val;
  }
}
