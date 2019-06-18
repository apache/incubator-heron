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

package org.apache.heron.scheduler.nomad;

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
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import com.google.common.base.Optional;
import com.hashicorp.nomad.apimodel.Job;
import com.hashicorp.nomad.apimodel.JobListStub;
import com.hashicorp.nomad.apimodel.NetworkResource;
import com.hashicorp.nomad.apimodel.Port;
import com.hashicorp.nomad.apimodel.Resources;
import com.hashicorp.nomad.apimodel.Service;
import com.hashicorp.nomad.apimodel.ServiceCheck;
import com.hashicorp.nomad.apimodel.Task;
import com.hashicorp.nomad.apimodel.TaskGroup;
import com.hashicorp.nomad.apimodel.Template;
import com.hashicorp.nomad.javasdk.EvaluationResponse;
import com.hashicorp.nomad.javasdk.NomadApiClient;
import com.hashicorp.nomad.javasdk.NomadApiConfiguration;
import com.hashicorp.nomad.javasdk.NomadException;
import com.hashicorp.nomad.javasdk.ServerQueryResponse;

import org.apache.heron.metricsmgr.MetricsSinksConfig;
import org.apache.heron.metricsmgr.sink.PrometheusSink;
import org.apache.heron.proto.scheduler.Scheduler;
import org.apache.heron.scheduler.UpdateTopologyManager;
import org.apache.heron.scheduler.utils.Runtime;
import org.apache.heron.scheduler.utils.SchedulerUtils;
import org.apache.heron.spi.common.Config;
import org.apache.heron.spi.common.ConfigLoader;
import org.apache.heron.spi.common.Context;
import org.apache.heron.spi.packing.PackingPlan;
import org.apache.heron.spi.packing.Resource;
import org.apache.heron.spi.scheduler.IScheduler;

import static org.apache.heron.scheduler.nomad.NomadConstants.METRICS_PORT;

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
    this.clusterConfig = Config.toClusterMode(Config.newBuilder()
        .putAll(config)
        .putAll(ConfigLoader.loadConfig(
            Context.heronHome(config), Context.heronConf(config),
            null, Context.apiserverOverrideFile(config) != null
                ? Context.apiserverOverrideFile(config) : Context.overrideFile(config)))
        .build());
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

    NomadApiClient apiClient = getApiClient(NomadContext.getSchedulerURI(this.localConfig));
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
    String schedulerUri = NomadContext.getSchedulerURI(this.localConfig);
    jobLinks.add(schedulerUri + NomadConstants.JOB_LINK);
    return jobLinks;
  }

  @Override
  public boolean onKill(Scheduler.KillTopologyRequest request) {
    String topologyName = request.getTopologyName();

    LOG.fine("Killing Topology " + topologyName);
    NomadApiClient apiClient = getApiClient(NomadContext.getSchedulerURI(this.localConfig));

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
    NomadApiClient apiClient = getApiClient(NomadContext.getSchedulerURI(this.localConfig));

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

    PackingPlan homogeneousPackingPlan = getHomogeneousPackingPlan(packing);

    Resource resource = getHomogeneousContainerResource(homogeneousPackingPlan);

    for (int i = 0; i < Runtime.numContainers(this.runtimeConfig); i++) {
      Optional<PackingPlan.ContainerPlan> containerPlan = homogeneousPackingPlan.getContainer(i);
      ret.add(getJob(i, containerPlan, resource));
    }
    return ret;
  }

  Job getJob(int containerIndex, Optional<PackingPlan.ContainerPlan> containerPlan,
             Resource containerResource) {

    String topologyName = Runtime.topologyName(this.runtimeConfig);
    String topologyId = Runtime.topologyId(this.runtimeConfig);
    Job job = new Job();
    job.setId(getJobId(topologyId, containerIndex));
    job.setName(getJobId(topologyName, containerIndex));
    job.addTaskGroups(getTaskGroup(getJobId(topologyName, containerIndex),
        containerIndex, containerResource));
    job.setDatacenters(Arrays.asList(NomadConstants.NOMAD_DEFAULT_DATACENTER));
    job.setMeta(getMetaData(this.runtimeConfig, containerPlan));
    return job;
  }

  TaskGroup getTaskGroup(String groupName, int containerIndex, Resource containerResource) {
    TaskGroup taskGroup = new TaskGroup();
    taskGroup.setCount(1);
    taskGroup.setName(groupName);
    taskGroup.addTasks(getTask(groupName, containerIndex, containerResource));
    return taskGroup;
  }

  Task getTask(String taskName, int containerIndex, Resource containerResource) {

    String nomadDriver = NomadContext.getHeronNomadDriver(this.localConfig);
    Task task = new Task();

    if (nomadDriver.equals(NomadConstants.NomadDriver.RAW_EXEC.getName())) {

      getTaskSpecRawDriver(task, taskName, containerIndex);

    } else if (nomadDriver.equals(NomadConstants.NomadDriver.DOCKER.getName())) {

      getTaskSpecDockerDriver(task, taskName, containerIndex);

    } else {
      throw new IllegalArgumentException("Invalid Nomad driver specified: " + nomadDriver);
    }

    // set resources requests
    Resources resourceReqs = new Resources();
    // configure nomad to allocate dynamic ports
    Port[] ports = new Port[NomadConstants.EXECUTOR_PORTS.size()];
    int i = 0;
    for (SchedulerUtils.ExecutorPort port : NomadConstants.EXECUTOR_PORTS.keySet()) {
      ports[i] = new Port().setLabel(port.getName().replace("-", "_"));
      i++;
    }

    NetworkResource networkResource = new NetworkResource();
    networkResource.addDynamicPorts(ports);

    // set memory requirements
    long memoryReqMb = containerResource.getRam().asMegabytes();
    resourceReqs.setMemoryMb(longToInt(memoryReqMb));

    // set CPU requirements
    double coresReq = containerResource.getCpu();
    double coresReqFreq = NomadContext.getCoreFreqMapping(this.localConfig) * coresReq;
    resourceReqs.setCpu(Integer.valueOf((int) Math.round(coresReqFreq)));

    // set disk requirements
    long diskReqMb = containerResource.getDisk().asMegabytes();
    resourceReqs.setDiskMb(longToInt(diskReqMb));

    // allocate dynamic port for prometheus/websink metrics
    String prometheusPortFile = getPrometheusMetricsFile(this.localConfig);
    if (prometheusPortFile == null) {
      LOG.severe("Failed to find port file for Prometheus metrics. "
          + "Please check metrics sinks configurations");
    } else {
      networkResource.addDynamicPorts(new Port().setLabel(METRICS_PORT));
      task.addEnv(NomadConstants.METRICS_PORT_FILE, prometheusPortFile);

      if (NomadContext.getHeronNomadMetricsServiceRegister(this.localConfig)) {
        // getting tags for service
        List<String> tags = new LinkedList<>();
        tags.add(String.format("%s-%s",
            Runtime.topologyName(this.runtimeConfig), containerIndex));
        tags.addAll(Arrays.asList(
            NomadContext.getHeronNomadMetricsServiceAdditionalTags(this.localConfig)));
        //register metrics service with consul
        Service service = new Service()
            .setName(
                getMetricsServiceName(Runtime.topologyName(this.runtimeConfig), containerIndex))
            .setPortLabel(METRICS_PORT)
            .setTags(tags)
            .addChecks(new ServiceCheck().setType(NomadConstants.NOMAD_SERVICE_CHECK_TYPE)
                .setPortLabel(METRICS_PORT)
                .setInterval(TimeUnit.NANOSECONDS.convert(
                    NomadContext.getHeronNomadMetricsServiceCheckIntervalSec(this.localConfig),
                    TimeUnit.SECONDS))
                .setTimeout(TimeUnit.NANOSECONDS.convert(
                    NomadContext.getHeronNomadMetricsServiceCheckTimeoutSec(this.localConfig),
                    TimeUnit.SECONDS)));

        task.addServices(service);
      }
    }

    resourceReqs.addNetworks(networkResource);
    task.setResources(resourceReqs);
    return task;
  }

  /**
   * Get the task spec for using the docker driver in Nomad
   * In docker mode, Heron will be use in docker containers
   */
  Task getTaskSpecDockerDriver(Task task, String taskName, int containerIndex) {

    String executorBinary = Context.executorBinary(this.clusterConfig);
    // get arguments for heron executor command
    String[] executorArgs = SchedulerUtils.executorCommandArgs(
        this.clusterConfig, this.runtimeConfig, NomadConstants.EXECUTOR_PORTS,
        String.valueOf(containerIndex));

    // get complete heron executor command
    String executorCmd = executorBinary + " " + String.join(" ", executorArgs);
    // get heron_downloader command for downloading topology package
    String topologyDownloadCmd = getFetchCommand(this.clusterConfig,
        this.clusterConfig, this.runtimeConfig);

    task.setName(taskName);
    // use nomad driver
    task.setDriver(NomadConstants.NomadDriver.DOCKER.getName());

    // set docker image to use
    task.addConfig(NomadConstants.NOMAD_IMAGE,
        NomadContext.getHeronExecutorDockerImage(this.localConfig));

    task.addConfig(NomadConstants.NOMAD_TASK_COMMAND, NomadConstants.SHELL_CMD);
    task.addConfig(NomadConstants.NETWORK_MODE,
        NomadContext.getHeronNomadNetworkMode(this.localConfig));

    String setMetricsPortFileCmd = getSetMetricsPortFileCmd();

    String[] args = {"-c", String.format("%s && %s && %s",
        topologyDownloadCmd, setMetricsPortFileCmd, executorCmd)};

    task.addConfig(NomadConstants.NOMAD_TASK_COMMAND_ARGS, args);

    Map<String, String> envVars = new HashMap<>();
    envVars.put(NomadConstants.HOST, "${attr.unique.network.ip-address}");
    task.setEnv(envVars);

    return task;
  }

  /**
   * Get the task spec for using raw_exec driver in Nomad
   * In raw exec mode, Heron will be run directly on the machine
   */
  Task getTaskSpecRawDriver(Task task, String taskName, int containerIndex) {
    String executorBinary = Context.executorBinary(this.clusterConfig);
    // get arguments for heron executor command
    String[] executorArgs = SchedulerUtils.executorCommandArgs(
        this.clusterConfig, this.runtimeConfig, NomadConstants.EXECUTOR_PORTS,
        String.valueOf(containerIndex));
    // get complete heron executor command
    String executorCmd = executorBinary + " " + String.join(" ", executorArgs);
    // get heron_downloader command for downloading topology package
    String topologyDownloadCmd = getFetchCommand(this.localConfig,
        this.clusterConfig, this.runtimeConfig);
    // read nomad heron executor start up script from file
    String heronNomadScript = getHeronNomadScript(this.localConfig);

    task.setName(taskName);
    // use raw_exec driver
    task.setDriver(NomadConstants.NomadDriver.RAW_EXEC.getName());
    // call nomad heron start up script
    task.addConfig(NomadConstants.NOMAD_TASK_COMMAND, NomadConstants.SHELL_CMD);
    String[] args = {NomadConstants.NOMAD_HERON_SCRIPT_NAME};
    task.addConfig(NomadConstants.NOMAD_TASK_COMMAND_ARGS, args);
    Template template = new Template();
    template.setEmbeddedTmpl(heronNomadScript);
    template.setDestPath(NomadConstants.NOMAD_HERON_SCRIPT_NAME);
    task.addTemplates(template);

    // configure nomad to allocate dynamic ports
    Port[] ports = new Port[NomadConstants.EXECUTOR_PORTS.size()];
    int i = 0;
    for (SchedulerUtils.ExecutorPort port : NomadConstants.EXECUTOR_PORTS.keySet()) {
      ports[i] = new Port().setLabel(port.getName().replace("-", "_"));
      i++;
    }

    // set enviroment variables used int the heron nomad start up script
    Map<String, String> envVars = new HashMap<>();
    envVars.put(NomadConstants.HERON_NOMAD_WORKING_DIR,
        NomadContext.workingDirectory(this.localConfig) + "/container-"
            + String.valueOf(containerIndex));

    if (NomadContext.useCorePackageUri(this.localConfig)) {
      envVars.put(NomadConstants.HERON_USE_CORE_PACKAGE_URI, "true");
      envVars.put(NomadConstants.HERON_CORE_PACKAGE_URI,
          NomadContext.corePackageUri(this.localConfig));
    } else {
      envVars.put(NomadConstants.HERON_USE_CORE_PACKAGE_URI, "false");
      envVars.put(NomadConstants.HERON_CORE_PACKAGE_DIR,
          NomadContext.corePackageDirectory(this.localConfig));
    }

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
      if (metaData != null && metaData.containsKey(NomadConstants.NOMAD_TOPOLOGY_NAME)) {
        if (metaData.get(NomadConstants.NOMAD_TOPOLOGY_NAME).equals(topologyName)) {
          ret.add(jobActual);
        }
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
      LOG.fine("Job spec: " + job.toString());
      try {
        EvaluationResponse response = apiClient.getJobsApi().register(job);
        LOG.fine("response: " + response);
      } catch (IOException | NomadException e) {
        String msg = "Failed to submit job "
            + job.getId() + " with error: " + e.getMessage();

        LOG.log(Level.FINE, msg + " with job spec: " + job.toString());
        throw new RuntimeException(msg, e);
      }
    }
  }

  static Map<String, String> getMetaData(
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
  static String getFetchCommand(Config localConfig, Config clusterConfig, Config runtime) {
    return String.format("%s -u %s -f . -m local -p %s -d %s",
        Context.downloaderBinary(clusterConfig),
        Runtime.topologyPackageUri(runtime).toString(), Context.heronConf(localConfig),
        Context.heronHome(clusterConfig));
  }

  static int longToInt(long val) {
    if (val < Integer.MIN_VALUE || val > Integer.MAX_VALUE) {
      throw new IllegalArgumentException(val + " is too large and cannot be cast to an Integer");
    }
    return (int) val;
  }

  PackingPlan getHomogeneousPackingPlan(PackingPlan packingPlan) {
    // Align resources to maximal requested resource
    PackingPlan updatedPackingPlan = packingPlan.cloneWithHomogeneousScheduledResource();
    SchedulerUtils.persistUpdatedPackingPlan(Runtime.topologyName(this.runtimeConfig),
        updatedPackingPlan, Runtime.schedulerStateManagerAdaptor(this.runtimeConfig));

    return updatedPackingPlan;
  }

  Resource getHomogeneousContainerResource(PackingPlan homogeneousPackingPlan) {
    return homogeneousPackingPlan.getContainers().iterator().next().getRequiredResource();
  }

  static String getPrometheusMetricsFile(Config config) {
    MetricsSinksConfig metricsSinksConfig;
    try {
      metricsSinksConfig = new MetricsSinksConfig(Context.metricsSinksFile(config),
                                                  Context.overrideFile(config));
    } catch (IOException e) {
      return null;
    }

    String prometheusSinkId = null;
    Map<String, Object> prometheusSinkConfig = null;
    for (String sinkId : metricsSinksConfig.getSinkIds()) {
      Map<String, Object> sinkConfig = metricsSinksConfig.getConfigForSink(sinkId);
      Object className = sinkConfig.get(MetricsSinksConfig.CONFIG_KEY_CLASSNAME);
      if (className != null && className instanceof String) {
        if (PrometheusSink.class.getName().equals(className)) {
          prometheusSinkId = sinkId;
          prometheusSinkConfig = sinkConfig;
        }
      }
    }
    if (prometheusSinkId == null || prometheusSinkConfig == null) {
      return null;
    }

    String prometheusMetricsPortFile = PrometheusSink.getServerPortFile(prometheusSinkConfig);
    return prometheusMetricsPortFile;
  }

  static String getMetricsServiceName(String topologyName, int containerIndex) {
    return String.format("metrics-heron-%s-%s", topologyName, containerIndex);
  }

  static String getSetMetricsPortFileCmd() {
    return String.format("echo ${NOMAD_PORT_%s} > ${%s}",
        NomadConstants.METRICS_PORT, NomadConstants.METRICS_PORT_FILE);
  }
}
