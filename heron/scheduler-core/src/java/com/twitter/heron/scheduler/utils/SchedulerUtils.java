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

package com.twitter.heron.scheduler.utils;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.xml.bind.DatatypeConverter;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.api.utils.TopologyUtils;
import com.twitter.heron.common.basics.FileUtils;
import com.twitter.heron.proto.scheduler.Scheduler;
import com.twitter.heron.proto.system.Common;
import com.twitter.heron.scheduler.ExecutorFlag;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Context;
import com.twitter.heron.spi.packing.PackingPlan;
import com.twitter.heron.spi.packing.PackingPlanProtoSerializer;
import com.twitter.heron.spi.scheduler.IScheduler;
import com.twitter.heron.spi.statemgr.SchedulerStateManagerAdaptor;
import com.twitter.heron.spi.utils.ShellUtils;

public final class SchedulerUtils {
  public static final int PORTS_REQUIRED_FOR_SCHEDULER = 1;
  public static final String SCHEDULER_COMMAND_LINE_PROPERTIES_OVERRIDE_OPTION = "P";

  private static final Logger LOG = Logger.getLogger(SchedulerUtils.class.getName());

  /**
   * Enum that defines the type of ports that an heron executor needs
   */

  public enum ExecutorPort {
    MASTER_PORT("master", true),
    TMASTER_CONTROLLER_PORT("tmaster-ctl", true),
    TMASTER_STATS_PORT("tmaster-stats", true),
    SHELL_PORT("shell-port", true),
    METRICS_MANAGER_PORT("metrics-mgr", true),
    SCHEDULER_PORT("scheduler", true),
    METRICS_CACHE_MASTER_PORT("metrics-cache-m", true),
    METRICS_CACHE_STATS_PORT("metrics-cache-s", true),
    CHECKPOINT_MANAGER_PORT("ckptmgr", true),
    JVM_REMOTE_DEBUGGER_PORTS("jvm-remote-debugger", false);

    private final String name;
    private final boolean required;

    ExecutorPort(String name, boolean required) {
      this.name = name;
      this.required = required;
    }

    public String getName() {
      return name;
    }

    public boolean isRequired() {
      return required;
    }

    public static String getPort(ExecutorPort executorPort,
                                 Map<ExecutorPort, String> portMap) {
      if (!portMap.containsKey(executorPort) && executorPort.isRequired()) {
        throw new RuntimeException("Required port " + executorPort.getName() + " not provided");
      }

      return portMap.get(executorPort);
    }

    public static Set<ExecutorPort> getRequiredPorts() {
      Set<ExecutorPort> executorPorts = new HashSet<>();
      for (ExecutorPort executorPort : ExecutorPort.values()) {
        if (executorPort.isRequired()) {
          executorPorts.add(executorPort);
        }
      }
      return executorPorts;
    }
  }

  private SchedulerUtils() {
  }

  /**
   * Utils method to construct the command to start heron-scheduler
   *
   * @param config The static Config
   * @param runtime The runtime Config
   * @param freePorts list of free ports
   * @return String[] representing the command to start heron-scheduler
   */
  public static String[] schedulerCommand(
      Config config,
      Config runtime,
      List<Integer> freePorts) {
    List<String> commands = new ArrayList<>();

    // The java executable should be "{JAVA_HOME}/bin/java"
    String javaExecutable = String.format("%s/%s", Context.clusterJavaHome(config), "bin/java");
    commands.add(javaExecutable);
    commands.add("-cp");

    // Construct the complete classpath to start scheduler
    String completeSchedulerProcessClassPath = String.format("%s:%s:%s",
        Context.schedulerClassPath(config),
        Context.packingClassPath(config),
        Context.stateManagerClassPath(config));
    commands.add(completeSchedulerProcessClassPath);
    commands.add("com.twitter.heron.scheduler.SchedulerMain");

    String[] commandArgs = schedulerCommandArgs(config, runtime, freePorts);
    commands.addAll(Arrays.asList(commandArgs));

    return commands.toArray(new String[0]);
  }

  /**
   * Util method to get the arguments to the heron scheduler.
   *
   * @param config The static Config
   * @param runtime The runtime Config
   * @param freePorts list of free ports
   * @return String[] representing the arguments to start heron-scheduler
   */
  public static String[] schedulerCommandArgs(
      Config config, Config runtime, List<Integer> freePorts) {
    // First let us have some safe checks
    if (freePorts.size() < PORTS_REQUIRED_FOR_SCHEDULER) {
      throw new RuntimeException("Failed to find enough ports for executor");
    }
    for (int port : freePorts) {
      if (port == -1) {
        throw new RuntimeException("Failed to find available ports for executor");
      }
    }

    int httpPort = freePorts.get(0);

    List<String> commands = new ArrayList<>();

    commands.add("--cluster");
    commands.add(Context.cluster(config));
    commands.add("--role");
    commands.add(Context.role(config));
    commands.add("--environment");
    commands.add(Context.environ(config));
    commands.add("--topology_name");
    commands.add(Context.topologyName(config));
    commands.add("--topology_bin");
    commands.add(Context.topologyBinaryFile(config));
    commands.add("--http_port");
    commands.add(Integer.toString(httpPort));

    return commands.toArray(new String[0]);
  }

  /**
   * Utils method to construct the command to start heron-executor
   *
   * @param config The static config
   * @param runtime The runtime config
   * @param shardId the executor/container index
   * @param ports a map of ports to use where the key indicate the port type and the
   * value is the port
   * @return String[] representing the command to start heron-executor
   */
  public static String[] getExecutorCommand(
      Config config,
      Config runtime,
      int shardId,
      Map<ExecutorPort, String> ports) {
    return getExecutorCommand(config, runtime, Integer.toString(shardId), ports);
  }

  /**
   * Utils method to construct the command to start heron-executor
   *
   * @param config The static config
   * @param runtime The runtime config
   * @param shardId the executor/container index
   * @param ports a map of ports to use where the key indicate the port type and the
   * value is the port
   * @return String[] representing the command to start heron-executor
   */
  public static String[] getExecutorCommand(
      Config config,
      Config runtime,
      String shardId,
      Map<ExecutorPort, String> ports) {
    List<String> commands = new ArrayList<>();
    commands.add(Context.executorBinary(config));
    commands.add(createCommandArg(ExecutorFlag.Shard, shardId));

    String[] commandArgs = executorCommandArgs(config, runtime, ports);
    commands.addAll(Arrays.asList(commandArgs));

    return commands.toArray(new String[0]);
  }

  /**
   * Util method to get the arguments to the heron executor. This method creates the arguments
   * without the container index, which is the first argument to the executor
   *
   * @param config The static Config
   * @param runtime The runtime Config
   * @param ports a map of ports to use where the key indicate the port type and the
   * value is the port
   * @return String[] representing the arguments to start heron-executor
   */
  public static String[] executorCommandArgs(
      Config config, Config runtime, Map<ExecutorPort, String> ports) {
    TopologyAPI.Topology topology = Runtime.topology(runtime);

    String masterPort = ExecutorPort.getPort(
        ExecutorPort.MASTER_PORT, ports);
    String tmasterControllerPort = ExecutorPort.getPort(
        ExecutorPort.TMASTER_CONTROLLER_PORT, ports);
    String tmasterStatsPort = ExecutorPort.getPort(
        ExecutorPort.TMASTER_STATS_PORT, ports);
    String shellPort = ExecutorPort.getPort(
        ExecutorPort.SHELL_PORT, ports);
    String metricsmgrPort = ExecutorPort.getPort(
        ExecutorPort.METRICS_MANAGER_PORT, ports);
    String schedulerPort = ExecutorPort.getPort(
        ExecutorPort.SCHEDULER_PORT, ports);
    String metricsCacheMasterPort =  ExecutorPort.getPort(
        ExecutorPort.METRICS_CACHE_MASTER_PORT, ports);
    String metricsCacheStatsPort = ExecutorPort.getPort(
        ExecutorPort.METRICS_CACHE_STATS_PORT, ports);
    String ckptmgrPort = ExecutorPort.getPort(
        ExecutorPort.CHECKPOINT_MANAGER_PORT, ports);
    String remoteDebuggerPorts = ExecutorPort.getPort(
        ExecutorPort.JVM_REMOTE_DEBUGGER_PORTS, ports
    );

    List<String> commands = new ArrayList<>();
    commands.add(createCommandArg(ExecutorFlag.TopologyName, topology.getName()));
    commands.add(createCommandArg(ExecutorFlag.TopologyId, topology.getId()));
    commands.add(createCommandArg(ExecutorFlag.TopologyDefinitionFile,
        FileUtils.getBaseName(Context.topologyDefinitionFile(config))));
    commands.add(createCommandArg(ExecutorFlag.StateManagerConnection,
        Context.stateManagerConnectionString(config)));
    commands.add(createCommandArg(ExecutorFlag.StateManagerRoot,
        Context.stateManagerRootPath(config)));
    commands.add(createCommandArg(ExecutorFlag.TMasterBinary, Context.tmasterBinary(config)));
    commands.add(createCommandArg(ExecutorFlag.StmgrBinary, Context.stmgrBinary(config)));
    commands.add(createCommandArg(ExecutorFlag.MetricsManagerClasspath,
        Context.metricsManagerClassPath(config)));
    commands.add(createCommandArg(ExecutorFlag.InstanceJvmOpts,
        SchedulerUtils.encodeJavaOpts(TopologyUtils.getInstanceJvmOptions(topology))));
    commands.add(createCommandArg(ExecutorFlag.Classpath,
        TopologyUtils.makeClassPath(topology, Context.topologyBinaryFile(config))));
    commands.add(createCommandArg(ExecutorFlag.MasterPort, masterPort));
    commands.add(createCommandArg(ExecutorFlag.TMasterControllerPort, tmasterControllerPort));
    commands.add(createCommandArg(ExecutorFlag.TMasterStatsPort, tmasterStatsPort));
    commands.add(createCommandArg(ExecutorFlag.HeronInternalsConfigFile,
        Context.systemConfigFile(config)));
    commands.add(createCommandArg(ExecutorFlag.OverrideConfigFile, Context.overrideFile(config)));
    commands.add(createCommandArg(ExecutorFlag.ComponentRamMap, Runtime.componentRamMap(runtime)));
    commands.add(createCommandArg(ExecutorFlag.ComponentJvmOpts,
        SchedulerUtils.encodeJavaOpts(TopologyUtils.getComponentJvmOptions(topology))));
    commands.add(createCommandArg(ExecutorFlag.PkgType,
        Context.topologyPackageType(config).name().toLowerCase()));
    commands.add(createCommandArg(ExecutorFlag.TopologyBinaryFile,
        Context.topologyBinaryFile(config)));
    commands.add(createCommandArg(ExecutorFlag.HeronJavaHome, Context.clusterJavaHome(config)));
    commands.add(createCommandArg(ExecutorFlag.ShellPort, shellPort));
    commands.add(createCommandArg(ExecutorFlag.HeronShellBinary, Context.shellBinary(config)));
    commands.add(createCommandArg(ExecutorFlag.MetricsManagerPort, metricsmgrPort));
    commands.add(createCommandArg(ExecutorFlag.Cluster, Context.cluster(config)));
    commands.add(createCommandArg(ExecutorFlag.Role, Context.role(config)));
    commands.add(createCommandArg(ExecutorFlag.Environment, Context.environ(config)));
    commands.add(createCommandArg(ExecutorFlag.InstanceClasspath,
        Context.instanceClassPath(config)));
    commands.add(createCommandArg(ExecutorFlag.MetricsSinksConfigFile,
        Context.metricsSinksFile(config)));

    String completeSchedulerProcessClassPath = String.format("%s:%s:%s",
        Context.schedulerClassPath(config),
        Context.packingClassPath(config),
        Context.stateManagerClassPath(config));

    commands.add(createCommandArg(ExecutorFlag.SchedulerClasspath,
        completeSchedulerProcessClassPath));
    commands.add(createCommandArg(ExecutorFlag.SchedulerPort, schedulerPort));
    commands.add(createCommandArg(ExecutorFlag.PythonInstanceBinary,
        Context.pythonInstanceBinary(config)));
    commands.add(createCommandArg(ExecutorFlag.CppInstanceBinary,
        Context.cppInstanceBinary(config)));

    commands.add(createCommandArg(ExecutorFlag.MetricsCacheManagerClasspath,
        Context.metricsCacheManagerClassPath(config)));
    commands.add(createCommandArg(ExecutorFlag.MetricsCacheManagerMasterPort,
        metricsCacheMasterPort));
    commands.add(createCommandArg(ExecutorFlag.MetricsCacheManagerStatsPort,
        metricsCacheStatsPort));

    Boolean ckptMgrEnabled = TopologyUtils.shouldStartCkptMgr(topology);
    commands.add(createCommandArg(ExecutorFlag.IsStateful, Boolean.toString(ckptMgrEnabled)));
    String completeCkptmgrProcessClassPath = String.format("%s:%s:%s",
        Context.ckptmgrClassPath(config),
        Context.statefulStoragesClassPath(config),
        Context.statefulStorageCustomClassPath(config));
    commands.add(createCommandArg(ExecutorFlag.CheckpointManagerClasspath,
        completeCkptmgrProcessClassPath));
    commands.add(createCommandArg(ExecutorFlag.CheckpointManagerPort, ckptmgrPort));
    commands.add(createCommandArg(ExecutorFlag.StatefulConfigFile,
        Context.statefulConfigFile(config)));

    String healthMgrMode =
        Context.healthMgrMode(config) == null ? "disabled" : Context.healthMgrMode(config);
    commands.add(createCommandArg(ExecutorFlag.HealthManagerMode, healthMgrMode));
    commands.add(createCommandArg(ExecutorFlag.HealthManagerClasspath,
        Context.healthMgrClassPath(config)));
    if (remoteDebuggerPorts != null) {
      commands.add(createCommandArg(ExecutorFlag.JvmRemoteDebuggerPorts, remoteDebuggerPorts));
    }

    return commands.toArray(new String[commands.size()]);
  }

  public static String createCommandArg(ExecutorFlag flag, String value) {
    return String.format("%s=%s", flag.getFlag(), value);
  }

  public static String[] splitCommandArg(String command) {
    return command.split("=");
  }

  /**
   * Set the location of scheduler for other processes to discover,
   * when invoke IScheduler as a library on client side
   *
   * @param runtime the runtime configuration
   * @param scheduler the IScheduler to provide more info
   * @param isService true if the scheduler is a service; false otherwise
   */
  public static boolean setLibSchedulerLocation(
      Config runtime,
      IScheduler scheduler,
      boolean isService) {
    // Dummy value since there is no scheduler running as service
    final String endpoint = "scheduler_as_lib_no_endpoint";
    return setSchedulerLocation(runtime, endpoint, scheduler);
  }

  /**
   * Set the location of scheduler for other processes to discover
   *
   * @param runtime the runtime configuration
   * @param schedulerEndpoint the endpoint that scheduler listens for receives requests
   * @param scheduler the IScheduler to provide more info
   */
  public static boolean setSchedulerLocation(
      Config runtime,
      String schedulerEndpoint,
      IScheduler scheduler) {

    // Set scheduler location to host:port by default. Overwrite scheduler location if behind DNS.
    Scheduler.SchedulerLocation.Builder builder = Scheduler.SchedulerLocation.newBuilder()
        .setTopologyName(Runtime.topologyName(runtime))
        .setHttpEndpoint(schedulerEndpoint);

    // Set the job link in SchedulerLocation if any
    List<String> jobLinks = scheduler.getJobLinks();
    // Check whether IScheduler provides valid job link
    if (jobLinks != null) {
      builder.addAllJobPageLink(jobLinks);
    }

    Scheduler.SchedulerLocation location = builder.build();

    LOG.log(Level.INFO, "Setting Scheduler locations: {0}", location);
    SchedulerStateManagerAdaptor statemgr = Runtime.schedulerStateManagerAdaptor(runtime);
    Boolean result =
        statemgr.setSchedulerLocation(location, Runtime.topologyName(runtime));

    if (result == null || !result) {
      LOG.severe("Failed to set Scheduler location");
      return false;
    }

    return true;
  }

  /**
   * construct heron scheduler response basing on the given result
   *
   * @param isOK whether the request successful
   */
  public static Scheduler.SchedulerResponse constructSchedulerResponse(boolean isOK) {
    Common.Status.Builder status = Common.Status.newBuilder();
    if (isOK) {
      status.setStatus(Common.StatusCode.OK);
    } else {
      status.setStatus(Common.StatusCode.NOTOK);
    }

    return Scheduler.SchedulerResponse.newBuilder().
        setStatus(status).
        build();
  }

  /**
   * Encode the JVM options
   * <br> 1. Convert it into Base64 format
   * <br> 2. Add \" at the start and at the end
   * <br> 3. replace "=" with "&amp;equals;"
   *
   * @return encoded string
   */
  public static String encodeJavaOpts(String javaOpts) {
    String javaOptsBase64 = DatatypeConverter.printBase64Binary(
        javaOpts.getBytes(StandardCharsets.UTF_8));

    return String.format("\"%s\"", javaOptsBase64.replace("=", "&equals;"));
  }

  /**
   * Decode the JVM options
   * <br> 1. strip \" at the start and at the end
   * <br> 2. replace "&amp;equals;" with "="
   * <br> 3. Revert from Base64 format
   *
   * @return decoded string
   */
  public static String decodeJavaOpts(String encodedJavaOpts) {
    String javaOptsBase64 =
        encodedJavaOpts.
            replaceAll("^\"+", "").
            replaceAll("\\s+$", "").
            replace("&equals;", "=");

    return new String(
        DatatypeConverter.parseBase64Binary(javaOptsBase64), StandardCharsets.UTF_8);
  }

  /**
   * Create the directory if it does not exist otherwise clean the directory.
   *
   * @param directory the working directory to setup
   * @return true if successful
   */
  public static boolean createOrCleanDirectory(String directory) {
    // if the directory does not exist, create it.
    if (!FileUtils.isDirectoryExists(directory)) {
      LOG.fine("The directory does not exist; creating it.");
      if (!FileUtils.createDirectory(directory)) {
        LOG.severe("Failed to create directory: " + directory);
        return false;
      }
    }

    // Cleanup the directory
    if (!FileUtils.cleanDir(directory)) {
      LOG.severe("Failed to clean directory: " + directory);
      return false;
    }

    return true;
  }

  public static boolean extractPackage(String workingDirectory, String packageURI,
      String packageDestination, boolean deletePackage, boolean verbose) {
    return curlAndExtractPackage(workingDirectory, packageURI, packageDestination,
        deletePackage, verbose);
  }

  /**
   * Curl a package, extract it to working directory
   *
   * @param workingDirectory the working directory to setup
   * @param packageURI the URL of core release package
   * @param packageDestination the destination of the core release package fetched
   * @param isDeletePackage delete the package curled or not
   * @param isVerbose display verbose output or not
   * @return true if successful
   */
  public static boolean curlAndExtractPackage(
      String workingDirectory,
      String packageURI,
      String packageDestination,
      boolean isDeletePackage,
      boolean isVerbose) {
    // curl the package to the working directory and extract it
    LOG.log(Level.FINE, "Fetching package {0}", packageURI);
    LOG.fine("Fetched package can overwrite old one.");
    if (!ShellUtils.curlPackage(
        packageURI, packageDestination, isVerbose, false)) {
      LOG.severe("Failed to fetch package.");
      return false;
    }

    // untar the heron core release package in the working directory
    LOG.log(Level.FINE, "Extracting the package {0}", packageURI);
    if (!ShellUtils.extractPackage(
        packageDestination, workingDirectory, isVerbose, false)) {
      LOG.severe("Failed to extract package.");
      return false;
    }

    // remove the core release package
    if (isDeletePackage && !FileUtils.deleteFile(packageDestination)) {
      LOG.warning("Failed to delete the package: " + packageDestination);
    }

    return true;
  }

  /**
   * Replaces persisted packing plan in state manager.
   */
  public static void persistUpdatedPackingPlan(String topologyName,
                                               PackingPlan updatedPackingPlan,
                                               SchedulerStateManagerAdaptor stateManager) {
    LOG.log(Level.INFO, "Updating scheduled-resource in packing plan: {0}", topologyName);
    PackingPlanProtoSerializer serializer = new PackingPlanProtoSerializer();

    if (!stateManager.updatePackingPlan(serializer.toProto(updatedPackingPlan), topologyName)) {
      throw new RuntimeException(String.format(
          "Failed to update packing plan for topology %s", topologyName));
    }
  }
}
