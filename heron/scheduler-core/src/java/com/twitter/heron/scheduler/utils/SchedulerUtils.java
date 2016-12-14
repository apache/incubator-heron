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
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.xml.bind.DatatypeConverter;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.common.basics.FileUtils;
import com.twitter.heron.proto.scheduler.Scheduler;
import com.twitter.heron.proto.system.Common;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Context;
import com.twitter.heron.spi.packing.PackingPlan;
import com.twitter.heron.spi.packing.PackingPlanProtoSerializer;
import com.twitter.heron.spi.scheduler.IScheduler;
import com.twitter.heron.spi.statemgr.SchedulerStateManagerAdaptor;
import com.twitter.heron.spi.utils.ShellUtils;
import com.twitter.heron.spi.utils.TopologyUtils;

public final class SchedulerUtils {
  public static final int PORTS_REQUIRED_FOR_EXECUTOR = 6;
  public static final int PORTS_REQUIRED_FOR_SCHEDULER = 1;

  private static final Logger LOG = Logger.getLogger(SchedulerUtils.class.getName());

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
    String javaExecutable = String.format("%s/%s", Context.javaSandboxHome(config), "bin/java");
    commands.add(javaExecutable);
    commands.add("-cp");

    // Construct the complete classpath to start scheduler
    String completeSchedulerProcessClassPath = new StringBuilder()
        .append(Context.schedulerSandboxClassPath(config)).append(":")
        .append(Context.packingSandboxClassPath(config)).append(":")
        .append(Context.stateManagerSandboxClassPath(config))
        .toString();
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
   * @param config The static Config
   * @param runtime The runtime Config
   * @param containerIndex the executor/container index
   * @param freePorts list of free ports
   * @return String[] representing the command to start heron-executor
   */
  public static String[] executorCommand(
      Config config,
      Config runtime,
      int containerIndex,
      List<Integer> freePorts) {
    // First let us have some safe checks
    if (freePorts.size() < PORTS_REQUIRED_FOR_EXECUTOR) {
      throw new RuntimeException("Failed to find enough ports for executor");
    }
    for (int port : freePorts) {
      if (port == -1) {
        throw new RuntimeException("Failed to find available ports for executor");
      }
    }

    // Convert port to string
    List<String> ports = new LinkedList<>();
    for (int port : freePorts) {
      ports.add(Integer.toString(port));
    }

    return getExecutorCommand(config, runtime, containerIndex, ports);
  }

  /**
   * Utils method to construct the command to start heron-executor
   *
   * @param config The static config
   * @param runtime The runtime config
   * @param containerIndex the executor/container index
   * @param ports list of free ports in String
   * @return String[] representing the command to start heron-executor
   */
  public static String[] getExecutorCommand(
      Config config,
      Config runtime,
      int containerIndex,
      List<String> ports) {
    List<String> commands = new ArrayList<>();
    commands.add(Context.executorSandboxBinary(config));
    commands.add(Integer.toString(containerIndex));

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
   * @param freePorts list of free ports
   * @return String[] representing the arguments to start heron-executor
   */
  public static String[] executorCommandArgs(
      Config config, Config runtime, List<String> freePorts) {
    TopologyAPI.Topology topology = Runtime.topology(runtime);

    String masterPort = freePorts.get(0);
    String tmasterControllerPort = freePorts.get(1);
    String tmasterStatsPort = freePorts.get(2);
    String shellPort = freePorts.get(3);
    String metricsmgrPort = freePorts.get(4);
    String schedulerPort = freePorts.get(5);

    List<String> commands = new ArrayList<>();
    commands.add(topology.getName());
    commands.add(topology.getId());
    commands.add(FileUtils.getBaseName(Context.topologyDefinitionFile(config)));
    commands.add(Context.stateManagerConnectionString(config));
    commands.add(Context.stateManagerRootPath(config));
    commands.add(Context.tmasterSandboxBinary(config));
    commands.add(Context.stmgrSandboxBinary(config));
    commands.add(Context.metricsManagerSandboxClassPath(config));
    commands.add(SchedulerUtils.encodeJavaOpts(TopologyUtils.getInstanceJvmOptions(topology)));
    commands.add(TopologyUtils.makeClassPath(topology, Context.topologyBinaryFile(config)));
    commands.add(masterPort);
    commands.add(tmasterControllerPort);
    commands.add(tmasterStatsPort);
    commands.add(Context.systemConfigSandboxFile(config));
    commands.add(Runtime.componentRamMap(runtime));
    commands.add(SchedulerUtils.encodeJavaOpts(TopologyUtils.getComponentJvmOptions(topology)));
    commands.add(Context.topologyPackageType(config).name().toLowerCase());
    commands.add(Context.topologyBinaryFile(config));
    commands.add(Context.javaSandboxHome(config));
    commands.add(shellPort);
    commands.add(Context.shellSandboxBinary(config));
    commands.add(metricsmgrPort);
    commands.add(Context.cluster(config));
    commands.add(Context.role(config));
    commands.add(Context.environ(config));
    commands.add(Context.instanceSandboxClassPath(config));
    commands.add(Context.metricsSinksSandboxFile(config));

    String completeSchedulerProcessClassPath = new StringBuilder()
        .append(Context.schedulerSandboxClassPath(config)).append(":")
        .append(Context.packingSandboxClassPath(config)).append(":")
        .append(Context.stateManagerSandboxClassPath(config))
        .toString();

    commands.add(completeSchedulerProcessClassPath);
    commands.add(schedulerPort);
    commands.add(Context.pythonInstanceSandboxBinary(config));

    return commands.toArray(new String[commands.size()]);
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

    LOG.log(Level.INFO, "Setting SchedulerLocation: {0}", location);
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
   * Setup the working directory:
   * <br> 1. Download heron core and the topology packages into topology working directory,
   * <br> 2. Extract heron core and the topology packages
   * <br> 3. Remove the downloaded heron core and the topology packages
   *
   * @param workingDirectory the working directory to setup
   * @param coreReleasePackageURL the URL of core release package
   * @param coreReleaseDestination the destination of the core release package fetched
   * @param topologyPackageURL the URL of heron topology release package
   * @param topologyPackageDestination the destination of heron topology release package fetched
   * @param isVerbose display verbose output or not
   * @return true if successful
   */
  public static boolean setupWorkingDirectory(
      String workingDirectory,
      String coreReleasePackageURL,
      String coreReleaseDestination,
      String topologyPackageURL,
      String topologyPackageDestination,
      boolean isVerbose) {
    // if the working directory does not exist, create it.
    if (!FileUtils.isDirectoryExists(workingDirectory)) {
      LOG.fine("The working directory does not exist; creating it.");
      if (!FileUtils.createDirectory(workingDirectory)) {
        LOG.severe("Failed to create directory: " + workingDirectory);
        return false;
      }
    }

    // Curl and extract heron core release package and topology package
    // And then delete the downloaded release package
    boolean ret =
        curlAndExtractPackage(
            workingDirectory, coreReleasePackageURL, coreReleaseDestination, true, isVerbose)
            &&
            curlAndExtractPackage(
                workingDirectory, topologyPackageURL, topologyPackageDestination, true, isVerbose);

    return ret;
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
