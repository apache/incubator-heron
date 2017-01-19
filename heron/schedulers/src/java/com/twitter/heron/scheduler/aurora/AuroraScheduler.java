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

package com.twitter.heron.scheduler.aurora;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.xml.bind.DatatypeConverter;

import com.google.common.base.Optional;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.common.basics.FileUtils;
import com.twitter.heron.proto.scheduler.Scheduler;
import com.twitter.heron.scheduler.UpdateTopologyManager;
import com.twitter.heron.scheduler.utils.Runtime;
import com.twitter.heron.scheduler.utils.SchedulerUtils;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Context;
import com.twitter.heron.spi.common.Misc;
import com.twitter.heron.spi.packing.PackingPlan;
import com.twitter.heron.spi.packing.Resource;
import com.twitter.heron.spi.scheduler.IScalable;
import com.twitter.heron.spi.scheduler.IScheduler;
import com.twitter.heron.spi.utils.TopologyUtils;

public class AuroraScheduler implements IScheduler, IScalable {
  private static final Logger LOG = Logger.getLogger(AuroraLauncher.class.getName());

  private Config config;
  private Config runtime;
  private AuroraController controller;
  private UpdateTopologyManager updateTopologyManager;

  @Override
  public void initialize(Config mConfig, Config mRuntime) {
    this.config = mConfig;
    this.runtime = mRuntime;
    this.controller = getController();
    this.updateTopologyManager =
        new UpdateTopologyManager(config, runtime, Optional.<IScalable>of(this));
  }

  /**
   * Get an AuroraController based on the config and runtime
   *
   * @return AuroraController
   */
  protected AuroraController getController() {
    return new AuroraCLIController(
        Runtime.topologyName(runtime),
        Context.cluster(config),
        Context.role(config),
        Context.environ(config),
        AuroraContext.getHeronAuroraPath(config),
        Context.verbose(config));
  }

  @Override
  public void close() {
    if (updateTopologyManager != null) {
      updateTopologyManager.close();
    }
  }

  @Override
  public boolean onSchedule(PackingPlan packing) {
    if (packing == null || packing.getContainers().isEmpty()) {
      LOG.severe("No container requested. Can't schedule");
      return false;
    }

    LOG.info("Launching topology in aurora");

    // Align the cpu, ram, disk to the maximal one
    PackingPlan updatedPackingPlan = packing.cloneWithHomogeneousScheduledResource();
    SchedulerUtils.persistUpdatedPackingPlan(Runtime.topologyName(runtime), updatedPackingPlan,
        Runtime.schedulerStateManagerAdaptor(runtime));

    Map<AuroraField, String> auroraProperties = createAuroraProperties(updatedPackingPlan);

    return controller.createJob(auroraProperties);
  }

  @Override
  public List<String> getJobLinks() {
    List<String> jobLinks = new ArrayList<>();

    //Only the aurora job page is returned
    String jobLinkFormat = AuroraContext.getJobLinkTemplate(config);
    if (jobLinkFormat != null && !jobLinkFormat.isEmpty()) {
      String jobLink = Misc.substitute(config, jobLinkFormat);
      jobLinks.add(jobLink);
    }

    return jobLinks;
  }

  @Override
  public boolean onKill(Scheduler.KillTopologyRequest request) {
    return controller.killJob();
  }

  @Override
  public boolean onRestart(Scheduler.RestartTopologyRequest request) {
    Integer containerId = null;
    if (request.getContainerIndex() != -1) {
      containerId = request.getContainerIndex();
    }
    return controller.restart(containerId);
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
  public void addContainers(Set<PackingPlan.ContainerPlan> containersToAdd) {
    controller.addContainers(containersToAdd.size());
  }

  @Override
  public void removeContainers(Set<PackingPlan.ContainerPlan> containersToRemove) {
    controller.removeContainers(containersToRemove);
  }

  /**
   * Encode the JVM options
   *
   * @return encoded string
   */
  protected String formatJavaOpts(String javaOpts) {
    String javaOptsBase64 = DatatypeConverter.printBase64Binary(
        javaOpts.getBytes(StandardCharsets.UTF_8));

    return String.format("\"%s\"", javaOptsBase64.replace("=", "&equals;"));
  }

  @SuppressWarnings("deprecation") // remove once we remove ISPRODUCTION usage below
  protected Map<AuroraField, String> createAuroraProperties(PackingPlan packing) {
    Map<AuroraField, String> auroraProperties = new HashMap<>();

    TopologyAPI.Topology topology = Runtime.topology(runtime);
    Resource containerResource = packing.getContainers().iterator().next().getRequiredResource();

    auroraProperties.put(AuroraField.SANDBOX_EXECUTOR_BINARY,
        Context.executorSandboxBinary(config));
    auroraProperties.put(AuroraField.TOPOLOGY_NAME, topology.getName());
    auroraProperties.put(AuroraField.TOPOLOGY_ID, topology.getId());
    auroraProperties.put(AuroraField.TOPOLOGY_DEFINITION_FILE,
        FileUtils.getBaseName(Context.topologyDefinitionFile(config)));
    auroraProperties.put(AuroraField.STATEMGR_CONNECTION_STRING,
        Context.stateManagerConnectionString(config));
    auroraProperties.put(AuroraField.STATEMGR_ROOT_PATH, Context.stateManagerRootPath(config));
    auroraProperties.put(AuroraField.SANDBOX_TMASTER_BINARY, Context.tmasterSandboxBinary(config));
    auroraProperties.put(AuroraField.SANDBOX_STMGR_BINARY, Context.stmgrSandboxBinary(config));
    auroraProperties.put(AuroraField.SANDBOX_METRICSMGR_CLASSPATH,
        Context.metricsManagerSandboxClassPath(config));
    auroraProperties.put(AuroraField.INSTANCE_JVM_OPTS_IN_BASE64,
        formatJavaOpts(TopologyUtils.getInstanceJvmOptions(topology)));
    auroraProperties.put(AuroraField.TOPOLOGY_CLASSPATH,
        TopologyUtils.makeClassPath(topology, Context.topologyBinaryFile(config)));

    auroraProperties.put(AuroraField.SANDBOX_SYSTEM_YAML, Context.systemConfigSandboxFile(config));
    auroraProperties.put(AuroraField.COMPONENT_RAMMAP, Runtime.componentRamMap(runtime));
    auroraProperties.put(AuroraField.COMPONENT_JVM_OPTS_IN_BASE64,
        formatJavaOpts(TopologyUtils.getComponentJvmOptions(topology)));
    auroraProperties.put(AuroraField.TOPOLOGY_PACKAGE_TYPE,
        Context.topologyPackageType(config).name().toLowerCase());
    auroraProperties.put(AuroraField.TOPOLOGY_BINARY_FILE,
        FileUtils.getBaseName(Context.topologyBinaryFile(config)));
    auroraProperties.put(AuroraField.HERON_SANDBOX_JAVA_HOME, Context.javaSandboxHome(config));

    auroraProperties.put(AuroraField.SANDBOX_SHELL_BINARY, Context.shellSandboxBinary(config));
    auroraProperties.put(AuroraField.SANDBOX_PYTHON_INSTANCE_BINARY,
        Context.pythonInstanceSandboxBinary(config));

    auroraProperties.put(AuroraField.CPUS_PER_CONTAINER,
        Double.toString(containerResource.getCpu()));
    auroraProperties.put(AuroraField.DISK_PER_CONTAINER,
        Long.toString(containerResource.getDisk().asBytes()));
    auroraProperties.put(AuroraField.RAM_PER_CONTAINER,
        Long.toString(containerResource.getRam().asBytes()));

    auroraProperties.put(AuroraField.NUM_CONTAINERS,
        Integer.toString(1 + TopologyUtils.getNumContainers(topology)));

    auroraProperties.put(AuroraField.CLUSTER, Context.cluster(config));
    auroraProperties.put(AuroraField.ENVIRON, Context.environ(config));
    auroraProperties.put(AuroraField.ROLE, Context.role(config));

    // TODO (nlu): currently enforce environment to be "prod" for a Production job
    String isProduction = Boolean.toString("prod".equals(Context.environ(config)));
    // TODO: remove this and suppress above once we cut a release and update the aurora config file
    auroraProperties.put(AuroraField.ISPRODUCTION, isProduction);
    auroraProperties.put(AuroraField.IS_PRODUCTION, isProduction);

    auroraProperties.put(AuroraField.SANDBOX_INSTANCE_CLASSPATH,
        Context.instanceSandboxClassPath(config));
    auroraProperties.put(AuroraField.SANDBOX_METRICS_YAML, Context.metricsSinksSandboxFile(config));

    String completeSchedulerClassPath = new StringBuilder()
        .append(Context.schedulerSandboxClassPath(config)).append(":")
        .append(Context.packingSandboxClassPath(config)).append(":")
        .append(Context.stateManagerSandboxClassPath(config))
        .toString();
    auroraProperties.put(AuroraField.SANDBOX_SCHEDULER_CLASSPATH, completeSchedulerClassPath);

    String heronCoreReleasePkgURI = Context.corePackageUri(config);
    String topologyPkgURI = Runtime.topologyPackageUri(runtime).toString();

    auroraProperties.put(AuroraField.CORE_PACKAGE_URI, heronCoreReleasePkgURI);
    auroraProperties.put(AuroraField.TOPOLOGY_PACKAGE_URI, topologyPkgURI);

    return auroraProperties;
  }
}
