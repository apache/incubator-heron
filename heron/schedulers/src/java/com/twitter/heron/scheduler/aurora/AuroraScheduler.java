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
import com.twitter.heron.spi.common.TokenSub;
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
    this.config = Config.toClusterMode(mConfig);
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
    Config localConfig = Config.toLocalMode(this.config);
    return new AuroraCLIController(
        Runtime.topologyName(runtime),
        Context.cluster(localConfig),
        Context.role(localConfig),
        Context.environ(localConfig),
        AuroraContext.getHeronAuroraPath(localConfig),
        Context.verbose(localConfig));
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

    // Align the cpu, ram, disk to the maximal one, and set them to ScheduledResource
    PackingPlan updatedPackingPlan = packing.cloneWithHomogeneousScheduledResource();
    SchedulerUtils.persistUpdatedPackingPlan(Runtime.topologyName(runtime), updatedPackingPlan,
        Runtime.schedulerStateManagerAdaptor(runtime));

    // Use the ScheduledResource to create aurora properties
    // the ScheduledResource is guaranteed to be set after calling
    // cloneWithHomogeneousScheduledResource in the above code
    Resource containerResource =
        updatedPackingPlan.getContainers().iterator().next().getScheduledResource().get();
    Map<AuroraField, String> auroraProperties = createAuroraProperties(containerResource);

    return controller.createJob(auroraProperties);
  }

  @Override
  public List<String> getJobLinks() {
    List<String> jobLinks = new ArrayList<>();

    //Only the aurora job page is returned
    String jobLinkFormat = AuroraContext.getJobLinkTemplate(config);
    if (jobLinkFormat != null && !jobLinkFormat.isEmpty()) {
      String jobLink = TokenSub.substitute(config, jobLinkFormat);
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

  protected Map<AuroraField, String> createAuroraProperties(Resource containerResource) {
    Map<AuroraField, String> auroraProperties = new HashMap<>();

    TopologyAPI.Topology topology = Runtime.topology(runtime);

    auroraProperties.put(AuroraField.EXECUTOR_BINARY,
        Context.executorBinary(config));
    auroraProperties.put(AuroraField.TOPOLOGY_NAME, topology.getName());
    auroraProperties.put(AuroraField.TOPOLOGY_ID, topology.getId());
    auroraProperties.put(AuroraField.TOPOLOGY_DEFINITION_FILE,
        FileUtils.getBaseName(Context.topologyDefinitionFile(config)));
    auroraProperties.put(AuroraField.STATEMGR_CONNECTION_STRING,
        Context.stateManagerConnectionString(config));
    auroraProperties.put(AuroraField.STATEMGR_ROOT_PATH, Context.stateManagerRootPath(config));
    auroraProperties.put(AuroraField.TMASTER_BINARY, Context.tmasterBinary(config));
    auroraProperties.put(AuroraField.STMGR_BINARY, Context.stmgrBinary(config));
    auroraProperties.put(AuroraField.METRICSMGR_CLASSPATH,
        Context.metricsManagerClassPath(config));
    auroraProperties.put(AuroraField.INSTANCE_JVM_OPTS_IN_BASE64,
        formatJavaOpts(TopologyUtils.getInstanceJvmOptions(topology)));
    auroraProperties.put(AuroraField.TOPOLOGY_CLASSPATH,
        TopologyUtils.makeClassPath(topology, Context.topologyBinaryFile(config)));

    auroraProperties.put(AuroraField.SYSTEM_YAML, Context.systemConfigFile(config));
    auroraProperties.put(AuroraField.COMPONENT_RAMMAP, Runtime.componentRamMap(runtime));
    auroraProperties.put(AuroraField.COMPONENT_JVM_OPTS_IN_BASE64,
        formatJavaOpts(TopologyUtils.getComponentJvmOptions(topology)));
    auroraProperties.put(AuroraField.TOPOLOGY_PACKAGE_TYPE,
        Context.topologyPackageType(config).name().toLowerCase());
    auroraProperties.put(AuroraField.TOPOLOGY_BINARY_FILE,
        Context.topologyBinaryFile(config));
    auroraProperties.put(AuroraField.JAVA_HOME, Context.clusterJavaHome(config));

    auroraProperties.put(AuroraField.SHELL_BINARY, Context.shellBinary(config));
    auroraProperties.put(AuroraField.PYTHON_INSTANCE_BINARY,
        Context.pythonInstanceBinary(config));

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

    // Job configuration attribute 'production' is deprecated.
    // Use 'tier' attribute instead
    // See: http://aurora.apache.org/documentation/latest/reference/configuration/#job-objects
    if ("prod".equals(Context.environ(config))) {
      auroraProperties.put(AuroraField.TIER, "preferred");
    } else {
      auroraProperties.put(AuroraField.TIER, "preemptible");
    }

    auroraProperties.put(AuroraField.INSTANCE_CLASSPATH, Context.instanceClassPath(config));
    auroraProperties.put(AuroraField.METRICS_YAML, Context.metricsSinksFile(config));

    String completeSchedulerClassPath = String.format("%s:%s:%s",
        Context.schedulerClassPath(config),
        Context.packingClassPath(config),
        Context.stateManagerClassPath(config));

    auroraProperties.put(AuroraField.SCHEDULER_CLASSPATH, completeSchedulerClassPath);

    String heronCoreReleasePkgURI = Context.corePackageUri(config);
    String topologyPkgURI = Runtime.topologyPackageUri(runtime).toString();

    auroraProperties.put(AuroraField.CORE_PACKAGE_URI, heronCoreReleasePkgURI);
    auroraProperties.put(AuroraField.TOPOLOGY_PACKAGE_URI, topologyPkgURI);

    auroraProperties.put(AuroraField.METRICSCACHEMGR_CLASSPATH,
        Context.metricsCacheManagerClassPath(config));

    boolean isStatefulEnabled = TopologyUtils.shouldStartCkptMgr(topology);
    auroraProperties.put(AuroraField.IS_STATEFUL_ENABLED, Boolean.toString(isStatefulEnabled));

    String completeCkptmgrProcessClassPath = String.format("%s:%s:%s",
        Context.ckptmgrClassPath(config),
        Context.statefulStoragesClassPath(config),
        Context.statefulStorageCustomClassPath(config));
    auroraProperties.put(AuroraField.CKPTMGR_CLASSPATH, completeCkptmgrProcessClassPath);
    auroraProperties.put(AuroraField.STATEFUL_CONFIG_YAML, Context.statefulConfigFile(config));

    return auroraProperties;
  }
}
