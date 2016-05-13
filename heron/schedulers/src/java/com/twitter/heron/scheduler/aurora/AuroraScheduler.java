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

import java.io.File;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import javax.xml.bind.DatatypeConverter;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.common.basics.FileUtils;
import com.twitter.heron.proto.scheduler.Scheduler;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Context;
import com.twitter.heron.spi.common.PackingPlan;
import com.twitter.heron.spi.scheduler.IScheduler;
import com.twitter.heron.spi.utils.Runtime;
import com.twitter.heron.spi.utils.TopologyUtils;

public class AuroraScheduler implements IScheduler {
  private static final Logger LOG = Logger.getLogger(AuroraLauncher.class.getName());

  private Config config;
  private Config runtime;
  private AuroraController controller;

  @Override
  public void initialize(Config mConfig, Config mRuntime) {
    this.config = mConfig;
    this.runtime = mRuntime;
    this.controller = getController();
  }

  /**
   * Get an AuroraControl basing on the config and runtime
   *
   * @return AuroraControl
   */
  protected AuroraController getController() {
    return new AuroraController(
        Runtime.topologyName(runtime),
        Context.cluster(config),
        Context.role(config),
        Context.environ(config),
        Context.verbose(config));
  }

  @Override
  public void close() {
    // Nothing to do here
  }

  @Override
  public boolean onSchedule(PackingPlan packing) {
    if (packing == null || packing.containers.isEmpty()) {
      LOG.severe("No container requested. Can't schedule");
      return false;
    }

    LOG.info("Launching topology in aurora");

    Map<String, String> auroraProperties = createAuroraProperties(packing);

    return controller.createJob(getHeronAuroraPath(), auroraProperties);
  }

  @Override
  public List<String> getJobLinks() {
    return new ArrayList<>();
  }

  @Override
  public boolean onKill(Scheduler.KillTopologyRequest request) {
    return controller.killJob();
  }

  @Override
  public boolean onRestart(Scheduler.RestartTopologyRequest request) {
    int containerId = request.getContainerIndex();
    return controller.restartJob(containerId);
  }

  /**
   * Encode the JVM options
   *
   * @return encoded string
   */
  protected String formatJavaOpts(String javaOpts) {
    String javaOptsBase64 = DatatypeConverter.printBase64Binary(
        javaOpts.getBytes(Charset.forName("UTF-8")));

    return String.format("\"%s\"", javaOptsBase64.replace("=", "&equals;"));
  }

  protected String getHeronAuroraPath() {
    return new File(Context.heronConf(config), "heron.aurora").getPath();
  }

  protected Map<String, String> createAuroraProperties(PackingPlan packing) {
    Map<String, String> auroraProperties = new HashMap<>();

    TopologyAPI.Topology topology = Runtime.topology(runtime);

    PackingPlan.Resource containerResource =
        packing.containers.values().iterator().next().resource;

    auroraProperties.put("SANDBOX_EXECUTOR_BINARY", Context.executorSandboxBinary(config));
    auroraProperties.put("TOPOLOGY_NAME", topology.getName());
    auroraProperties.put("TOPOLOGY_ID", topology.getId());
    auroraProperties.put("TOPOLOGY_DEFINITION_FILE",
        FileUtils.getBaseName(Context.topologyDefinitionFile(config)));
    auroraProperties.put("INSTANCE_DISTRIBUTION", TopologyUtils.packingToString(packing));
    auroraProperties.put("STATEMGR_CONNECTION_STRING",
        Context.stateManagerConnectionString(config));
    auroraProperties.put("STATEMGR_ROOT_PATH", Context.stateManagerRootPath(config));
    auroraProperties.put("SANDBOX_TMASTER_BINARY", Context.tmasterSandboxBinary(config));
    auroraProperties.put("SANDBOX_STMGR_BINARY", Context.stmgrSandboxBinary(config));
    auroraProperties.put("SANDBOX_METRICSMGR_CLASSPATH",
        Context.metricsManagerSandboxClassPath(config));
    auroraProperties.put("INSTANCE_JVM_OPTS_IN_BASE64",
        formatJavaOpts(TopologyUtils.getInstanceJvmOptions(topology)));
    auroraProperties.put("TOPOLOGY_CLASSPATH",
        TopologyUtils.makeClassPath(topology, Context.topologyJarFile(config)));

    auroraProperties.put("SANDBOX_SYSTEM_YAML", Context.systemConfigSandboxFile(config));
    auroraProperties.put("COMPONENT_RAMMAP",
        TopologyUtils.formatRamMap(
            TopologyUtils.getComponentRamMap(topology, Context.instanceRam(config))));
    auroraProperties.put("COMPONENT_JVM_OPTS_IN_BASE64",
        formatJavaOpts(TopologyUtils.getComponentJvmOptions(topology)));
    auroraProperties.put("TOPOLOGY_PACKAGE_TYPE", Context.topologyPackageType(config));
    auroraProperties.put("TOPOLOGY_JAR_FILE",
        FileUtils.getBaseName(Context.topologyJarFile(config)));
    auroraProperties.put("HERON_SANDBOX_JAVA_HOME", Context.javaSandboxHome(config));

    auroraProperties.put("SANDBOX_SHELL_BINARY", Context.shellSandboxBinary(config));

    auroraProperties.put("CPUS_PER_CONTAINER", containerResource.cpu + "");
    auroraProperties.put("DISK_PER_CONTAINER", containerResource.disk + "");
    auroraProperties.put("RAM_PER_CONTAINER", containerResource.ram + "");

    auroraProperties.put("NUM_CONTAINERS", (1 + TopologyUtils.getNumContainers(topology)) + "");

    auroraProperties.put("CLUSTER", Context.cluster(config));
    auroraProperties.put("ENVIRON", Context.environ(config));
    auroraProperties.put("ROLE", Context.role(config));
    auroraProperties.put("ISPRODUCTION", isProduction() + "");

    auroraProperties.put("SANDBOX_INSTANCE_CLASSPATH", Context.instanceSandboxClassPath(config));
    auroraProperties.put("SANDBOX_METRICS_YAML", Context.metricsSinksSandboxFile(config));

    String completeSchedulerClassPath = new StringBuilder()
        .append(Context.schedulerSandboxClassPath(config)).append(":")
        .append(Context.packingSandboxClassPath(config)).append(":")
        .append(Context.stateManagerSandboxClassPath(config))
        .toString();
    auroraProperties.put("SANDBOX_SCHEDULER_CLASSPATH", completeSchedulerClassPath);

    String heronCoreReleasePkgURI = Context.corePackageUri(config);
    String topologyPkgURI = Runtime.topologyPackageUri(runtime).toString();

    auroraProperties.put("CORE_PACKAGE_URI", heronCoreReleasePkgURI);
    auroraProperties.put("TOPOLOGY_PACKAGE_URI", topologyPkgURI);

    return auroraProperties;
  }

  protected boolean isProduction() {
    // TODO (nlu): currently enforce environment to be "prod" for a Production job
    return "prod".equals(Context.environ(config));
  }
}
