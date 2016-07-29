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

package com.twitter.heron.scheduler.marathon;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Logger;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Joiner;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.common.basics.FileUtils;
import com.twitter.heron.proto.scheduler.Scheduler;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Constants;
import com.twitter.heron.spi.common.Context;
import com.twitter.heron.spi.packing.PackingPlan;
import com.twitter.heron.spi.scheduler.IScheduler;
import com.twitter.heron.spi.utils.Runtime;
import com.twitter.heron.spi.utils.SchedulerUtils;
import com.twitter.heron.spi.utils.TopologyUtils;

public class MarathonScheduler implements IScheduler {
  private static final Logger LOG = Logger.getLogger(MarathonScheduler.class.getName());

  private Config config;
  private Config runtime;
  private MarathonController controller;

  @Override
  public void initialize(Config aConfig, Config aRuntime) {
    this.config = aConfig;
    this.runtime = aRuntime;
    this.controller = getController();
  }

  protected MarathonController getController() {
    return new MarathonController(
        MarathonContext.getSchedulerURI(config),
        Runtime.topologyName(runtime),
        Context.verbose(config));
  }

  @Override
  public void close() {
    // Do nothing
  }

  @Override
  public boolean onSchedule(PackingPlan packing) {
    if (packing == null || packing.containers.isEmpty()) {
      LOG.severe("No container requested. Can't schedule");
      return false;
    }

    LOG.info("Submitting topology to Marathon Scheduler");

    String topologyConf = getTopologyConf(packing);

    return controller.submitTopology(topologyConf);
  }

  @Override
  public List<String> getJobLinks() {
    List<String> jobLinks = new LinkedList<>();
    String marathonGroupLink = MarathonContext.getSchedulerURI(config)
        + MarathonConstants.JOB_LINK + Runtime.topologyName(runtime);
    jobLinks.add(marathonGroupLink);
    return jobLinks;
  }

  @Override
  public boolean onKill(Scheduler.KillTopologyRequest request) {
    return controller.killTopology();
  }

  @Override
  public boolean onRestart(Scheduler.RestartTopologyRequest request) {
    int appId = request.getContainerIndex();
    return controller.restartApp(appId);
  }

  protected String getTopologyConf(PackingPlan packing) {
    ObjectMapper mapper = new ObjectMapper();

    // TODO (nlu): use heterogeneous resources
    // Align resources to maximal requested resource
    PackingPlan.Resource containerResource = SchedulerUtils.getMaxRequiredResource(packing);
    // Add ram for tmaster container
    packing.resource.ram = containerResource.ram * (packing.containers.size() + 1);

    // Create app conf list for each container
    ArrayNode instances = mapper.createArrayNode();
    for (int i = 0; i < Runtime.numContainers(runtime); i++) {
      ObjectNode instance = mapper.createObjectNode();

      instance.put(MarathonConstants.ID, Integer.toString(i));
      instance.put(MarathonConstants.COMMAND, getExecutorCommand(i));
      instance.put(MarathonConstants.CPU, containerResource.cpu);
      instance.put(MarathonConstants.MEMORY, containerResource.ram / Constants.MB);
      instance.put(MarathonConstants.DISK, containerResource.disk / Constants.MB);
      instance.set(MarathonConstants.PORT_DEFINITIONS, getPorts(mapper));
      instance.put(MarathonConstants.INSTANCES, 1);
      instance.set(MarathonConstants.LABELS, getLabels(mapper));
      instance.set(MarathonConstants.FETCH, getFetchList(mapper));
      instance.put(MarathonConstants.USER, Context.role(config));

      instances.add(instance);
    }

    // Create marathon group for a topology
    ObjectNode appConf = mapper.createObjectNode();
    appConf.put(MarathonConstants.ID, Runtime.topologyName(runtime));
    appConf.set(MarathonConstants.APPS, instances);

    return appConf.toString();
  }

  protected  ObjectNode getLabels(ObjectMapper mapper) {
    ObjectNode labelNode = mapper.createObjectNode();
    labelNode.put(MarathonConstants.ENVIRONMENT, Context.environ(config));
    return labelNode;
  }

  protected ArrayNode getFetchList(ObjectMapper mapper) {
    String heronCoreURI = Context.corePackageUri(config);
    String topologyURI = Runtime.topologyPackageUri(runtime).toString();

    String[] uris = new String[]{heronCoreURI, topologyURI};

    ArrayNode urisNode = mapper.createArrayNode();
    for (String uri : uris) {
      ObjectNode uriObject = mapper.createObjectNode();
      uriObject.put(MarathonConstants.URI, uri);
      uriObject.put(MarathonConstants.EXECUTABLE, false);
      uriObject.put(MarathonConstants.EXTRACT, true);
      uriObject.put(MarathonConstants.CACHE, false);

      urisNode.add(uriObject);
    }

    return urisNode;
  }

  protected ArrayNode getPorts(ObjectMapper mapper) {
    ArrayNode ports = mapper.createArrayNode();

    for (String portName : MarathonConstants.PORT_NAMES) {
      ObjectNode port = mapper.createObjectNode();
      port.put(MarathonConstants.PORT, 0);
      port.put(MarathonConstants.PROTOCOL, MarathonConstants.TCP);
      port.put(MarathonConstants.PORT_NAME, portName);

      ports.add(port);
    }

    return ports;
  }

  protected String getExecutorCommand(int containerIndex) {
    List<String> commands = new ArrayList<>();
    commands.add(Context.executorSandboxBinary(config));
    commands.add(Integer.toString(containerIndex));

    TopologyAPI.Topology topology = Runtime.topology(runtime);
    commands.add(topology.getName());
    commands.add(topology.getId());
    commands.add(FileUtils.getBaseName(Context.topologyDefinitionFile(config)));
    commands.add(Runtime.instanceDistribution(runtime));
    commands.add(Context.stateManagerConnectionString(config));
    commands.add(Context.stateManagerRootPath(config));
    commands.add(Context.tmasterSandboxBinary(config));
    commands.add(Context.stmgrSandboxBinary(config));
    commands.add(Context.metricsManagerSandboxClassPath(config));
    commands.add(SchedulerUtils.encodeJavaOpts(TopologyUtils.getInstanceJvmOptions(topology)));
    commands.add(TopologyUtils.makeClassPath(topology, Context.topologyJarFile(config)));
    commands.add(MarathonConstants.MASTER_PORT);
    commands.add(MarathonConstants.TMASTER_CONTROLLER_PORT);
    commands.add(MarathonConstants.TMASTER_STATS_PORT);
    commands.add(Context.systemConfigSandboxFile(config));
    commands.add(Runtime.componentRamMap(runtime));
    commands.add(SchedulerUtils.encodeJavaOpts(TopologyUtils.getComponentJvmOptions(topology)));
    commands.add(Context.topologyPackageType(config));
    commands.add(Context.topologyJarFile(config));
    commands.add(Context.javaSandboxHome(config));
    commands.add(MarathonConstants.SHELL_PORT);
    commands.add(Context.shellSandboxBinary(config));
    commands.add(MarathonConstants.METRICSMGR_PORT);
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
    commands.add(MarathonConstants.SCHEDULER_PORT);

    return Joiner.on(" ").join(commands);
  }
}
