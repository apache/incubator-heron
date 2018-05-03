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

package org.apache.heron.scheduler.marathon;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Joiner;

import org.apache.heron.proto.scheduler.Scheduler;
import org.apache.heron.scheduler.utils.Runtime;
import org.apache.heron.scheduler.utils.SchedulerUtils;
import org.apache.heron.scheduler.utils.SchedulerUtils.ExecutorPort;
import org.apache.heron.spi.common.Config;
import org.apache.heron.spi.common.Context;
import org.apache.heron.spi.common.Key;
import org.apache.heron.spi.packing.PackingPlan;
import org.apache.heron.spi.packing.Resource;
import org.apache.heron.spi.scheduler.IScheduler;


public class MarathonScheduler implements IScheduler {
  private static final Logger LOG = Logger.getLogger(MarathonScheduler.class.getName());

  private Config config;
  private Config runtime;
  private MarathonController controller;
  private String marathonGroupId;

  @Override
  public void initialize(Config aConfig, Config aRuntime) {
    this.config = aConfig;
    this.runtime = aRuntime;
    this.marathonGroupId = MarathonConstants.MARATHON_GROUP_PATH + Runtime.topologyName(runtime);
    this.controller = getController();
  }

  protected MarathonController getController() {

    return new MarathonController(
        MarathonContext.getSchedulerURI(config),
        MarathonContext.getSchedulerAuthToken(config),
        marathonGroupId,
        Context.verbose(config));
  }

  @Override
  public void close() {
    // Do nothing
  }

  @Override
  public boolean onSchedule(PackingPlan packing) {
    if (packing == null || packing.getContainers().isEmpty()) {
      LOG.severe("No container requested. Can't schedule");
      return false;
    }

    LOG.info("Submitting topology to Marathon Scheduler");

    String topologyConf = getTopologyConf(packing);

    return controller.submitTopology(topologyConf);
  }

  @Override
  public boolean onUpdate(Scheduler.UpdateTopologyRequest request) {
    LOG.severe("Topology onUpdate not implemented by this scheduler.");
    return false;
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

    config = Config.newBuilder()
        .putAll(config)
        .put(Key.TOPOLOGY_BINARY_FILE,
            Context.topologyBinaryFile(config))
        .build();

    ObjectMapper mapper = new ObjectMapper();

    // TODO (nlu): use heterogeneous resources
    // Align resources to maximal requested resource
    PackingPlan updatedPackingPlan = packing.cloneWithHomogeneousScheduledResource();
    SchedulerUtils.persistUpdatedPackingPlan(Runtime.topologyName(runtime),
        updatedPackingPlan, Runtime.schedulerStateManagerAdaptor(runtime));

    Resource containerResource = updatedPackingPlan.getContainers()
        .iterator().next().getRequiredResource();

    // Create app conf list for each container
    ArrayNode instances = mapper.createArrayNode();
    for (int i = 0; i < Runtime.numContainers(runtime); i++) {
      ObjectNode instance = mapper.createObjectNode();

      instance.put(MarathonConstants.ID, marathonGroupId + "/" + Integer.toString(i));
      instance.put(MarathonConstants.COMMAND, getExecutorCommand(i));
      instance.put(MarathonConstants.CPU, containerResource.getCpu());
      instance.set(MarathonConstants.CONTAINER, getContainer(mapper));
      instance.put(MarathonConstants.MEMORY, containerResource.getRam().asMegabytes());
      instance.put(MarathonConstants.DISK, containerResource.getDisk().asMegabytes());
      instance.put(MarathonConstants.INSTANCES, 1);
      instance.set(MarathonConstants.LABELS, getLabels(mapper));
      instance.set(MarathonConstants.FETCH, getFetchList(mapper));

      instances.add(instance);
    }

    // Create marathon group for a topology
    ObjectNode appConf = mapper.createObjectNode();
    appConf.put(MarathonConstants.ID, marathonGroupId);
    appConf.set(MarathonConstants.APPS, instances);

    return appConf.toString();
  }

  // build the container object
  protected ObjectNode getContainer(ObjectMapper mapper) {
    ObjectNode containerNode = mapper.createObjectNode();
    containerNode.put(MarathonConstants.CONTAINER_TYPE, "DOCKER");
    containerNode.set("docker", getDockerContainer(mapper));

    return containerNode;
  }

  protected ObjectNode getDockerContainer(ObjectMapper mapper) {
    ObjectNode dockerNode = mapper.createObjectNode();

    dockerNode.put(MarathonConstants.DOCKER_IMAGE,
        MarathonContext.getExecutorDockerImage(config));
    dockerNode.put(MarathonConstants.DOCKER_NETWORK, MarathonConstants.DOCKER_NETWORK_BRIDGE);
    dockerNode.put(MarathonConstants.DOCKER_PRIVILEGED, false);
    dockerNode.put(MarathonConstants.DOCKER_FORCE_PULL, true);
    dockerNode.set(MarathonConstants.DOCKER_PORT_MAPPINGS, getPorts(mapper));

    return dockerNode;
  }

  protected ObjectNode getLabels(ObjectMapper mapper) {
    ObjectNode labelNode = mapper.createObjectNode();
    labelNode.put(MarathonConstants.ENVIRONMENT, Context.environ(config));
    return labelNode;
  }

  protected ArrayNode getFetchList(ObjectMapper mapper) {
    final String topologyURI = Runtime.topologyPackageUri(runtime).toString();

    final String[] uris = new String[]{topologyURI};

    final ArrayNode urisNode = mapper.createArrayNode();
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

    for (Map.Entry<ExecutorPort, String> entry
        : MarathonConstants.EXECUTOR_PORTS.entrySet()) {
      ObjectNode port = mapper.createObjectNode();
      port.put(MarathonConstants.DOCKER_CONTAINER_PORT, 0);
      port.put(MarathonConstants.PROTOCOL, MarathonConstants.TCP);
      port.put(MarathonConstants.HOST_PORT, 0);
      port.put(MarathonConstants.PORT_NAME, entry.getKey().getName());

      ports.add(port);
    }

    return ports;
  }

  protected String getExecutorCommand(int containerIndex) {
    String[] commands = SchedulerUtils.getExecutorCommand(config, runtime,
        containerIndex, MarathonConstants.EXECUTOR_PORTS);
    return "cd $MESOS_SANDBOX && " + Joiner.on(" ").join(commands);
  }
}
