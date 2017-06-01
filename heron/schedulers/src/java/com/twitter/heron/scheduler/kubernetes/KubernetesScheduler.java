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

package com.twitter.heron.scheduler.kubernetes;

import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Joiner;
import com.google.common.primitives.Ints;

import com.twitter.heron.common.basics.FileUtils;
import com.twitter.heron.proto.scheduler.Scheduler;
import com.twitter.heron.scheduler.utils.Runtime;
import com.twitter.heron.scheduler.utils.SchedulerUtils;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Context;
import com.twitter.heron.spi.common.Key;
import com.twitter.heron.spi.packing.PackingPlan;
import com.twitter.heron.spi.packing.Resource;
import com.twitter.heron.spi.scheduler.IScheduler;

public class KubernetesScheduler implements IScheduler {
  private static final Logger LOG = Logger.getLogger(KubernetesScheduler.class.getName());

  private Config config;
  private Config runtime;
  private KubernetesController controller;

  protected KubernetesController getController() {
    return new KubernetesController(
        KubernetesContext.getSchedulerURI(config),
        Runtime.topologyName(runtime),
        Context.verbose(config));
  }

  @Override
  public void initialize(Config aConfig, Config aRuntime) {
    this.config = aConfig;
    this.runtime = aRuntime;
    this.controller = getController();
  }

  @Override
  public void close() {
    // Nothing to do here
  }

  @Override
  public boolean onSchedule(PackingPlan packing) {
    if (packing == null || packing.getContainers().isEmpty()) {
      LOG.severe("No container requested. Can't schedule");
      return false;
    }

    LOG.info("Submitting topology to Kubernetes");

    String[] topologyConf = getTopologyConf(packing);

    return controller.submitTopology(topologyConf);
  }

  @Override
  public List<String> getJobLinks() {
    return null;
  }

  @Override
  public boolean onKill(Scheduler.KillTopologyRequest request) {
    return controller.killTopology();
  }

  @Override
  public boolean onRestart(Scheduler.RestartTopologyRequest request) {
    return false;
  }

  @Override
  public boolean onUpdate(Scheduler.UpdateTopologyRequest request) {
    LOG.severe("Topology onUpdate not implemented by this scheduler.");
    return false;
  }

  protected String[] getTopologyConf(PackingPlan packing) {

    config = Config.newBuilder()
        .putAll(config)
        .put(Key.TOPOLOGY_BINARY_FILE,
            FileUtils.getBaseName(Context.topologyBinaryFile(config)))
        .build();

    ObjectMapper mapper = new ObjectMapper();

    // Align resources to maximal requested resource
    PackingPlan updatedPackingPlan = packing.cloneWithHomogeneousScheduledResource();
    SchedulerUtils.persistUpdatedPackingPlan(Runtime.topologyName(runtime),
        updatedPackingPlan, Runtime.schedulerStateManagerAdaptor(runtime));

    Resource containerResource = updatedPackingPlan.getContainers()
        .iterator().next().getRequiredResource();

    // Create app conf list for each container

    String[] deploymentConfs = new String[Ints.checkedCast(Runtime.numContainers(runtime))];
    for (int i = 0; i < Runtime.numContainers(runtime); i++) {
      ObjectNode instance = mapper.createObjectNode();

      instance.put("apiVersion", "v1");
      instance.put("kind", "Pod");
      instance.set("metadata", getMetadata(mapper, i));

      instance.set("spec", getContainerSpec(mapper, i, containerResource));
      deploymentConfs[i] = instance.toString();
    }

    return deploymentConfs;
  }

  // build the metadata for a deployment
  protected ObjectNode getMetadata(ObjectMapper mapper, int containerIndex) {
    ObjectNode metadataNode = mapper.createObjectNode();
    metadataNode.put("name", Joiner.on("-").join(Runtime.topologyName(runtime), containerIndex));

    ObjectNode labels = mapper.createObjectNode();
    labels.put("topology", Runtime.topologyName(runtime));

    metadataNode.set("labels", labels);

    return metadataNode;
  }

  // build the metadata for a pod template
  protected ObjectNode getPodMetadata(ObjectMapper mapper, int containerIndex) {
    ObjectNode podMetadata = mapper.createObjectNode();
    ObjectNode labelData = mapper.createObjectNode();

    labelData.put("app", Integer.toString(containerIndex));
    podMetadata.set("labels", labelData);

    return podMetadata;
  }

  // build the container spec for the deployment
  protected ObjectNode getContainerSpec(ObjectMapper mapper,
                                        int containerIndex,
                                        Resource containerResource) {

    ObjectNode containerSpec = mapper.createObjectNode();
    ArrayNode containerList = mapper.createArrayNode();

    ObjectNode containerInfo = mapper.createObjectNode();
    containerInfo.put("name", Joiner.on("-").join("executor", Integer.toString(containerIndex)));

    // set the host for this container
    ArrayNode envList = mapper.createArrayNode();
    ObjectNode envVar = mapper.createObjectNode();
    envVar.put("name", "HOST");

    ObjectNode fieldRef = mapper.createObjectNode();

    ObjectNode fieldPath = mapper.createObjectNode();
    fieldPath.put("fieldPath", "status.podIP");

    fieldRef.set("fieldRef", fieldPath);

    envVar.set("valueFrom", fieldRef);

    envList.add(envVar);

    containerInfo.set("env", envList);

    // Image information for this container
    containerInfo.put("image", KubernetesContext.getExecutorDockerImage(config));

    // Port information for this container
    containerInfo.set("ports", getPorts(mapper));

    // Heron command for the container
    String[] command = getExecutorCommand(containerIndex);
    ArrayNode commandsArray = mapper.createArrayNode();
    for (int i = 0; i < command.length; i++) {
      commandsArray.add(command[i]);
    }
    containerInfo.set("command", commandsArray);

    // Requested resource info
    ObjectNode requestedResourceInfo = mapper.createObjectNode();
    requestedResourceInfo.put("memory", containerResource.getRam().asMegabytes());
    //requestedResourceInfo.put("cpu", containerResource.getCpu());

    // Wrap it up into a resources dictionary
    ObjectNode resourceInfo = mapper.createObjectNode();
    resourceInfo.set("requests", requestedResourceInfo);

    containerInfo.set("resources", resourceInfo);

    containerList.add(containerInfo);

    containerSpec.set("containers", containerList);

    return containerSpec;
  }

  // build the pod template spec for the deployment
  protected ObjectNode getPodTemplateSpec(ObjectMapper mapper,
                                          int containerIndex,
                                          Resource containerResource) {
    ObjectNode templateNode = mapper.createObjectNode();

    // Pod metadata
    ObjectNode podMetadata = getPodMetadata(mapper, containerIndex);
    templateNode.set("metadata", podMetadata);

    // Container spec
    templateNode.set("spec", getContainerSpec(mapper, containerIndex, containerResource));

    return templateNode;
  }


  protected ObjectNode getLabels(ObjectMapper mapper) {
    ObjectNode labelNode = mapper.createObjectNode();
    labelNode.put(KubernetesConstants.ENVIRONMENT, Context.environ(config));
    return labelNode;
  }


  protected ArrayNode getPorts(ObjectMapper mapper) {
    ArrayNode ports = mapper.createArrayNode();

    for (int i = 0; i < KubernetesConstants.PORT_NAMES.length; i++) {
      ObjectNode port = mapper.createObjectNode();
      port.put(KubernetesConstants.DOCKER_CONTAINER_PORT,
          Integer.parseInt(KubernetesConstants.PORT_LIST[i], 10));
      port.put(KubernetesConstants.PORT_NAME, KubernetesConstants.PORT_NAMES[i]);
      ports.add(port);
    }

    return ports;
  }

  protected String getFetchCommand() {
    return "cd /opt/heron/ && curl " + Runtime.topologyPackageUri(runtime).toString()
            + " | tar xvz";
  }

  protected String[] getExecutorCommand(int containerIndex) {
    String[] executorCommand = SchedulerUtils.getExecutorCommand(config, runtime,
        containerIndex, Arrays.asList(KubernetesConstants.PORT_LIST));
    String[] command = {
        "sh",
        "-c",
        getFetchCommand() + " && " + Joiner.on(" ").join(executorCommand)
    };

    return command;
  }
}
