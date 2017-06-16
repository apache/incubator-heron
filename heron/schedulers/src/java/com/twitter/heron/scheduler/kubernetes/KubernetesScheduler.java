// Copyright 2017 Twitter. All rights reserved.
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
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.primitives.Ints;

import com.twitter.heron.common.basics.FileUtils;
import com.twitter.heron.proto.scheduler.Scheduler;
import com.twitter.heron.scheduler.UpdateTopologyManager;
import com.twitter.heron.scheduler.utils.Runtime;
import com.twitter.heron.scheduler.utils.SchedulerUtils;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Context;
import com.twitter.heron.spi.common.Key;
import com.twitter.heron.spi.packing.PackingPlan;
import com.twitter.heron.spi.packing.Resource;
import com.twitter.heron.spi.scheduler.IScalable;
import com.twitter.heron.spi.scheduler.IScheduler;

public class KubernetesScheduler implements IScheduler, IScalable {
  private static final Logger LOG = Logger.getLogger(KubernetesScheduler.class.getName());

  private Config config;
  private Config runtime;
  private KubernetesController controller;
  private UpdateTopologyManager updateTopologyManager;

  protected KubernetesController getController() {
    return new KubernetesController(
        KubernetesContext.getSchedulerURI(config),
        Runtime.topologyName(runtime),
        Context.verbose(config));
  }

  @Override
  public void initialize(Config aConfig, Config aRuntime) {
    LOG.log(Level.INFO, aRuntime.toString());
    this.config = aConfig;
    this.runtime = aRuntime;
    this.controller = getController();
    this.updateTopologyManager =
        new UpdateTopologyManager(config, runtime, Optional.<IScalable>of(this));
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
    List<String> jobLinks = new LinkedList<>();
    String kubernetesPodsLink = KubernetesContext.getSchedulerURI(config)
        + KubernetesConstants.JOB_LINK;
    jobLinks.add(kubernetesPodsLink);
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
        .iterator().next().getScheduledResource().get();

    // Create app conf list for each container

    String[] deploymentConfs = new String[Ints.checkedCast(Runtime.numContainers(runtime))];
    for (int i = 0; i < Runtime.numContainers(runtime); i++) {

      deploymentConfs[i] = buildKubernetesPodSpec(mapper, i, containerResource);
    }

    return deploymentConfs;
  }

  // build the pod spec for a container
  protected String buildKubernetesPodSpec(ObjectMapper mapper,
                                          Integer containerIndex,
                                          Resource containerResource) {
    ObjectNode instance = mapper.createObjectNode();

    instance.put(KubernetesConstants.API_VERSION, KubernetesConstants.API_VERSION_1);
    instance.put(KubernetesConstants.API_KIND, KubernetesConstants.API_POD);
    instance.set(KubernetesConstants.API_METADATA, getMetadata(mapper, containerIndex));

    instance.set(KubernetesConstants.API_SPEC, getContainerSpec(mapper,
        containerIndex,
        containerResource));

    return instance.toString();
  }

  protected String rebuildKubernetesPodSpec(JsonNode podSpec,
                                            ObjectMapper mapper,
                                            PackingPlan.ContainerPlan containerPlan,
                                            Integer oldContainerIndex) {

    ObjectNode newContainer = mapper.createObjectNode();

    newContainer.put(KubernetesConstants.API_VERSION,
        podSpec.get(KubernetesConstants.API_VERSION).asText());
    newContainer.put(KubernetesConstants.API_KIND,
        podSpec.get(KubernetesConstants.API_KIND).asText());
    newContainer.set(KubernetesConstants.API_METADATA, getMetadata(mapper, containerPlan.getId()));

    newContainer.set(KubernetesConstants.API_SPEC, rebuildContainerSpec(
        podSpec.get(KubernetesConstants.API_SPEC),
        mapper,
        containerPlan,
        oldContainerIndex));

    return newContainer.toString();
  }


  // build the metadata for a deployment
  protected ObjectNode getMetadata(ObjectMapper mapper, int containerIndex) {
    ObjectNode metadataNode = mapper.createObjectNode();
    metadataNode.put(KubernetesConstants.NAME,
        Joiner.on("-").join(Runtime.topologyName(runtime), containerIndex));

    ObjectNode labels = mapper.createObjectNode();
    labels.put(KubernetesConstants.TOPOLOGY_LABEL, Runtime.topologyName(runtime));

    metadataNode.set(KubernetesConstants.METADATA_LABELS, labels);

    return metadataNode;
  }

  // build the metadata for a pod template
  protected ObjectNode getPodMetadata(ObjectMapper mapper, int containerIndex) {
    ObjectNode podMetadata = mapper.createObjectNode();
    ObjectNode labelData = mapper.createObjectNode();

    labelData.put(KubernetesConstants.APP, Integer.toString(containerIndex));
    podMetadata.set(KubernetesConstants.METADATA_LABELS, labelData);

    return podMetadata;
  }

  protected ObjectNode rebuildContainerSpec(JsonNode existingSpec,
                                            ObjectMapper mapper,
                                            PackingPlan.ContainerPlan containerPlan,
                                            int oldContainerIndex) {

    ObjectNode containerSpec = mapper.createObjectNode();
    ArrayNode containerList = mapper.createArrayNode();

    for (JsonNode existingContainer : existingSpec.get("containers")) {
      ObjectNode containerInfo = mapper.createObjectNode();
      containerInfo.put(KubernetesConstants.NAME, Joiner.on("-").join("executor",
          Integer.toString(containerPlan.getId())));

      // set the host for this container
      ArrayNode envList = mapper.createArrayNode();
      ObjectNode envVar = mapper.createObjectNode();
      envVar.put(KubernetesConstants.NAME, KubernetesConstants.HOST);

      // build the JSON to attach the Pod IP as the "HOST" environment variable
      ObjectNode fieldRef = mapper.createObjectNode();
      ObjectNode fieldPath = mapper.createObjectNode();
      fieldPath.put(KubernetesConstants.FIELD_PATH, KubernetesConstants.POD_IP);
      fieldRef.set(KubernetesConstants.FIELD_REF, fieldPath);
      envVar.set(KubernetesConstants.VALUE_FROM, fieldRef);
      envList.add(envVar);

      containerInfo.set(KubernetesConstants.ENV, envList);

      // set the docker image to the same image as the existing container
      containerInfo.put(KubernetesConstants.DOCKER_IMAGE,
          existingContainer.get(KubernetesConstants.DOCKER_IMAGE).asText());

      // Port info -- all the same
      containerInfo.set(KubernetesConstants.PORTS, getPorts(mapper));

      // Heron command for the new container
      ArrayNode commandsArray = mapper.createArrayNode();
      for (JsonNode cmd : existingContainer.get("command")) {
        String oldPattern = " " + oldContainerIndex + " " + Runtime.topologyName(runtime);
        commandsArray.add(cmd.asText().replaceAll(oldPattern,
            " " + containerPlan.getId() + " " + Runtime.topologyName(runtime)));
      }
      containerInfo.set(KubernetesConstants.COMMAND, commandsArray);

      // Requested resource info
      ObjectNode requestedResourceInfo = mapper.createObjectNode();
      requestedResourceInfo.put(KubernetesConstants.MEMORY,
          containerPlan.getScheduledResource().get().getRam().asMegabytes());
      requestedResourceInfo.put(KubernetesConstants.CPU,
          containerPlan.getScheduledResource().get().getCpu());

      // Wrap it up into a resources dictionary
      ObjectNode resourceInfo = mapper.createObjectNode();
      resourceInfo.set(KubernetesConstants.REQUESTS, requestedResourceInfo);

      containerInfo.set(KubernetesConstants.RESOURCES, resourceInfo);

      containerList.add(containerInfo);

      containerSpec.set(KubernetesConstants.CONTAINERS, containerList);
    }
    return containerSpec;
  }

  // build the container spec for the deployment
  protected ObjectNode getContainerSpec(ObjectMapper mapper,
                                        int containerIndex,
                                        Resource containerResource) {

    ObjectNode containerSpec = mapper.createObjectNode();
    ArrayNode containerList = mapper.createArrayNode();

    ObjectNode containerInfo = mapper.createObjectNode();
    containerInfo.put(KubernetesConstants.NAME, Joiner.on("-").join("executor",
        Integer.toString(containerIndex)));

    // set the host for this container
    ArrayNode envList = mapper.createArrayNode();
    ObjectNode envVar = mapper.createObjectNode();
    envVar.put(KubernetesConstants.NAME, KubernetesConstants.HOST);

    // build the JSON to attach the Pod IP as the "HOST" environment variable
    ObjectNode fieldRef = mapper.createObjectNode();
    ObjectNode fieldPath = mapper.createObjectNode();
    fieldPath.put(KubernetesConstants.FIELD_PATH, KubernetesConstants.POD_IP);
    fieldRef.set(KubernetesConstants.FIELD_REF, fieldPath);
    envVar.set(KubernetesConstants.VALUE_FROM, fieldRef);
    envList.add(envVar);

    containerInfo.set(KubernetesConstants.ENV, envList);

    // Image information for this container
    containerInfo.put(KubernetesConstants.DOCKER_IMAGE,
        KubernetesContext.getExecutorDockerImage(config));

    // Port information for this container
    containerInfo.set(KubernetesConstants.PORTS, getPorts(mapper));

    // Heron command for the container
    String[] command = getExecutorCommand(containerIndex);
    ArrayNode commandsArray = mapper.createArrayNode();
    for (int i = 0; i < command.length; i++) {
      commandsArray.add(command[i]);
    }
    containerInfo.set(KubernetesConstants.COMMAND, commandsArray);

    // Requested resource info
    ObjectNode requestedResourceInfo = mapper.createObjectNode();
    requestedResourceInfo.put(KubernetesConstants.MEMORY, containerResource.getRam().asMegabytes());
    requestedResourceInfo.put(KubernetesConstants.CPU, containerResource.getCpu());

    // Wrap it up into a resources dictionary
    ObjectNode resourceInfo = mapper.createObjectNode();
    resourceInfo.set(KubernetesConstants.REQUESTS, requestedResourceInfo);

    containerInfo.set(KubernetesConstants.RESOURCES, resourceInfo);

    containerList.add(containerInfo);

    containerSpec.set(KubernetesConstants.CONTAINERS, containerList);

    return containerSpec;
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

  @Override
  public void addContainers(Set<PackingPlan.ContainerPlan> containersToAdd) {
    // grab the base pod so we can copy and modify some stuff
    String basePodName = Runtime.topologyName(runtime) + "-0";
    JsonNode podConfig = controller.getBasePod(basePodName);

    // iterate over the containers we need to add and rebuild the spec based on the new plan
    ObjectMapper mapper = new ObjectMapper();
    for (PackingPlan.ContainerPlan containerPlan : containersToAdd) {
      String newContainer = rebuildKubernetesPodSpec(podConfig, mapper, containerPlan, 0);

      // deploy this new container
      controller.deployContainer(newContainer);
    }

  }

  @Override
  public void removeContainers(Set<PackingPlan.ContainerPlan> containersToRemove) {
    for (PackingPlan.ContainerPlan container : containersToRemove) {
      String podName = Runtime.topologyName(runtime) + "-" + container.getId();
      controller.removeContainer(podName);
    }
  }
}
