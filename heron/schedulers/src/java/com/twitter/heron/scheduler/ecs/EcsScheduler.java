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

package com.twitter.heron.scheduler.ecs;

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.primitives.Ints;

import com.twitter.heron.common.basics.FileUtils;
import com.twitter.heron.proto.scheduler.Scheduler;
import com.twitter.heron.scheduler.TopologyRuntimeManagementException;
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

public class EcsScheduler implements IScheduler, IScalable {
  private static final Logger LOG = Logger.getLogger(EcsScheduler.class.getName());

  private Config configuration;
  private Config runtimeConfiguration;
  private EcsController controller;
  private UpdateTopologyManager updateTopologyManager;

  protected EcsController getController() {
    return new EcsController(
        EcsContext.getSchedulerURI(configuration),
        EcsContext.getEcsNamespace(configuration),
        Runtime.topologyName(runtimeConfiguration),
        Context.verbose(configuration));
  }

  @Override
  public void initialize(Config config, Config runtime) {
    // validate the topology name before moving forward
    if (!topologyNameIsValid(Runtime.topologyName(runtime))) {
      throw new RuntimeException(getInvalidTopologyNameMessage(Runtime.topologyName(runtime)));
    }

    // validate that the image pull policy has been set correctly
    if (!imagePullPolicyIsValid(EcsContext.getEcsImagePullPolicy(config))) {
      throw new RuntimeException(
          getInvalidImagePullPolicyMessage(EcsContext.getEcsImagePullPolicy(config))
      );
    }

    this.configuration = config;
    this.runtimeConfiguration = runtime;
    this.controller = getController();
    this.updateTopologyManager =
        new UpdateTopologyManager(configuration, runtimeConfiguration,
            Optional.<IScalable>of(this));
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

    LOG.info("Submitting topology to Ecs");

    String[] topologyConf = getTopologyConf(packing);

    return controller.submitTopology(topologyConf);
  }

  @Override
  public List<String> getJobLinks() {
    List<String> jobLinks = new LinkedList<>();
    String kubernetesPodsLink = EcsContext.getSchedulerURI(configuration)
        + EcsConstants.JOB_LINK;
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

  /**
   * Build all the pod specifications so we can deploy the Heron topology. This will be a list of
   * JSON strings that contain all the necessary pod specifications we'll need to pass to the
   * K8S API
   *
   * @param packing - PackingPlan of the topology
   */
  protected String[] getTopologyConf(PackingPlan packing) {

    configuration = Config.newBuilder()
        .putAll(configuration)
        .put(Key.TOPOLOGY_BINARY_FILE,
            FileUtils.getBaseName(Context.topologyBinaryFile(configuration)))
        .build();

    ObjectMapper mapper = new ObjectMapper();

    // Align resources to maximal requested resource
    PackingPlan updatedPackingPlan = packing.cloneWithHomogeneousScheduledResource();
    SchedulerUtils.persistUpdatedPackingPlan(Runtime.topologyName(runtimeConfiguration),
        updatedPackingPlan, Runtime.schedulerStateManagerAdaptor(runtimeConfiguration));

    Resource containerResource = updatedPackingPlan.getContainers()
        .iterator().next().getScheduledResource().get();

    // Create app conf list for each container

    String[] deploymentConfs =
        new String[Ints.checkedCast(Runtime.numContainers(runtimeConfiguration))];
    for (int i = 0; i < Runtime.numContainers(runtimeConfiguration); i++) {

      deploymentConfs[i] = buildEcsPodSpec(mapper, i, containerResource);
    }

    return deploymentConfs;
  }

  /**
   * Build a specification object for a K8S Pod, which will house the Docker image and necessary
   * setup information about the Pod so we can run a Heron container
   *
   * @param mapper - ObjectMapper instance we can use to create new JSON nodes
   * @param containerIndex - The index of the container
   * @param containerResource - The Resource object for the new container
   */
  protected String buildEcsPodSpec(ObjectMapper mapper,
                                          Integer containerIndex,
                                          Resource containerResource) {
    ObjectNode instance = mapper.createObjectNode();

    instance.put(EcsConstants.API_VERSION, EcsConstants.API_VERSION_1);
    instance.put(EcsConstants.API_KIND, EcsConstants.API_POD);
    instance.set(EcsConstants.API_METADATA, getMetadata(mapper, containerIndex));

    instance.set(EcsConstants.API_SPEC, getContainerSpec(mapper,
        containerIndex,
        containerResource));

    return instance.toString();
  }

  /**
   * Build a new container spec based of an existing spec. This is used when we want to copy
   * an existing spec so we can easily create a new one, while replacing necessary information like
   * the container index
   *
   * @param podSpec - Existing pod specification JSON object
   * @param mapper - ObjectMapper instance we can use to create new JSON nodes
   * @param containerPlan - The ContainerPlan for the new container
   * @param oldContainerIndex - The index of the existing container
   */
  protected String rebuildEcsPodSpec(JsonNode podSpec,
                                            ObjectMapper mapper,
                                            PackingPlan.ContainerPlan containerPlan,
                                            Integer oldContainerIndex) {

    ObjectNode newContainer = mapper.createObjectNode();

    newContainer.put(EcsConstants.API_VERSION,
        podSpec.get(EcsConstants.API_VERSION).asText());
    newContainer.put(EcsConstants.API_KIND,
        podSpec.get(EcsConstants.API_KIND).asText());
    newContainer.set(EcsConstants.API_METADATA, getMetadata(mapper, containerPlan.getId()));

    newContainer.set(EcsConstants.API_SPEC, rebuildContainerSpec(
        podSpec.get(EcsConstants.API_SPEC),
        mapper,
        containerPlan,
        oldContainerIndex));

    return newContainer.toString();
  }

  /**
   * Build the metadata that we're going to attach to our Pod specification. This metadata
   * will allow us to query the set of pods that belong to a certain topology so we can easily
   * retrieve or delete all of them at once.
   *
   * @param mapper - ObjectMapper instance we can use to create other JSON nodes
   * @param containerIndex - Index of the container
   */
  protected ObjectNode getMetadata(ObjectMapper mapper, int containerIndex) {
    ObjectNode metadataNode = mapper.createObjectNode();
    metadataNode.put(EcsConstants.NAME,
        Joiner.on("-").join(Runtime.topologyName(runtimeConfiguration), containerIndex));

    ObjectNode labels = mapper.createObjectNode();
    labels.put(EcsConstants.TOPOLOGY_LABEL, Runtime.topologyName(runtimeConfiguration));

    metadataNode.set(EcsConstants.METADATA_LABELS, labels);

    ObjectNode annotations = mapper.createObjectNode();
    applyPrometheusAnnotations(annotations);
    metadataNode.set(EcsConstants.METADATA_ANNOTATIONS, annotations);

    return metadataNode;
  }

  private void applyPrometheusAnnotations(ObjectNode node) {
    node.put(EcsConstants.ANNOTATION_PROMETHEUS_SCRAPE, "true");
    node.put(EcsConstants.ANNOTATION_PROMETHEUS_PORT, EcsConstants.PROMETHEUS_PORT);
  }

  /**
   * Based on an existing container spec, rebuild it and replace necessary information for building
   * a new container (primarily the container index)
   *
   * @param existingSpec - Existing container spec
   * @param mapper - ObjectMapper instance to use for creating new JSON nodes
   * @param containerPlan - New container's ContainerPlan
   * @param oldContainerIndex - The index of the old container
   */
  protected ObjectNode rebuildContainerSpec(JsonNode existingSpec,
                                            ObjectMapper mapper,
                                            PackingPlan.ContainerPlan containerPlan,
                                            int oldContainerIndex) {

    ObjectNode containerSpec = mapper.createObjectNode();
    ArrayNode containerList = mapper.createArrayNode();

    for (JsonNode existingContainer : existingSpec.get("containers")) {
      ObjectNode containerInfo = mapper.createObjectNode();
      containerInfo.put(EcsConstants.NAME, Joiner.on("-").join("executor",
          Integer.toString(containerPlan.getId())));

      // set the host for this container
      ArrayNode envList = mapper.createArrayNode();
      ObjectNode envVar = mapper.createObjectNode();
      envVar.put(EcsConstants.NAME, EcsConstants.HOST);

      // build the JSON to attach the Pod IP as the "HOST" environment variable
      ObjectNode fieldRef = mapper.createObjectNode();
      ObjectNode fieldPath = mapper.createObjectNode();
      fieldPath.put(EcsConstants.FIELD_PATH, EcsConstants.POD_IP);
      fieldRef.set(EcsConstants.FIELD_REF, fieldPath);
      envVar.set(EcsConstants.VALUE_FROM, fieldRef);
      envList.add(envVar);

      containerInfo.set(EcsConstants.ENV, envList);

      // set the docker image to the same image as the existing container
      containerInfo.put(EcsConstants.DOCKER_IMAGE,
          existingContainer.get(EcsConstants.DOCKER_IMAGE).asText());

      // Set the image pull policy for this container
      setImagePullPolicyIfPresent(containerInfo);

      // Port info -- all the same
      containerInfo.set(EcsConstants.PORTS, getPorts(mapper));

      // In order for the container to run with the correct index, we're copying the base
      // configuration for container with index 0, and replacing the container index with
      // the index for the new container we're going to deploy
      // Example: " 0 exclamationTopology" will be replaced w/ " <new_cntr_idx> exclamationTopology"
      // The rest of the command will stay the same
      ArrayNode commandsArray = mapper.createArrayNode();
      for (JsonNode cmd : existingContainer.get("command")) {
        String oldPattern = " " + oldContainerIndex + " "
            + Runtime.topologyName(runtimeConfiguration);
        commandsArray.add(cmd.asText().replaceAll(oldPattern,
            " " + containerPlan.getId() + " "
                + Runtime.topologyName(runtimeConfiguration)));
      }
      containerInfo.set(EcsConstants.COMMAND, commandsArray);

      // Requested resource info
      ObjectNode requestedResourceInfo = mapper.createObjectNode();
      requestedResourceInfo.put(EcsConstants.MEMORY,
          containerPlan.getRequiredResource().getRam().asMegabytes());
      requestedResourceInfo.put(EcsConstants.CPU,
          containerPlan.getRequiredResource().getCpu());

      // Wrap it up into a resources dictionary
      ObjectNode resourceInfo = mapper.createObjectNode();
      resourceInfo.set(EcsConstants.REQUESTS, requestedResourceInfo);

      containerInfo.set(EcsConstants.RESOURCES, resourceInfo);

      containerList.add(containerInfo);

      containerSpec.set(EcsConstants.CONTAINERS, containerList);
    }
    return containerSpec;
  }

  /**
   * Get the JSON-based specification for the K8S Pod
   *
   * @param mapper - An ObjectMapper instance which can be used to create JSON nodes
   * @param containerIndex - Index of the container
   * @param containerResource - The containers Resource object
   */
  protected ObjectNode getContainerSpec(ObjectMapper mapper,
                                        int containerIndex,
                                        Resource containerResource) {

    ObjectNode containerSpec = mapper.createObjectNode();
    ArrayNode containerList = mapper.createArrayNode();

    ObjectNode containerInfo = mapper.createObjectNode();
    containerInfo.put(EcsConstants.NAME, Joiner.on("-").join("executor",
        Integer.toString(containerIndex)));

    // set the host for this container
    ArrayNode envList = mapper.createArrayNode();
    ObjectNode envVar = mapper.createObjectNode();
    envVar.put(EcsConstants.NAME, EcsConstants.HOST);

    // build the JSON to attach the Pod IP as the "HOST" environment variable
    ObjectNode fieldRef = mapper.createObjectNode();
    ObjectNode fieldPath = mapper.createObjectNode();
    fieldPath.put(EcsConstants.FIELD_PATH, EcsConstants.POD_IP);
    fieldRef.set(EcsConstants.FIELD_REF, fieldPath);
    envVar.set(EcsConstants.VALUE_FROM, fieldRef);
    envList.add(envVar);

    containerInfo.set(EcsConstants.ENV, envList);

    // Image information for this container
    containerInfo.put(EcsConstants.DOCKER_IMAGE,
        EcsContext.getExecutorDockerImage(configuration));

    // Set the image pull policy for this container
    setImagePullPolicyIfPresent(containerInfo);

    // Port information for this container
    containerInfo.set(EcsConstants.PORTS, getPorts(mapper));

    // Heron command for the container
    String[] command = getExecutorCommand(containerIndex);
    ArrayNode commandsArray = mapper.createArrayNode();
    for (int i = 0; i < command.length; i++) {
      commandsArray.add(command[i]);
    }
    containerInfo.set(EcsConstants.COMMAND, commandsArray);

    // Requested resource info
    ObjectNode requestedResourceInfo = mapper.createObjectNode();
    requestedResourceInfo.put(EcsConstants.MEMORY, containerResource.getRam().asMegabytes());
    requestedResourceInfo.put(EcsConstants.CPU, containerResource.getCpu());

    // Wrap it up into a resources dictionary
    ObjectNode resourceInfo = mapper.createObjectNode();
    resourceInfo.set(EcsConstants.REQUESTS, requestedResourceInfo);

    containerInfo.set(EcsConstants.RESOURCES, resourceInfo);

    containerList.add(containerInfo);

    containerSpec.set(EcsConstants.CONTAINERS, containerList);

    return containerSpec;
  }

  /**
   * Get the ports the container will need to expose so other containers can access its services
   *
   * @param mapper
   */
  protected ArrayNode getPorts(ObjectMapper mapper) {
    ArrayNode ports = mapper.createArrayNode();

    for (int i = 0; i < EcsConstants.PORT_NAMES.length; i++) {
      ObjectNode port = mapper.createObjectNode();
      port.put(EcsConstants.DOCKER_CONTAINER_PORT,
          Integer.parseInt(EcsConstants.PORT_LIST[i], 10));
      port.put(EcsConstants.PORT_NAME, EcsConstants.PORT_NAMES[i]);
      ports.add(port);
    }

    return ports;
  }

  private void setImagePullPolicyIfPresent(ObjectNode containerInfo) {
    if (EcsContext.hasImagePullPolicy(configuration)) {
      containerInfo.put(EcsConstants.IMAGE_PULL_POLICY,
          EcsContext.getEcsImagePullPolicy(configuration));
    }
  }


  /**
   * Get the command that will be used to retrieve the topology JAR
   */
  static String getFetchCommand(Config config, Config runtime) {
    return String.format("%s %s .", Context.downloaderBinary(config),
        Runtime.topologyPackageUri(runtime).toString());
  }

  /**
   * Get the command string needed to start the container
   *
   * @param containerIndex
   */
  protected String[] getExecutorCommand(int containerIndex) {
    String[] executorCommand =
        SchedulerUtils.getExecutorCommand(configuration, runtimeConfiguration,
        containerIndex, Arrays.asList(EcsConstants.PORT_LIST));
    String[] command = {
        "sh",
        "-c",
        getFetchCommand(configuration, runtimeConfiguration)
        + " && " + Joiner.on(" ").join(executorCommand)
    };

    return command;
  }

  /**
   * Add containers for a scale-up event from an update command
   *
   * @param containersToAdd the list of containers that need to be added
   *
   * NOTE: Due to the mechanics of Ecs pod creation, each container must be created on
   * a one-by-one basis. If one container out of many containers to be deployed failed, it will
   * leave the topology in a bad state.
   *
   * TODO (jrcrawfo) -- (https://github.com/twitter/heron/issues/1981)
   */
  @Override
  public void addContainers(Set<PackingPlan.ContainerPlan> containersToAdd) {
    // grab the base pod so we can copy and modify some stuff
    String basePodName = Runtime.topologyName(runtimeConfiguration) + "-0";
    JsonNode podConfig;
    try {
      podConfig = controller.getBasePod(basePodName);
    } catch (IOException ioe) {
      throw new TopologyRuntimeManagementException("Unable to retrieve base pod configuration from "
          + basePodName, ioe);
    }

    // iterate over the containers we need to add and rebuild the spec based on the new plan
    ObjectMapper mapper = new ObjectMapper();
    int totalNewContainerCount = containersToAdd.size();
    int deployedContainerCount = 0;
    for (PackingPlan.ContainerPlan containerPlan : containersToAdd) {
      String newContainer = rebuildEcsPodSpec(podConfig, mapper, containerPlan, 0);

      // deploy this new container
      try {
        controller.deployContainer(newContainer);
        LOG.log(Level.INFO, "New container " + ++deployedContainerCount + "/"
            + totalNewContainerCount + " deployed");
      } catch (IOException ioe) {
        throw new TopologyRuntimeManagementException("Problem adding container with id "
            + containerPlan.getId() + ". Deployed " + deployedContainerCount + " out of "
            + totalNewContainerCount + " containers", ioe);
      }
    }
  }

  /**
   * Remove containers for a scale-down event from an update command
   *
   * @param containersToRemove the list of containers that need to be removed
   *
   * NOTE: Due to the mechanics of Ecs pod removal, each container must be removed on
   * a one-by-one basis. If one container out of many containers to be removed failed, it will
   * leave the topology in a bad state.
   *
   * TODO (jrcrawfo) -- (https://github.com/twitter/heron/issues/1981)
   */
  @Override
  public void removeContainers(Set<PackingPlan.ContainerPlan> containersToRemove) {
    for (PackingPlan.ContainerPlan container : containersToRemove) {
      String podName = Runtime.topologyName(runtimeConfiguration) + "-" + container.getId();
      try {
        controller.removeContainer(podName);
      } catch (IOException ioe) {
        throw new TopologyRuntimeManagementException("Problem removing container with id "
            + container.getId(), ioe);
      }
    }
  }

  static boolean topologyNameIsValid(String topologyName) {
    final Matcher matcher = EcsConstants.VALID_POD_NAME_REGEX.matcher(topologyName);
    return matcher.matches();
  }

  static boolean imagePullPolicyIsValid(String imagePullPolicy) {
    if (imagePullPolicy == null || imagePullPolicy.isEmpty()) {
      return true;
    }
    return EcsConstants.VALID_IMAGE_PULL_POLICIES.contains(imagePullPolicy);
  }

  private static String getInvalidTopologyNameMessage(String topologyName) {
    return String.format("Invalid topology name: \"%s\": "
        + "topology names in kubernetes must consist of lower case alphanumeric "
        + "characters, '-' or '.', and must start and end with an alphanumeric "
        + "character.", topologyName);
  }

  private static String getInvalidImagePullPolicyMessage(String policy) {
    return String.format("Invalid image pull policy: \"%s\": image pull polices must be one of "
        + " %s Defaults to Always if :latest tag is specified, or IfNotPresent otherwise.",
        policy, EcsConstants.VALID_IMAGE_PULL_POLICIES.toString());
  }
}
