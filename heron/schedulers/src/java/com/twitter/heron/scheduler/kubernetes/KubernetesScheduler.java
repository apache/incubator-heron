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

public class KubernetesScheduler implements IScheduler, IScalable {
  private static final Logger LOG = Logger.getLogger(KubernetesScheduler.class.getName());

  private Config configuration;
  private Config runtimeConfiguration;
  private KubernetesController controller;
  private UpdateTopologyManager updateTopologyManager;

  protected KubernetesController getController() {
    return new KubernetesController(
        KubernetesContext.getSchedulerURI(configuration),
        KubernetesContext.getKubernetesNamespace(configuration),
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
    if (!imagePullPolicyIsValid(KubernetesContext.getKubernetesImagePullPolicy(config))) {
      throw new RuntimeException(
          getInvalidImagePullPolicyMessage(KubernetesContext.getKubernetesImagePullPolicy(config))
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

    LOG.info("Submitting topology to Kubernetes");

    String[] topologyConf = getTopologyConf(packing);

    return controller.submitTopology(topologyConf);
  }

  @Override
  public List<String> getJobLinks() {
    List<String> jobLinks = new LinkedList<>();
    String kubernetesPodsLink = KubernetesContext.getSchedulerURI(configuration)
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

      deploymentConfs[i] = buildKubernetesReplicationControllerSpec(mapper, i, containerResource);
    }

    return deploymentConfs;
  }

  protected String buildKubernetesReplicationControllerSpec(ObjectMapper mapper,
                                                            Integer containerIndex,
                                                            Resource containerResource) {
    ObjectNode instance = mapper.createObjectNode();

    instance.put(KubernetesConstants.API_VERSION, KubernetesConstants.API_VERSION_1);
    instance.put(KubernetesConstants.API_KIND, KubernetesConstants.API_RC);
    instance.set(KubernetesConstants.API_METADATA, getMetadata(mapper, containerIndex));

    instance.set(KubernetesConstants.API_SPEC, getReplicationControllerSpec(mapper,
        containerIndex,
        containerResource));

    return instance.toString();


  }

  protected ObjectNode getReplicationControllerSpec(ObjectMapper mapper,
                                                int containerIndex,
                                                Resource containerResource) {
    ObjectNode instance = mapper.createObjectNode();

    instance.put("replicas", 1);

    ObjectNode selectorInstance = mapper.createObjectNode();

    selectorInstance.put(KubernetesConstants.TOPOLOGY_LABEL,
        Runtime.topologyName(runtimeConfiguration));

    instance.set("selector", selectorInstance);

    ObjectNode templateInstance = mapper.createObjectNode();
    templateInstance.set("metadata", getMetadata(mapper, containerIndex));
    templateInstance.set("spec", getContainerSpec(mapper, containerIndex, containerResource));

    instance.set("template", templateInstance);

    return instance;
  }

  /**
   * Build a specification object for a K8S Pod, which will house the Docker image and necessary
   * setup information about the Pod so we can run a Heron container
   *
   * @param mapper - ObjectMapper instance we can use to create new JSON nodes
   * @param containerIndex - The index of the container
   * @param containerResource - The Resource object for the new container
   */
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
    metadataNode.put(KubernetesConstants.NAME,
        Joiner.on("-").join(Runtime.topologyName(runtimeConfiguration), containerIndex));

    ObjectNode labels = mapper.createObjectNode();
    labels.put(KubernetesConstants.TOPOLOGY_LABEL, Runtime.topologyName(runtimeConfiguration));

    metadataNode.set(KubernetesConstants.METADATA_LABELS, labels);

    ObjectNode annotations = mapper.createObjectNode();
    applyPrometheusAnnotations(annotations);
    metadataNode.set(KubernetesConstants.METADATA_ANNOTATIONS, annotations);

    return metadataNode;
  }

  private void applyPrometheusAnnotations(ObjectNode node) {
    node.put(KubernetesConstants.ANNOTATION_PROMETHEUS_SCRAPE, "true");
    node.put(KubernetesConstants.ANNOTATION_PROMETHEUS_PORT, KubernetesConstants.PROMETHEUS_PORT);
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

      // Set the image pull policy for this container
      setImagePullPolicyIfPresent(containerInfo);

      // Port info -- all the same
      containerInfo.set(KubernetesConstants.PORTS, getPorts(mapper));

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
      containerInfo.set(KubernetesConstants.COMMAND, commandsArray);

      // Requested resource info
      ObjectNode requestedResourceInfo = mapper.createObjectNode();
      requestedResourceInfo.put(KubernetesConstants.MEMORY,
          containerPlan.getRequiredResource().getRam().asMegabytes());
      requestedResourceInfo.put(KubernetesConstants.CPU,
          containerPlan.getRequiredResource().getCpu());

      // Wrap it up into a resources dictionary
      ObjectNode resourceInfo = mapper.createObjectNode();
      resourceInfo.set(KubernetesConstants.REQUESTS, requestedResourceInfo);

      containerInfo.set(KubernetesConstants.RESOURCES, resourceInfo);

      containerList.add(containerInfo);

      containerSpec.set(KubernetesConstants.CONTAINERS, containerList);
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
        KubernetesContext.getExecutorDockerImage(configuration));

    // Set the image pull policy for this container
    setImagePullPolicyIfPresent(containerInfo);

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

  /**
   * Get the ports the container will need to expose so other containers can access its services
   *
   * @param mapper
   */
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

  private void setImagePullPolicyIfPresent(ObjectNode containerInfo) {
    if (KubernetesContext.hasImagePullPolicy(configuration)) {
      containerInfo.put(KubernetesConstants.IMAGE_PULL_POLICY,
          KubernetesContext.getKubernetesImagePullPolicy(configuration));
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
        containerIndex, Arrays.asList(KubernetesConstants.PORT_LIST));
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
   * NOTE: Due to the mechanics of Kubernetes pod creation, each container must be created on
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
      podConfig = controller.getBaseRC(basePodName);
    } catch (IOException ioe) {
      throw new TopologyRuntimeManagementException("Unable to retrieve base pod configuration from "
          + basePodName, ioe);
    }

    // iterate over the containers we need to add and rebuild the spec based on the new plan
    ObjectMapper mapper = new ObjectMapper();
    int totalNewContainerCount = containersToAdd.size();
    int deployedContainerCount = 0;
    for (PackingPlan.ContainerPlan containerPlan : containersToAdd) {
      String newContainer = rebuildKubernetesPodSpec(podConfig, mapper, containerPlan, 0);

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
   * NOTE: Due to the mechanics of Kubernetes pod removal, each container must be removed on
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
    final Matcher matcher = KubernetesConstants.VALID_POD_NAME_REGEX.matcher(topologyName);
    return matcher.matches();
  }

  static boolean imagePullPolicyIsValid(String imagePullPolicy) {
    if (imagePullPolicy == null || imagePullPolicy.isEmpty()) {
      return true;
    }
    return KubernetesConstants.VALID_IMAGE_PULL_POLICIES.contains(imagePullPolicy);
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
        policy, KubernetesConstants.VALID_IMAGE_PULL_POLICIES.toString());
  }
}
