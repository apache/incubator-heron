//  Copyright 2017 Twitter. All rights reserved.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License
package com.twitter.heron.scheduler.kubernetes;

import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Joiner;

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

/**
 * Created by john on 5/20/17.
 */
public class KubernetesController implements IScheduler {
  private static final Logger LOG = Logger.getLogger(KubernetesScheduler.class.getName());

  private Config config;
  private Config runtime;
  private KubernetesController controller;

  @Override
  public void initialize(Config config, Config runtime) {
    this.config = aConfig;
    this.runtime = aRuntime;
    this.controller = getController();
  }

  protected KubernetesController getController() {
    return new KubernetesController(
        KubernetesContext.getSchedulerURI(config),
        Runtime.topologyName(runtime),
        Context.verbose(config));
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

    String topologyConf = getTopologyConf(packing);

    return controller.submitTopology(topologyConf);
  }

  @Override
  public List<String> getJobLinks() {
    return null;
  }

  @Override
  public boolean onKill(Scheduler.KillTopologyRequest request) { return controller.killTopology(); }

  @Override
  public boolean onRestart(Scheduler.RestartTopologyRequest request) {
    return false;
  }

  @Override
  public boolean onUpdate(Scheduler.UpdateTopologyRequest request) {
    LOG.severe("Topology onUpdate not implemented by this scheduler.");
    return false;
  }

  protected String getTopologyConf(PackingPlan packing) {

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
    ArrayNode instances = mapper.createArrayNode();
    for (int i = 0; i < Runtime.numContainers(runtime); i++) {
      ObjectNode instance = mapper.createObjectNode();

      instance.put("apiVersion", "extensions/v1beta1");
      instance.put("kind", "Deployment");
      instance.set("metadata", getMetadata(mapper, i));
      instance.set("spec", getPodTemplateSpec(mapper, i));


      instance.put(KubernetesConstants.ID, Integer.toString(i));
      instance.put(KubernetesConstants.COMMAND, getExecutorCommand(i));
      //instance.put(KubernetesConstants.CPU, containerResource.getCpu());
      instance.set(KubernetesConstants.CONTAINER, getContainer(mapper));
      //instance.put(KubernetesConstants.MEMORY, containerResource.getRam().asMegabytes());
      //instance.put(KubernetesConstants.DISK, containerResource.getDisk().asMegabytes());
      instance.put(KubernetesConstants.INSTANCES, 1);
      instance.set(KubernetesConstants.LABELS, getLabels(mapper));
      instance.set(KubernetesConstants.FETCH, getFetchList(mapper));

      instances.add(instance);
    }

    // Create kubernetes deployment for a topology
    ObjectNode deploymentConf = mapper.createObjectNode();
    deploymentConf.put(KubernetesConstants.ID, Runtime.topologyName(runtime));
    deploymentConf.set(KubernetesConstants.APPS, instances);

    return deploymentConf.toString();
  }

  // build the metadata for a deployment
  protected ObjectNode getMetadata(ObjectMapper mapper, int containerIndex) {
    ObjectNode metadataNode = mapper.createObjectNode();
    metadataNode.put("name", Joiner.on("-").join(Runtime.topologyName(runtime), containerIndex));
    metadataNode.put("namespace", Runtime.topologyName(runtime));

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
  protected ObjectNode getContainerSpec(ObjectMapper mapper, int containerIndex) {
    ObjectNode containerSpec = mapper.createObjectNode();
    ArrayNode containerList = mapper.createArrayNode();

    ObjectNode containerInfo = mapper.createObjectNode();
    containerInfo.put("name", Joiner.on("-").join("executor", Integer.toString(containerIndex)));
    containerInfo.put("image", KubernetesConstants.DOCKER_IMAGE);
    containerInfo.set("ports", getPorts(mapper));
    containerInfo.set("command", getExecutorCommand(containerIndex));

    containerList.add(containerInfo);

    containerSpec.set("containers", containerList);

    return containerSpec;
  }

  // build the pod template spec for the deployment
  protected ObjectNode getPodTemplateSpec(ObjectMapper mapper, int containerIndex) {
    ObjectNode templateNode = mapper.createObjectNode();

    // Pod metadata
    ObjectNode podMetadata = getPodMetadata(mapper, containerIndex);
    templateNode.set("metadata", podMetadata);

    // Container spec
    templateNode.set("spec", getContainerSpec(mapper, containerIndex));

    return templateNode;
  }


  protected ObjectNode getLabels(ObjectMapper mapper) {
    ObjectNode labelNode = mapper.createObjectNode();
    labelNode.put(KubernetesConstants.ENVIRONMENT, Context.environ(config));
    return labelNode;
  }

  protected ArrayNode getFetchList(ObjectMapper mapper) {
    String heronCoreURI = Context.corePackageUri(config);
    String topologyURI = Runtime.topologyPackageUri(runtime).toString();

    String[] uris = new String[]{heronCoreURI, topologyURI};

    ArrayNode urisNode = mapper.createArrayNode();
    for (String uri : uris) {
      ObjectNode uriObject = mapper.createObjectNode();
      uriObject.put(KubernetesConstants.URI, uri);
      uriObject.put(KubernetesConstants.EXECUTABLE, false);
      uriObject.put(KubernetesConstants.EXTRACT, true);
      uriObject.put(KubernetesConstants.CACHE, false);

      urisNode.add(uriObject);
    }

    return urisNode;
  }

  protected ArrayNode getPorts(ObjectMapper mapper) {
    ArrayNode ports = mapper.createArrayNode();

    for (String portName : KubernetesConstants.PORT_NAMES) {
      ObjectNode port = mapper.createObjectNode();
      port.put(KubernetesConstants.DOCKER_CONTAINER_PORT, 0);
      //port.put(KubernetesConstants.PROTOCOL, KubernetesConstants.TCP);
      //port.put(KubernetesConstants.HOST_PORT, 0);
      port.put(KubernetesConstants.PORT_NAME, portName);

      ports.add(port);
    }

    return ports;
  }

  protected String[] getExecutorCommand(ObjectMapper mapper, int containerIndex) {
    String[] commands = SchedulerUtils.getExecutorCommand(config, runtime,
        containerIndex, Arrays.asList(KubernetesConstants.PORT_LIST));

    ArrayNode commandsArray = mapper.createArrayNode();

    return commands;
  }
}
