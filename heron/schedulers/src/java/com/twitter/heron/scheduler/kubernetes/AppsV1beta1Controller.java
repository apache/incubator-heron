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
//  limitations under the License.
package com.twitter.heron.scheduler.kubernetes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.squareup.okhttp.Response;

import com.twitter.heron.api.utils.TopologyUtils;
import com.twitter.heron.scheduler.TopologyRuntimeManagementException;
import com.twitter.heron.scheduler.TopologySubmissionException;
import com.twitter.heron.scheduler.utils.Runtime;
import com.twitter.heron.scheduler.utils.SchedulerUtils;
import com.twitter.heron.scheduler.utils.SchedulerUtils.ExecutorPort;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.packing.PackingPlan;
import com.twitter.heron.spi.packing.Resource;

import io.kubernetes.client.ApiClient;
import io.kubernetes.client.ApiException;
import io.kubernetes.client.apis.AppsV1beta1Api;
import io.kubernetes.client.models.V1Container;
import io.kubernetes.client.models.V1ContainerPort;
import io.kubernetes.client.models.V1DeleteOptions;
import io.kubernetes.client.models.V1EnvVar;
import io.kubernetes.client.models.V1EnvVarSource;
import io.kubernetes.client.models.V1LabelSelector;
import io.kubernetes.client.models.V1ObjectFieldSelector;
import io.kubernetes.client.models.V1ObjectMeta;
import io.kubernetes.client.models.V1PodSpec;
import io.kubernetes.client.models.V1PodTemplateSpec;
import io.kubernetes.client.models.V1ResourceRequirements;
import io.kubernetes.client.models.V1beta1StatefulSet;
import io.kubernetes.client.models.V1beta1StatefulSetSpec;

public class AppsV1beta1Controller extends KubernetesController {

  private static final Logger LOG =
      Logger.getLogger(AppsV1beta1Controller.class.getName());

  private static final String ENV_SHARD_ID = "SHARD_ID";

  private final AppsV1beta1Api client;

  AppsV1beta1Controller(Config configuration, Config runtimeConfiguration) {
    super(configuration, runtimeConfiguration);
    final ApiClient apiClient = new ApiClient().setBasePath(getKubernetesUri());
    client = new AppsV1beta1Api(apiClient);
  }

  @Override
  boolean submit(PackingPlan packingPlan) {
    final String topologyName = getTopologyName();
    if (!topologyName.equals(topologyName.toLowerCase())) {
      throw new TopologySubmissionException("K8S scheduler does not allow upper case topologies.");
    }

    final Resource containerResource = getContainerResource(packingPlan);

    // find the max number of instances in a container so we can open
    // enough ports if remote debugging is enabled.
    int numberOfInstances = 0;
    for (PackingPlan.ContainerPlan containerPlan : packingPlan.getContainers()) {
      numberOfInstances = Math.max(numberOfInstances, containerPlan.getInstances().size());
    }
    final V1beta1StatefulSet statefulSet = createStatefulSet(containerResource, numberOfInstances);

    try {
      final Response response =
          client.createNamespacedStatefulSetCall(getNamespace(), statefulSet, null,
              null, null).execute();
      if (!response.isSuccessful()) {
        LOG.log(Level.SEVERE, "Error creating topology message: " + response.message());
        KubernetesUtils.logResponseBodyIfPresent(LOG, response);
        // construct a message based on the k8s api server response
        throw new TopologySubmissionException(
            KubernetesUtils.errorMessageFromResponse(response));
      }
    } catch (IOException | ApiException e) {
      KubernetesUtils.logExceptionWithDetails(LOG, "Error creating topology", e);
      throw new TopologySubmissionException(e.getMessage());
    }

    return true;
  }

  @Override
  boolean killTopology() {
    return
        isStatefulSet()
        ? deleteStatefulSet()
        :
        new KubernetesCompat().killTopology(getKubernetesUri(), getTopologyName(), getNamespace());
  }

  @Override
  boolean restart(int shardId) {
    final String message = "Restarting the whole topology is not supported yet. "
        + "Please kill and resubmit the topology.";
    LOG.log(Level.SEVERE, message);
    return false;
  }

  @Override
  public void addContainers(Set<PackingPlan.ContainerPlan> containersToAdd) {
    final V1beta1StatefulSet statefulSet;
    try {
      statefulSet = getStatefulSet();
    } catch (ApiException ae) {
      final String message = ae.getMessage() + "\ndetails:" + ae.getResponseBody();
      throw new TopologyRuntimeManagementException(message, ae);
    }
    final int currentContainerCount = statefulSet.getSpec().getReplicas();
    final int newContainerCount = currentContainerCount + containersToAdd.size();

    final V1beta1StatefulSetSpec newSpec = new V1beta1StatefulSetSpec();
    newSpec.setReplicas(newContainerCount);

    try {
      doPatch(newSpec);
    } catch (ApiException ae) {
      throw new TopologyRuntimeManagementException(
          ae.getMessage() + "\netails\n" + ae.getResponseBody());
    }
  }

  @Override
  public void removeContainers(Set<PackingPlan.ContainerPlan> containersToRemove) {
    final V1beta1StatefulSet statefulSet;
    try {
      statefulSet = getStatefulSet();
    } catch (ApiException ae) {
      final String message = ae.getMessage() + "\ndetails:" + ae.getResponseBody();
      throw new TopologyRuntimeManagementException(message, ae);
    }
    final int currentContainerCount = statefulSet.getSpec().getReplicas();
    final int newContainerCount = currentContainerCount - containersToRemove.size();

    final V1beta1StatefulSetSpec newSpec = new V1beta1StatefulSetSpec();
    newSpec.setReplicas(newContainerCount);

    try {
      doPatch(newSpec);
    } catch (ApiException e) {
      throw new TopologyRuntimeManagementException(
          e.getMessage() + "\netails\n" + e.getResponseBody());
    }
  }

  private void doPatch(V1beta1StatefulSetSpec patchedSpec) throws ApiException {
    client.patchNamespacedStatefulSet(getTopologyName(), getNamespace(), patchedSpec, null);
  }


  V1beta1StatefulSet getStatefulSet() throws ApiException {
    return client.readNamespacedStatefulSet(getTopologyName(), getNamespace(), null, null, null);
  }

  boolean deleteStatefulSet() {
    try {
      final V1DeleteOptions options = new V1DeleteOptions();
      options.setGracePeriodSeconds(0L);
      options.setPropagationPolicy(KubernetesConstants.DELETE_OPTIONS_PROPAGATION_POLICY);
      final Response response = client.deleteNamespacedStatefulSetCall(getTopologyName(),
          getNamespace(), options, null, null, null, null, null, null)
          .execute();

      if (!response.isSuccessful()) {
        LOG.log(Level.SEVERE, "Error killing topology message: " + response.message());
        KubernetesUtils.logResponseBodyIfPresent(LOG, response);

        throw new TopologyRuntimeManagementException(
            KubernetesUtils.errorMessageFromResponse(response));
      }
    } catch (IOException | ApiException e) {
      KubernetesUtils.logExceptionWithDetails(LOG, "Error deleting topology", e);
      return false;
    }

    return true;
  }

  boolean isStatefulSet() {
    try {
      final Response response =
          client.readNamespacedStatefulSetCall(getTopologyName(), getNamespace(),
              null, null, null, null, null)
              .execute();
      return response.isSuccessful();
    } catch (IOException | ApiException e) {
      LOG.warning("isStatefulSet check " +  e.getMessage());
    }
    return false;
  }

  protected List<String> getExecutorCommand(String containerId) {
    final Map<ExecutorPort, String> ports =
        KubernetesConstants.EXECUTOR_PORTS.entrySet()
            .stream()
            .collect(Collectors.toMap(Map.Entry::getKey,
                e -> e.getValue().toString()));

    final Config configuration = getConfiguration();
    final Config runtimeConfiguration = getRuntimeConfiguration();
    final String[] executorCommand =
        SchedulerUtils.getExecutorCommand(configuration, runtimeConfiguration,
            containerId, ports);
    return Arrays.asList(
        "sh",
        "-c",
        KubernetesUtils.getFetchCommand(configuration, runtimeConfiguration)
            + " && " + setShardIdEnvironmentVariableCommand()
            + " && " + String.join(" ", executorCommand)
    );
  }

  private static String setShardIdEnvironmentVariableCommand() {
    return String.format("%s=${POD_NAME##*-} && echo shardId=${%s}", ENV_SHARD_ID, ENV_SHARD_ID);
  }


  private V1beta1StatefulSet createStatefulSet(Resource containerResource, int numberOfInstances) {
    final String topologyName = getTopologyName();
    final Config runtimeConfiguration = getRuntimeConfiguration();

    final V1beta1StatefulSet statefulSet = new V1beta1StatefulSet();

    // setup stateful set metadata
    final V1ObjectMeta objectMeta = new V1ObjectMeta();
    objectMeta.name(topologyName);
    statefulSet.metadata(objectMeta);

    // create the stateful set spec
    final V1beta1StatefulSetSpec statefulSetSpec = new V1beta1StatefulSetSpec();
    statefulSetSpec.serviceName(topologyName);
    statefulSetSpec.setReplicas(Runtime.numContainers(runtimeConfiguration).intValue());

    // add selector match labels "app=heron" and "topology=topology-name"
    // so the we know which pods to manage
    final V1LabelSelector selector = new V1LabelSelector();
    selector.matchLabels(getMatchLabels(topologyName));
    statefulSetSpec.selector(selector);

    // create a pod template
    final V1PodTemplateSpec podTemplateSpec = new V1PodTemplateSpec();

    // set up pod meta
    final V1ObjectMeta templateMetaData = new V1ObjectMeta().labels(getLabels(topologyName));
    templateMetaData.annotations(getPrometheusAnnotations());
    podTemplateSpec.setMetadata(templateMetaData);

    final List<String> command = getExecutorCommand("$" + ENV_SHARD_ID);
    podTemplateSpec.spec(getPodSpec(command, containerResource, numberOfInstances));

    statefulSetSpec.setTemplate(podTemplateSpec);

    statefulSet.spec(statefulSetSpec);

    return statefulSet;
  }

  private Map<String, String> getPrometheusAnnotations() {
    final Map<String, String> annotations = new HashMap<>();
    annotations.put(KubernetesConstants.ANNOTATION_PROMETHEUS_SCRAPE, "true");
    annotations.put(KubernetesConstants.ANNOTATION_PROMETHEUS_PORT,
        KubernetesConstants.PROMETHEUS_PORT);

    return annotations;
  }

  private Map<String, String> getMatchLabels(String topologyName) {
    final Map<String, String> labels = new HashMap<>();
    labels.put(KubernetesConstants.LABEL_APP, KubernetesConstants.LABEL_APP_VALUE);
    labels.put(KubernetesConstants.LABEL_TOPOLOGY, topologyName);
    return labels;
  }

  private Map<String, String> getLabels(String topologyName) {
    final Map<String, String> labels = new HashMap<>();
    labels.put(KubernetesConstants.LABEL_APP, KubernetesConstants.LABEL_APP_VALUE);
    labels.put(KubernetesConstants.LABEL_TOPOLOGY, topologyName);
    return labels;
  }

  private V1PodSpec getPodSpec(List<String> executorCommand, Resource resource,
      int numberOfInstances) {
    final V1PodSpec podSpec = new V1PodSpec();
    podSpec.containers(Collections.singletonList(
        getContainer(executorCommand, resource, numberOfInstances)));
    return podSpec;
  }

  private V1Container getContainer(List<String> executorCommand, Resource resource,
      int numberOfInstances) {
    final Config configuration = getConfiguration();
    final V1Container container = new V1Container().name("executor");

    // set up the container images
    container.setImage(KubernetesContext.getExecutorDockerImage(configuration));

    // set up the container command
    container.setCommand(executorCommand);

    if (KubernetesContext.hasImagePullPolicy(configuration)) {
      container.setImagePullPolicy(KubernetesContext.getKubernetesImagePullPolicy(configuration));
    }

    // setup the environment variables for the container
    final V1EnvVar envVarHost = new V1EnvVar();
    envVarHost.name(KubernetesConstants.ENV_HOST)
        .valueFrom(new V1EnvVarSource()
            .fieldRef(new V1ObjectFieldSelector()
                .fieldPath(KubernetesConstants.POD_IP)));

    final V1EnvVar envVarPodName = new V1EnvVar();
    envVarPodName.name(KubernetesConstants.ENV_POD_NAME)
        .valueFrom(new V1EnvVarSource()
            .fieldRef(new V1ObjectFieldSelector()
                .fieldPath(KubernetesConstants.POD_NAME)));
    container.setEnv(Arrays.asList(envVarHost, envVarPodName));


    // set container resources
    final V1ResourceRequirements resourceRequirements = new V1ResourceRequirements();
    final Map<String, String> requests = new HashMap<>();
    requests.put(KubernetesConstants.MEMORY, Long.toString(resource.getRam().asMegabytes()));
    requests.put(KubernetesConstants.CPU, Double.toString(resource.getCpu()));
    resourceRequirements.setRequests(requests);
    container.setResources(resourceRequirements);

    // set container ports
    final boolean debuggingEnabled =
        TopologyUtils.getTopologyRemoteDebuggingEnabled(
            Runtime.topology(getRuntimeConfiguration()));
    container.setPorts(getContainerPorts(debuggingEnabled, numberOfInstances));

    return container;
  }

  private List<V1ContainerPort> getContainerPorts(boolean remoteDebugEnabled,
      int numberOfInstances) {
    List<V1ContainerPort> ports = new ArrayList<>();
    KubernetesConstants.EXECUTOR_PORTS.forEach((p, v) -> {
      final V1ContainerPort port = new V1ContainerPort();
      port.setName(p.getName());
      port.setContainerPort(v);
      ports.add(port);
    });


    if (remoteDebugEnabled) {
      IntStream.range(0, numberOfInstances).forEach(i -> {
        final String portName =
            KubernetesConstants.JVM_REMOTE_DEBUGGER_PORT_NAME + "-" + String.valueOf(i);
        final V1ContainerPort port = new V1ContainerPort();
        port.setName(portName);
        port.setContainerPort(KubernetesConstants.JVM_REMOTE_DEBUGGER_PORT + i);
        ports.add(port);
      });
    }

    return ports;
  }
}
