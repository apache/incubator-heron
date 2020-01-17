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

package org.apache.heron.scheduler.kubernetes;

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

import org.apache.heron.api.utils.TopologyUtils;
import org.apache.heron.scheduler.TopologyRuntimeManagementException;
import org.apache.heron.scheduler.TopologySubmissionException;
import org.apache.heron.scheduler.utils.Runtime;
import org.apache.heron.scheduler.utils.SchedulerUtils;
import org.apache.heron.scheduler.utils.SchedulerUtils.ExecutorPort;
import org.apache.heron.spi.common.Config;
import org.apache.heron.spi.packing.PackingPlan;
import org.apache.heron.spi.packing.Resource;

import io.kubernetes.client.custom.Quantity;
import io.kubernetes.client.custom.V1Patch;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.Configuration;
import io.kubernetes.client.openapi.apis.AppsV1Api;
import io.kubernetes.client.openapi.models.V1Container;
import io.kubernetes.client.openapi.models.V1ContainerPort;
import io.kubernetes.client.openapi.models.V1EnvVar;
import io.kubernetes.client.openapi.models.V1EnvVarSource;
import io.kubernetes.client.openapi.models.V1LabelSelector;
import io.kubernetes.client.openapi.models.V1ObjectFieldSelector;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1PodSpec;
import io.kubernetes.client.openapi.models.V1PodTemplateSpec;
import io.kubernetes.client.openapi.models.V1ResourceRequirements;
import io.kubernetes.client.openapi.models.V1StatefulSet;
import io.kubernetes.client.openapi.models.V1StatefulSetSpec;
import io.kubernetes.client.openapi.models.V1Toleration;
import io.kubernetes.client.openapi.models.V1Volume;
import io.kubernetes.client.openapi.models.V1VolumeMount;

import okhttp3.Response;

public class AppsV1Controller extends KubernetesController {

  private static final Logger LOG =
      Logger.getLogger(AppsV1Controller.class.getName());

  private static final String ENV_SHARD_ID = "SHARD_ID";

  private final AppsV1Api appsClient;

  AppsV1Controller(Config configuration, Config runtimeConfiguration) {
    super(configuration, runtimeConfiguration);
    try {
      final ApiClient apiClient = io.kubernetes.client.util.Config.defaultClient();
      Configuration.setDefaultApiClient(apiClient);
      appsClient = new AppsV1Api(apiClient);
    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Failed to setup Kubernetes client" + e);
      throw new RuntimeException(e);
    }
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
    final V1StatefulSet statefulSet = createStatefulSet(containerResource, numberOfInstances);

    try {
      final V1StatefulSet response =
          appsClient.createNamespacedStatefulSet(getNamespace(), statefulSet, null,
              null, null);
    } catch (ApiException e) {
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
  public Set<PackingPlan.ContainerPlan>
      addContainers(Set<PackingPlan.ContainerPlan> containersToAdd) {
    final V1StatefulSet statefulSet;
    try {
      statefulSet = getStatefulSet();
    } catch (ApiException ae) {
      final String message = ae.getMessage() + "\ndetails:" + ae.getResponseBody();
      throw new TopologyRuntimeManagementException(message, ae);
    }
    final int currentContainerCount = statefulSet.getSpec().getReplicas();
    final int newContainerCount = currentContainerCount + containersToAdd.size();

    final V1StatefulSetSpec newSpec = new V1StatefulSetSpec();
    newSpec.setReplicas(newContainerCount);

    try {
      doPatch(newSpec);
    } catch (ApiException ae) {
      throw new TopologyRuntimeManagementException(
          ae.getMessage() + "\ndetails\n" + ae.getResponseBody());
    }

    return containersToAdd;
  }

  @Override
  public void removeContainers(Set<PackingPlan.ContainerPlan> containersToRemove) {
    final V1StatefulSet statefulSet;
    try {
      statefulSet = getStatefulSet();
    } catch (ApiException ae) {
      final String message = ae.getMessage() + "\ndetails:" + ae.getResponseBody();
      throw new TopologyRuntimeManagementException(message, ae);
    }
    final int currentContainerCount = statefulSet.getSpec().getReplicas();
    final int newContainerCount = currentContainerCount - containersToRemove.size();

    final V1StatefulSetSpec newSpec = new V1StatefulSetSpec();
    newSpec.setReplicas(newContainerCount);

    try {
      doPatch(newSpec);
    } catch (ApiException e) {
      throw new TopologyRuntimeManagementException(
          e.getMessage() + "\ndetails\n" + e.getResponseBody());
    }
  }

  private void doPatch(V1StatefulSetSpec patchedSpec) throws ApiException {
    final String body =
            String.format(JSON_PATCH_STATEFUL_SET_REPLICAS_FORMAT,
                    patchedSpec.getReplicas().toString());
    final V1Patch patch = new V1Patch(body);
    appsClient.patchNamespacedStatefulSet(getTopologyName(),
            getNamespace(), patch, null, null, null, null);
  }

  private static final String JSON_PATCH_STATEFUL_SET_REPLICAS_FORMAT =
      "{\"op\":\"replace\",\"path\":\"/spec/replicas\",\"value\":%s}";

  V1StatefulSet getStatefulSet() throws ApiException {
    return appsClient.readNamespacedStatefulSet(getTopologyName(), getNamespace(),
        null, null, null);
  }

  boolean deleteStatefulSet() {
    try {
      final Response response = appsClient.deleteNamespacedStatefulSetCall(getTopologyName(),
          getNamespace(), null, null, 0, null,
          KubernetesConstants.DELETE_OPTIONS_PROPAGATION_POLICY, null, null).execute();

      if (response.isSuccessful()) {
        LOG.log(Level.INFO, "StatefulSet for the Job [" + getTopologyName()
            + "] in namespace [" + getNamespace() + "] is deleted.");
        return true;
      } else {
        LOG.log(Level.SEVERE, "Error when deleting the StatefulSet of the job ["
            + getTopologyName() + "]: in namespace [" + getNamespace() + "]");
        LOG.log(Level.SEVERE, "Error killing topology message: " + response.message());
        KubernetesUtils.logResponseBodyIfPresent(LOG, response);

        throw new TopologyRuntimeManagementException(
            KubernetesUtils.errorMessageFromResponse(response));
      }
    } catch (IOException | ApiException e) {
      KubernetesUtils.logExceptionWithDetails(LOG, "Error deleting topology", e);
      return false;
    }
  }

  boolean isStatefulSet() {
    try {
      final V1StatefulSet response =
          appsClient.readNamespacedStatefulSet(getTopologyName(), getNamespace(),
              null, null, null);
      return response.getKind().equals("StatefulSet");
    } catch (ApiException e) {
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
        KubernetesUtils.getConfCommand(configuration)
            + " && " + KubernetesUtils.getFetchCommand(configuration, runtimeConfiguration)
            + " && " + setShardIdEnvironmentVariableCommand()
            + " && " + String.join(" ", executorCommand)
    );
  }

  private static String setShardIdEnvironmentVariableCommand() {
    return String.format("%s=${POD_NAME##*-} && echo shardId=${%s}", ENV_SHARD_ID, ENV_SHARD_ID);
  }


  private V1StatefulSet createStatefulSet(Resource containerResource, int numberOfInstances) {
    final String topologyName = getTopologyName();
    final Config runtimeConfiguration = getRuntimeConfiguration();

    final V1StatefulSet statefulSet = new V1StatefulSet();

    // setup stateful set metadata
    final V1ObjectMeta objectMeta = new V1ObjectMeta();
    objectMeta.name(topologyName);
    statefulSet.metadata(objectMeta);

    // create the stateful set spec
    final V1StatefulSetSpec statefulSetSpec = new V1StatefulSetSpec();
    statefulSetSpec.serviceName(topologyName);
    statefulSetSpec.setReplicas(Runtime.numContainers(runtimeConfiguration).intValue());

    // Parallel pod management tells the StatefulSet controller to launch or terminate
    // all Pods in parallel, and not to wait for Pods to become Running and Ready or completely
    // terminated prior to launching or terminating another Pod.
    statefulSetSpec.setPodManagementPolicy("Parallel");

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

    // set the termination period to 0 so pods can be deleted quickly
    podSpec.setTerminationGracePeriodSeconds(0L);

    // set the pod tolerations so pods are rescheduled when nodes go down
    // https://kubernetes.io/docs/concepts/configuration/taint-and-toleration/#taint-based-evictions
    podSpec.setTolerations(getTolerations());

    podSpec.containers(Collections.singletonList(
        getContainer(executorCommand, resource, numberOfInstances)));

    addVolumesIfPresent(podSpec);

    return podSpec;
  }

  private List<V1Toleration> getTolerations() {
    final List<V1Toleration> tolerations = new ArrayList<>();
    KubernetesConstants.TOLERATIONS.forEach(t -> {
      final V1Toleration toleration =
          new V1Toleration()
              .key(t)
              .operator("Exists")
              .effect("NoExecute")
              .tolerationSeconds(10L);
      tolerations.add(toleration);
    });

    return tolerations;
  }

  private void addVolumesIfPresent(V1PodSpec spec) {
    final Config config = getConfiguration();
    if (KubernetesContext.hasVolume(config)) {
      final V1Volume volume = Volumes.get().create(config);
      if (volume != null) {
        LOG.fine("Adding volume: " + volume.toString());
        spec.volumes(Collections.singletonList(volume));
      }
    }
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
    final Map<String, Quantity> requests = new HashMap<>();
    requests.put(KubernetesConstants.MEMORY,
        Quantity.fromString(KubernetesUtils.Megabytes(resource.getRam())));
    requests.put(KubernetesConstants.CPU,
         Quantity.fromString(Double.toString(roundDecimal(resource.getCpu(), 3))));
    resourceRequirements.setRequests(requests);
    container.setResources(resourceRequirements);

    // set container ports
    final boolean debuggingEnabled =
        TopologyUtils.getTopologyRemoteDebuggingEnabled(
            Runtime.topology(getRuntimeConfiguration()));
    container.setPorts(getContainerPorts(debuggingEnabled, numberOfInstances));

    // setup volume mounts
    mountVolumeIfPresent(container);

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

  private void mountVolumeIfPresent(V1Container container) {
    final Config config = getConfiguration();
    if (KubernetesContext.hasContainerVolume(config)) {
      final V1VolumeMount mount =
          new V1VolumeMount()
              .name(KubernetesContext.getContainerVolumeName(config))
              .mountPath(KubernetesContext.getContainerVolumeMountPath(config));
      container.volumeMounts(Collections.singletonList(mount));
    }
  }

  public static double roundDecimal(double value, int places) {
    double scale = Math.pow(10, places);
    return Math.round(value * scale) / scale;
  }
}
