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
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.google.common.annotations.VisibleForTesting;

import org.apache.heron.api.utils.TopologyUtils;
import org.apache.heron.common.basics.ByteAmount;
import org.apache.heron.common.basics.Pair;
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
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1ConfigMap;
import io.kubernetes.client.openapi.models.V1Container;
import io.kubernetes.client.openapi.models.V1ContainerPort;
import io.kubernetes.client.openapi.models.V1EnvVar;
import io.kubernetes.client.openapi.models.V1EnvVarSource;
import io.kubernetes.client.openapi.models.V1LabelSelector;
import io.kubernetes.client.openapi.models.V1ObjectFieldSelector;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1PersistentVolumeClaim;
import io.kubernetes.client.openapi.models.V1PersistentVolumeClaimBuilder;
import io.kubernetes.client.openapi.models.V1PodSpec;
import io.kubernetes.client.openapi.models.V1PodTemplate;
import io.kubernetes.client.openapi.models.V1PodTemplateSpec;
import io.kubernetes.client.openapi.models.V1ResourceRequirements;
import io.kubernetes.client.openapi.models.V1SecretKeySelector;
import io.kubernetes.client.openapi.models.V1SecretVolumeSourceBuilder;
import io.kubernetes.client.openapi.models.V1Service;
import io.kubernetes.client.openapi.models.V1ServiceSpec;
import io.kubernetes.client.openapi.models.V1StatefulSet;
import io.kubernetes.client.openapi.models.V1StatefulSetBuilder;
import io.kubernetes.client.openapi.models.V1StatefulSetSpec;
import io.kubernetes.client.openapi.models.V1Status;
import io.kubernetes.client.openapi.models.V1Toleration;
import io.kubernetes.client.openapi.models.V1Volume;
import io.kubernetes.client.openapi.models.V1VolumeBuilder;
import io.kubernetes.client.openapi.models.V1VolumeMount;
import io.kubernetes.client.openapi.models.V1VolumeMountBuilder;
import io.kubernetes.client.util.PatchUtils;
import io.kubernetes.client.util.Yaml;
import okhttp3.Response;

import static java.net.HttpURLConnection.HTTP_NOT_FOUND;

public class V1Controller extends KubernetesController {

  private static final Logger LOG =
      Logger.getLogger(V1Controller.class.getName());

  private static final String ENV_SHARD_ID = "SHARD_ID";

  private final boolean isPodTemplateDisabled;

  private final AppsV1Api appsClient;
  private final CoreV1Api coreClient;

  private Map<String, Map<KubernetesConstants.VolumeClaimTemplateConfigKeys, String>>
      persistentVolumeClaimConfigs = null;

  V1Controller(Config configuration, Config runtimeConfiguration) {
    super(configuration, runtimeConfiguration);

    isPodTemplateDisabled = KubernetesContext.getPodTemplateConfigMapDisabled(configuration);
    LOG.log(Level.WARNING, String.format("Custom Pod Templates are %s",
        isPodTemplateDisabled ? "DISABLED" : "ENABLED"));

    try {
      final ApiClient apiClient = io.kubernetes.client.util.Config.defaultClient();
      Configuration.setDefaultApiClient(apiClient);
      appsClient = new AppsV1Api(apiClient);
      coreClient = new CoreV1Api(apiClient);
    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Failed to setup Kubernetes client" + e);
      throw new RuntimeException(e);
    }
  }

  @Override
  boolean submit(PackingPlan packingPlan) {
    final String topologyName = getTopologyName();
    if (!topologyName.equals(topologyName.toLowerCase())) {
      throw new TopologySubmissionException("K8S scheduler does not allow upper case topology's.");
    }

    final Resource containerResource = getContainerResource(packingPlan);

    final V1Service topologyService = createTopologyService();
    try {
      coreClient.createNamespacedService(getNamespace(), topologyService, null,
              null, null);
    } catch (ApiException e) {
      KubernetesUtils.logExceptionWithDetails(LOG, "Error creating topology service", e);
      throw new TopologySubmissionException(e.getMessage());
    }

    // Get and then create Persistent Volume Claims from the CLI.
    // TODO: Get PVC for the Manager and Executors.
    persistentVolumeClaimConfigs =
        KubernetesContext.getVolumeClaimTemplates(getConfiguration());
    if (KubernetesContext.getPersistentVolumeClaimDisabled(getConfiguration())
        && !persistentVolumeClaimConfigs.isEmpty()) {
      final String message =
          String.format("Configuring Persistent Volume Claim from CLI is disabled: '%s'",
              topologyName);
      LOG.log(Level.WARNING, message);
      throw new TopologySubmissionException(message);
    }

    // Find the max number of instances in a container so that we can open
    // enough ports if remote debugging is enabled.
    int numberOfInstances = 0;
    for (PackingPlan.ContainerPlan containerPlan : packingPlan.getContainers()) {
      numberOfInstances = Math.max(numberOfInstances, containerPlan.getInstances().size());
    }
    final V1StatefulSet executors = createStatefulSet(containerResource, numberOfInstances, true);
    final V1StatefulSet manager = createStatefulSetManager(executors, numberOfInstances);

    try {
      appsClient.createNamespacedStatefulSet(getNamespace(), executors, null,
              null, null);
      appsClient.createNamespacedStatefulSet(getNamespace(), manager, null,
              null, null);
    } catch (ApiException e) {
      final String message = String.format("Error creating topology: %s%n", e.getResponseBody());
      KubernetesUtils.logExceptionWithDetails(LOG, message, e);
      throw new TopologySubmissionException(message);
    }

    return true;
  }

  @Override
  boolean killTopology() {
    removePersistentVolumeClaims();
    deleteStatefulSets();
    deleteService();
    return true;
  }

  /**
   * Restarts a topology by deleting the Pods associated with it using <code>Selector Labels</code>.
   * @param shardId Not used but required because of interface.
   * @return Indicator of successful submission of restart request to Kubernetes cluster.
   */
  @Override
  boolean restart(int shardId) {
    try {
      coreClient.deleteCollectionNamespacedPod(getNamespace(), null, null, null, null,
          0, createTopologySelectorLabels(), null, null, null, null,
          null, null, null);
      LOG.log(Level.WARNING, String.format("Restarting topology '%s'...", getTopologyName()));
    } catch (ApiException e) {
      LOG.log(Level.SEVERE, String.format("Failed to restart topology '%s'...", getTopologyName()));
      return false;
    }
    return true;
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
    final V1StatefulSetSpec v1StatefulSet = Objects.requireNonNull(statefulSet.getSpec());
    final int currentContainerCount = Objects.requireNonNull(v1StatefulSet.getReplicas());
    final int newContainerCount = currentContainerCount + containersToAdd.size();

    try {
      patchStatefulSetReplicas(newContainerCount);
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

    final V1StatefulSetSpec v1StatefulSet = Objects.requireNonNull(statefulSet.getSpec());
    final int currentContainerCount = Objects.requireNonNull(v1StatefulSet.getReplicas());
    final int newContainerCount = currentContainerCount - containersToRemove.size();

    try {
      patchStatefulSetReplicas(newContainerCount);
    } catch (ApiException e) {
      throw new TopologyRuntimeManagementException(
          e.getMessage() + "\ndetails\n" + e.getResponseBody());
    }
  }

  private void patchStatefulSetReplicas(int replicas) throws ApiException {
    final String body =
            String.format(JSON_PATCH_STATEFUL_SET_REPLICAS_FORMAT,
                    replicas);
    final V1Patch patch = new V1Patch(body);

    PatchUtils.patch(V1StatefulSet.class,
        () ->
          appsClient.patchNamespacedStatefulSetCall(
            getStatefulSetName(true),
            getNamespace(),
            patch,
            null,
            null,
            null,
            null,
            null),
        V1Patch.PATCH_FORMAT_JSON_PATCH,
        appsClient.getApiClient());
  }

  private static final String JSON_PATCH_STATEFUL_SET_REPLICAS_FORMAT =
          "[{\"op\":\"replace\",\"path\":\"/spec/replicas\",\"value\":%d}]";

  V1StatefulSet getStatefulSet() throws ApiException {
    return appsClient.readNamespacedStatefulSet(getStatefulSetName(true), getNamespace(),
        null, null, null);
  }

  void deleteService() {
    try (Response response = coreClient.deleteNamespacedServiceCall(getTopologyName(),
          getNamespace(), null, null, 0, null,
          KubernetesConstants.DELETE_OPTIONS_PROPAGATION_POLICY, null, null).execute()) {

      if (!response.isSuccessful()) {
        if (response.code() == HTTP_NOT_FOUND) {
          LOG.log(Level.WARNING, "Deleting non-existent Kubernetes headless service for Topology: "
                  + getTopologyName());
          return;
        }
        LOG.log(Level.SEVERE, "Error when deleting the Service of the job ["
                + getTopologyName() + "] in namespace [" + getNamespace() + "]");
        LOG.log(Level.SEVERE, "Error killing topology message:" + response.message());
        KubernetesUtils.logResponseBodyIfPresent(LOG, response);

        throw new TopologyRuntimeManagementException(
                KubernetesUtils.errorMessageFromResponse(response));
      }
    } catch (ApiException e) {
      if (e.getCode() == HTTP_NOT_FOUND) {
        LOG.log(Level.WARNING, "Tried to delete a non-existent Kubernetes service for Topology: "
                + getTopologyName());
        return;
      }
      throw new TopologyRuntimeManagementException("Error deleting topology ["
              + getTopologyName() + "] Kubernetes service", e);
    } catch (IOException e) {
      throw new TopologyRuntimeManagementException("Error deleting topology ["
              + getTopologyName() + "] Kubernetes service", e);
    }
    LOG.log(Level.INFO, "Headless Service for the Job [" + getTopologyName()
            + "] in namespace [" + getNamespace() + "] is deleted.");
  }

  void deleteStatefulSets() {
    try (Response response = appsClient.deleteCollectionNamespacedStatefulSetCall(getNamespace(),
        null, null, null, null, null,
        createTopologySelectorLabels(), null, null, null, null, null, null, null, null)
        .execute()) {

      if (!response.isSuccessful()) {
        if (response.code() == HTTP_NOT_FOUND) {
          LOG.log(Level.WARNING, "Tried to delete a non-existent StatefulSets for Topology: "
                  + getTopologyName());
          return;
        }
        LOG.log(Level.SEVERE, "Error when deleting the StatefulSets of the job ["
                + getTopologyName() + "] in namespace [" + getNamespace() + "]");
        LOG.log(Level.SEVERE, "Error killing topology message: " + response.message());
        KubernetesUtils.logResponseBodyIfPresent(LOG, response);

        throw new TopologyRuntimeManagementException(
                KubernetesUtils.errorMessageFromResponse(response));
      }
    } catch (ApiException e) {
      if (e.getCode() == HTTP_NOT_FOUND) {
        LOG.log(Level.WARNING, "Tried to delete a non-existent StatefulSet for Topology: "
                + getTopologyName());
        return;
      }
      throw new TopologyRuntimeManagementException("Error deleting topology ["
              + getTopologyName() + "] Kubernetes StatefulSets", e);
    } catch (IOException e) {
      throw new TopologyRuntimeManagementException("Error deleting topology ["
              + getTopologyName() + "] Kubernetes StatefulSets", e);
    }
    LOG.log(Level.INFO, "StatefulSet for the Job [" + getTopologyName()
            + "] in namespace [" + getNamespace() + "] is deleted.");
  }

  /**
   * Generates the command to start Heron within the <code>container</code>.
   * @param containerId Passed down to <>SchedulerUtils</> to generate executor command.
   * @param numOfInstances Used to configure the debugging ports.
   * @param isExecutor Flag used to generate the correct <code>shard_id</code>.
   * @return The complete command to start Heron in a <code>container</code>.
   */
  protected List<String> getExecutorCommand(String containerId, int numOfInstances,
                                            boolean isExecutor) {
    final Config configuration = getConfiguration();
    final Config runtimeConfiguration = getRuntimeConfiguration();
    final Map<ExecutorPort, String> ports =
        KubernetesConstants.EXECUTOR_PORTS.entrySet()
            .stream()
            .collect(Collectors.toMap(Map.Entry::getKey,
                e -> e.getValue().toString()));

    if (TopologyUtils.getTopologyRemoteDebuggingEnabled(Runtime.topology(runtimeConfiguration))
            && numOfInstances != 0) {
      List<String> remoteDebuggingPorts = new LinkedList<>();
      IntStream.range(0, numOfInstances).forEach(i -> {
        int port = KubernetesConstants.JVM_REMOTE_DEBUGGER_PORT + i;
        remoteDebuggingPorts.add(String.valueOf(port));
      });
      ports.put(ExecutorPort.JVM_REMOTE_DEBUGGER_PORTS,
              String.join(",", remoteDebuggingPorts));
    }

    final String[] executorCommand =
        SchedulerUtils.getExecutorCommand(configuration, runtimeConfiguration,
            containerId, ports);
    return Arrays.asList(
        "sh",
        "-c",
        KubernetesUtils.getConfCommand(configuration)
            + " && " + KubernetesUtils.getFetchCommand(configuration, runtimeConfiguration)
            + " && " + setShardIdEnvironmentVariableCommand(isExecutor)
            + " && " + String.join(" ", executorCommand)
    );
  }

  /**
   * Configures the <code>shard_id</code> for the Heron container based on whether it is an <code>executor</code>
   * or <code>manager</code>. <code>executor</code> IDs are [1 - n) and the <code>manager</code> IDs start at 0.
   * @param isExecutor Switch flag to generate correct command.
   * @return The command required to put the Heron instance in <code>executor</code> or <code>manager</code> mode.
   */
  @VisibleForTesting
  protected static String setShardIdEnvironmentVariableCommand(boolean isExecutor) {
    final String pattern = String.format("%%s=%s && echo shardId=${%%s}",
        isExecutor ? "$((${POD_NAME##*-} + 1))" : "${POD_NAME##*-}");
    return String.format(pattern, ENV_SHARD_ID, ENV_SHARD_ID);
  }

  private V1Service createTopologyService() {
    final String topologyName = getTopologyName();

    final V1Service service = new V1Service();

    // Setup service metadata.
    final V1ObjectMeta objectMeta = new V1ObjectMeta()
        .name(topologyName)
        .annotations(getServiceAnnotations())
        .labels(getServiceLabels());
    service.setMetadata(objectMeta);

    // Create the headless service.
    final V1ServiceSpec serviceSpec = new V1ServiceSpec()
        .clusterIP("None")
        .selector(getPodMatchLabels(topologyName));

    service.setSpec(serviceSpec);

    return service;
  }

  /**
   * Creates and configures the <code>StatefulSet</code> which the topology's <code>manager</code>s will run in.
   * @param executor The <code>executor</code> to be used as the foundation for the <code>manager</code>.
   * @param numberOfInstances Used to configure the execution command and ports for the <code>manager</code>.
   * @return A fully configured <code>StatefulSet</code> for the topology's <code>manager</code>.
   */
  @VisibleForTesting
  protected V1StatefulSet createStatefulSetManager(V1StatefulSet executor, int numberOfInstances) {
    // Duplicate the executor.
    final V1StatefulSet manager = new V1StatefulSetBuilder()
        .withNewMetadataLike(executor.getMetadata())
        .endMetadata()
        .withNewSpecLike(executor.getSpec())
        .endSpec()
        .build();

    // Set replica count and name in StatefulSet Spec for single manager.
    manager.getSpec().setReplicas(1);
    manager.getMetadata()
        .setName(String.format("%s-%s", getTopologyName(), KubernetesConstants.MANAGER_NAME));

    // Locate executor container. Executor container will always exist and will never be null.
    final V1Container managerContainer =
        manager.getSpec().getTemplate().getSpec().getContainers().stream()
            .filter(container ->
                KubernetesConstants.EXECUTOR_NAME.equalsIgnoreCase(container.getName()))
            .findFirst()
            .orElse(null);

    // Update command and name of manager container.
    final List<String> command = getExecutorCommand("$" + ENV_SHARD_ID, numberOfInstances, false);
    managerContainer.setCommand(command);
    managerContainer.setName(KubernetesConstants.MANAGER_NAME);

    // Configure Limits.
    final Map<String, String> configLimits = KubernetesContext.getManagerLimits(getConfiguration());
    if (!configLimits.isEmpty()) {
      managerContainer.getResources().setLimits(createResourcesRequirement(configLimits));
    }

    // Configure Requests. Set Requests=Limits if no Requests are provided but Limits are.
    final Map<String, String> configRequests =
        KubernetesContext.getManagerRequests(getConfiguration());
    if (!configRequests.isEmpty()) {
      managerContainer.getResources().setRequests(createResourcesRequirement(configRequests));
    } else if (!configLimits.isEmpty()) {
      managerContainer.getResources().setRequests(createResourcesRequirement(configLimits));
    }

    return manager;
  }

  /**
   * Creates and configures the <code>StatefulSet</code> which the topology's <code>executor</code>s will run in.
   * @param containerResource Passed down to configure the <code>executor</code> resource limits.
   * @param numberOfInstances Used to configure the execution command and ports for the <code>executor</code>.
   * @param isExecutor Flag used to configure components specific to <code>executor</code> and <code>manager</code>.
   * @return A fully configured <code>StatefulSet</code> for the topology's <code>executors</code>.
   */
  private V1StatefulSet createStatefulSet(Resource containerResource, int numberOfInstances,
                                          boolean isExecutor) {
    final String topologyName = getTopologyName();
    final Config runtimeConfiguration = getRuntimeConfiguration();

    final V1StatefulSet statefulSet = new V1StatefulSet();

    // Setup StatefulSet's metadata.
    final V1ObjectMeta objectMeta = new V1ObjectMeta()
        .name(getStatefulSetName(isExecutor))
        .labels(getPodLabels(topologyName));
    statefulSet.setMetadata(objectMeta);

    // Create the StatefulSet Spec.
    // Reduce replica count by one for Executors and set to one for Manager.
    final int replicasCount =
        isExecutor ? Runtime.numContainers(runtimeConfiguration).intValue() - 1 : 1;
    final V1StatefulSetSpec statefulSetSpec = new V1StatefulSetSpec()
        .serviceName(topologyName)
        .replicas(replicasCount);

    // Parallel pod management tells the StatefulSet controller to launch or terminate
    // all Pods in parallel, and not to wait for Pods to become Running and Ready or completely
    // terminated prior to launching or terminating another Pod.
    statefulSetSpec.setPodManagementPolicy("Parallel");

    // Add selector match labels "app=heron" and "topology=topology-name"
    // so we know which pods to manage.
    final V1LabelSelector selector = new V1LabelSelector()
        .matchLabels(getPodMatchLabels(topologyName));
    statefulSetSpec.setSelector(selector);

    // Create a Pod Template.
    final V1PodTemplateSpec podTemplateSpec = loadPodFromTemplate(isExecutor);

    // Set up Pod Metadata.
    final V1ObjectMeta templateMetaData = new V1ObjectMeta().labels(getPodLabels(topologyName));
    Map<String, String> annotations = new HashMap<>();
    annotations.putAll(getPodAnnotations());
    annotations.putAll(getPrometheusAnnotations());
    templateMetaData.setAnnotations(annotations);
    podTemplateSpec.setMetadata(templateMetaData);

    configurePodSpec(podTemplateSpec, containerResource, numberOfInstances, isExecutor);

    statefulSetSpec.setTemplate(podTemplateSpec);

    statefulSet.setSpec(statefulSetSpec);

    // TODO: Configure PVCs for Executors and Manager separately.
    statefulSetSpec.setVolumeClaimTemplates(
        createPersistentVolumeClaims(persistentVolumeClaimConfigs));

    return statefulSet;
  }

  private Map<String, String> getPodAnnotations() {
    Config config = getConfiguration();
    return KubernetesContext.getPodAnnotations(config);
  }

  private Map<String, String> getServiceAnnotations() {
    Config config = getConfiguration();
    return KubernetesContext.getServiceAnnotations(config);
  }

  private Map<String, String> getPrometheusAnnotations() {
    final Map<String, String> annotations = new HashMap<>();
    annotations.put(KubernetesConstants.ANNOTATION_PROMETHEUS_SCRAPE, "true");
    annotations.put(KubernetesConstants.ANNOTATION_PROMETHEUS_PORT,
        KubernetesConstants.PROMETHEUS_PORT);

    return annotations;
  }

  private Map<String, String> getPodMatchLabels(String topologyName) {
    final Map<String, String> labels = new HashMap<>();
    labels.put(KubernetesConstants.LABEL_APP, KubernetesConstants.LABEL_APP_VALUE);
    labels.put(KubernetesConstants.LABEL_TOPOLOGY, topologyName);
    return labels;
  }

  private Map<String, String> getPodLabels(String topologyName) {
    final Map<String, String> labels = new HashMap<>();
    labels.put(KubernetesConstants.LABEL_APP, KubernetesConstants.LABEL_APP_VALUE);
    labels.put(KubernetesConstants.LABEL_TOPOLOGY, topologyName);
    labels.putAll(KubernetesContext.getPodLabels(getConfiguration()));
    return labels;
  }

  private Map<String, String> getServiceLabels() {
    return KubernetesContext.getServiceLabels(getConfiguration());
  }

  /**
   * Configures the <code>Pod Spec</code> section of the <code>StatefulSet</code>. The <code>Heron</code> container
   * will be configured to allow it to function but other supplied containers are loaded verbatim.
   * @param podTemplateSpec The <code>Pod Template Spec</code> section to update.
   * @param resource Passed down to configure the resource limits.
   * @param numberOfInstances Passed down to configure the ports.
   * @param isExecutor Flag used to configure components specific to <code>Executor</code> and <code>Manager</code>.
   */
  private void configurePodSpec(final V1PodTemplateSpec podTemplateSpec, Resource resource,
                                int numberOfInstances, boolean isExecutor) {
    if (podTemplateSpec.getSpec() == null) {
      podTemplateSpec.setSpec(new V1PodSpec());
    }
    final V1PodSpec podSpec = podTemplateSpec.getSpec();

    // Set the termination period to 0 so pods can be deleted quickly
    podSpec.setTerminationGracePeriodSeconds(0L);

    // Set the pod tolerations so pods are rescheduled when nodes go down
    // https://kubernetes.io/docs/concepts/configuration/taint-and-toleration/#taint-based-evictions
    configureTolerations(podSpec);

    // Get <Heron> container and ignore all others.
    final String containerName =
        isExecutor ? KubernetesConstants.EXECUTOR_NAME : KubernetesConstants.MANAGER_NAME;
    V1Container heronContainer = null;
    List<V1Container> containers = podSpec.getContainers();
    if (containers != null) {
      for (V1Container container : containers) {
        final String name = container.getName();
        if (name != null && name.equals(containerName)) {
          if (heronContainer != null) {
            throw new TopologySubmissionException(
                String.format("Multiple configurations found for '%s' container", containerName));
          }
          heronContainer = container;
        }
      }
    } else {
      containers = new LinkedList<>();
    }

    if (heronContainer == null) {
      heronContainer = new V1Container().name(containerName);
      containers.add(heronContainer);
    }

    if (!persistentVolumeClaimConfigs.isEmpty()) {
      configurePodWithPersistentVolumeClaimVolumesAndMounts(podSpec, heronContainer);
    }

    configureHeronContainer(resource, numberOfInstances, heronContainer, isExecutor);

    podSpec.setContainers(containers);

    addVolumesIfPresent(podSpec);

    mountSecretsAsVolumes(podSpec);
  }

  /**
   * Adds <code>tolerations</code> to the <code>Pod Spec</code> with Heron's values taking precedence.
   * @param spec <code>Pod Spec</code> to be configured.
   */
  @VisibleForTesting
  protected void configureTolerations(final V1PodSpec spec) {
    KubernetesUtils.V1ControllerUtils<V1Toleration> utils =
        new KubernetesUtils.V1ControllerUtils<>();
    spec.setTolerations(
        utils.mergeListsDedupe(getTolerations(), spec.getTolerations(),
            Comparator.comparing(V1Toleration::getKey), "Pod Specification Tolerations")
    );
  }

  /**
   * Generates a list of <code>tolerations</code> which Heron requires.
   * @return A list of configured <code>tolerations</code>.
   */
  @VisibleForTesting
  protected static List<V1Toleration> getTolerations() {
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

  /**
   * Adds volume to the <code>Pod Spec</code> that Heron requires. Heron's values taking precedence.
   * @param spec <code>Pod Spec</code> to be configured.
   */
  @VisibleForTesting
  protected void addVolumesIfPresent(final V1PodSpec spec) {
    final Config config = getConfiguration();
    if (KubernetesContext.hasVolume(config)) {
      final V1Volume volumeFromConfig = Volumes.get().create(config);
      if (volumeFromConfig != null) {
        // Merge volumes. Deduplicate using volume's name with Heron defaults taking precedence.
        KubernetesUtils.V1ControllerUtils<V1Volume> utils =
            new KubernetesUtils.V1ControllerUtils<>();
        spec.setVolumes(
            utils.mergeListsDedupe(Collections.singletonList(volumeFromConfig), spec.getVolumes(),
                Comparator.comparing(V1Volume::getName), "Pod Template Volumes")
        );
        LOG.fine("Adding volume: " + volumeFromConfig);
      }
    }
  }

  private void mountSecretsAsVolumes(V1PodSpec podSpec) {
    final Config config = getConfiguration();
    final Map<String, String> secrets = KubernetesContext.getPodSecretsToMount(config);
    for (Map.Entry<String, String> secret : secrets.entrySet()) {
      final V1VolumeMount mount = new V1VolumeMount()
              .name(secret.getKey())
              .mountPath(secret.getValue());
      final V1Volume secretVolume = new V1Volume()
              .name(secret.getKey())
              .secret(new V1SecretVolumeSourceBuilder()
                      .withSecretName(secret.getKey())
                      .build());
      podSpec.addVolumesItem(secretVolume);
      for (V1Container container : podSpec.getContainers()) {
        container.addVolumeMountsItem(mount);
      }
    }
  }

  /**
   * Configures the <code>Heron</code> container with values for parameters Heron requires for functioning.
   * @param resource Resource limits.
   * @param numberOfInstances Required number of <code>executor</code> containers which is used to configure ports.
   * @param container The <code>executor</code> container to be configured.
   * @param isExecutor Flag indicating whether to set a <code>executor</code> or <code>manager</code> command.
   */
  private void configureHeronContainer(Resource resource, int numberOfInstances,
                                       final V1Container container, boolean isExecutor) {
    final Config configuration = getConfiguration();

    // Set up the container images.
    container.setImage(KubernetesContext.getExecutorDockerImage(configuration));

    // Set up the container command.
    final List<String> command =
        getExecutorCommand("$" + ENV_SHARD_ID, numberOfInstances, isExecutor);
    container.setCommand(command);

    if (KubernetesContext.hasImagePullPolicy(configuration)) {
      container.setImagePullPolicy(KubernetesContext.getKubernetesImagePullPolicy(configuration));
    }

    // Configure environment variables.
    configureContainerEnvVars(container);

    // Set secret keys.
    setSecretKeyRefs(container);

    // Set container resources
    configureContainerResources(container, configuration, resource);

    // Set container ports.
    final boolean debuggingEnabled =
        TopologyUtils.getTopologyRemoteDebuggingEnabled(
            Runtime.topology(getRuntimeConfiguration()));
    configureContainerPorts(debuggingEnabled, numberOfInstances, container);

    // setup volume mounts
    mountVolumeIfPresent(container);
  }

  /**
   * Configures the resources in the <code>container</code> with values in the <code>config</code> taking precedence.
   * @param container The <code>container</code> to be configured.
   * @param configuration The <code>Config</code> object to check if a resource request needs to be set.
   * @param resource User defined resources limits from input.
   */
  @VisibleForTesting
  protected void configureContainerResources(final V1Container container,
                                             final Config configuration, final Resource resource) {
    if (container.getResources() == null) {
      container.setResources(new V1ResourceRequirements());
    }
    final V1ResourceRequirements resourceRequirements = container.getResources();

    // Configure resource limits. Deduplicate on limit name with user values taking precedence.
    if (resourceRequirements.getLimits() == null) {
      resourceRequirements.setLimits(new HashMap<>());
    }
    final Map<String, Quantity> limits = resourceRequirements.getLimits();
    limits.put(KubernetesConstants.MEMORY,
        Quantity.fromString(KubernetesUtils.Megabytes(resource.getRam())));
    limits.put(KubernetesConstants.CPU,
        Quantity.fromString(Double.toString(roundDecimal(resource.getCpu(), 3))));

    // Set the Kubernetes container resource request.
    KubernetesContext.KubernetesResourceRequestMode requestMode =
        KubernetesContext.getKubernetesRequestMode(configuration);
    if (requestMode == KubernetesContext.KubernetesResourceRequestMode.EQUAL_TO_LIMIT) {
      LOG.log(Level.CONFIG, "Setting K8s Request equal to Limit");
      resourceRequirements.setRequests(limits);
    } else {
      LOG.log(Level.CONFIG, "Not setting K8s request because config was NOT_SET");
    }
    container.setResources(resourceRequirements);
  }

  /**
   * Configures the environment variables in the <code>container</code> with those Heron requires.
   * Heron's values take precedence.
   * @param container The <code>container</code> to be configured.
   */
  @VisibleForTesting
  protected void configureContainerEnvVars(final V1Container container) {
    // Deduplicate on var name with Heron defaults take precedence.
    KubernetesUtils.V1ControllerUtils<V1EnvVar> utils = new KubernetesUtils.V1ControllerUtils<>();
    container.setEnv(
        utils.mergeListsDedupe(getExecutorEnvVars(), container.getEnv(),
          Comparator.comparing(V1EnvVar::getName), "Pod Template Environment Variables")
    );
  }

  /**
   * Generates a list of <code>Environment Variables</code> required by Heron to function.
   * @return A list of configured <code>Environment Variables</code> required by Heron to function.
   */
  @VisibleForTesting
  protected static List<V1EnvVar> getExecutorEnvVars() {
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

    return Arrays.asList(envVarHost, envVarPodName);
  }

  /**
   * Configures the ports in the <code>container</code> with those Heron requires. Heron's values take precedence.
   * @param remoteDebugEnabled Flag used to indicate if debugging ports need to be added.
   * @param numberOfInstances The number of debugging ports to be opened.
   * @param container <code>container</code> to be configured.
   */
  @VisibleForTesting
  protected void configureContainerPorts(boolean remoteDebugEnabled, int numberOfInstances,
                                         final V1Container container) {
    List<V1ContainerPort> ports = new ArrayList<>(getExecutorPorts());

    if (remoteDebugEnabled) {
      ports.addAll(getDebuggingPorts(numberOfInstances));
    }

    // Set container ports. Deduplicate using port number with Heron defaults taking precedence.
    KubernetesUtils.V1ControllerUtils<V1ContainerPort> utils =
        new KubernetesUtils.V1ControllerUtils<>();
    container.setPorts(
        utils.mergeListsDedupe(getExecutorPorts(), container.getPorts(),
            Comparator.comparing(V1ContainerPort::getContainerPort), "Pod Template Ports")
    );
  }

  /**
   * Generates a list of <code>ports</code> required by Heron to function.
   * @return A list of configured <code>ports</code> required by Heron to function.
   */
  @VisibleForTesting
  protected static List<V1ContainerPort> getExecutorPorts() {
    List<V1ContainerPort> ports = new LinkedList<>();
    KubernetesConstants.EXECUTOR_PORTS.forEach((p, v) -> {
      final V1ContainerPort port = new V1ContainerPort()
          .name(p.getName())
          .containerPort(v);
      ports.add(port);
    });
    return ports;
  }

  /**
   * Generate the debugging ports required by Heron.
   * @param numberOfInstances The number of debugging ports to generate.
   * @return A list of configured debugging <code>ports</code>.
   */
  @VisibleForTesting
  protected static List<V1ContainerPort> getDebuggingPorts(int numberOfInstances) {
    List<V1ContainerPort> ports = new LinkedList<>();
    IntStream.range(0, numberOfInstances).forEach(i -> {
      final String portName =
          KubernetesConstants.JVM_REMOTE_DEBUGGER_PORT_NAME + "-" + i;
      final V1ContainerPort port = new V1ContainerPort()
          .name(portName)
          .containerPort(KubernetesConstants.JVM_REMOTE_DEBUGGER_PORT + i);
      ports.add(port);
    });
    return ports;
  }

  /**
   * Adds volume mounts to the <code>container</code> that Heron requires. Heron's values taking precedence.
   * @param container <code>container</code> to be configured.
   */
  @VisibleForTesting
  protected void mountVolumeIfPresent(final V1Container container) {
    final Config config = getConfiguration();
    if (KubernetesContext.hasContainerVolume(config)) {
      final V1VolumeMount mount =
          new V1VolumeMount()
              .name(KubernetesContext.getContainerVolumeName(config))
              .mountPath(KubernetesContext.getContainerVolumeMountPath(config));

      // Merge volume mounts. Deduplicate using mount's name with Heron defaults taking precedence.
      KubernetesUtils.V1ControllerUtils<V1VolumeMount> utils =
          new KubernetesUtils.V1ControllerUtils<>();
      container.setVolumeMounts(
          utils.mergeListsDedupe(Collections.singletonList(mount), container.getVolumeMounts(),
              Comparator.comparing(V1VolumeMount::getName), "Pod Template Volume Mounts")
      );
    }
  }

  private void setSecretKeyRefs(V1Container container) {
    final Config config = getConfiguration();
    final Map<String, String> podSecretKeyRefs = KubernetesContext.getPodSecretKeyRefs(config);
    for (Map.Entry<String, String> secret : podSecretKeyRefs.entrySet()) {
      final String[] keyRefParts = secret.getValue().split(":");
      if (keyRefParts.length != 2) {
        LOG.log(Level.SEVERE,
                "SecretKeyRef must be in the form name:key. <" + secret.getValue() + ">");
        throw new TopologyRuntimeManagementException(
                "SecretKeyRef must be in the form name:key. <" + secret.getValue() + ">");
      }
      String name = keyRefParts[0];
      String key = keyRefParts[1];
      final V1EnvVar envVar = new V1EnvVar()
              .name(secret.getKey())
                .valueFrom(new V1EnvVarSource()
                  .secretKeyRef(new V1SecretKeySelector()
                          .key(key)
                          .name(name)));
      container.addEnvItem(envVar);
    }
  }

  public static double roundDecimal(double value, int places) {
    double scale = Math.pow(10, places);
    return Math.round(value * scale) / scale;
  }

  /**
   * Initiates the process of locating and loading <code>Pod Template</code> from a <code>ConfigMap</code>.
   * The loaded text is then parsed into a usable <code>Pod Template</code>.
   * @param isExecutor Flag to indicate loading of <code>Pod Template</code> for <code>Executor</code>
   *                   or <code>Manager</code>.
   * @return A <code>Pod Template</code> which is loaded and parsed from a <code>ConfigMap</code>.
   */
  @VisibleForTesting
  protected V1PodTemplateSpec loadPodFromTemplate(boolean isExecutor) {
    final Pair<String, String> podTemplateConfigMapName = getPodTemplateLocation(isExecutor);

    // Default Pod Template.
    if (podTemplateConfigMapName == null) {
      LOG.log(Level.INFO, "Configuring cluster with the Default Pod Template");
      return new V1PodTemplateSpec();
    }

    if (isPodTemplateDisabled) {
      throw new TopologySubmissionException("Custom Pod Templates are disabled");
    }

    final String configMapName = podTemplateConfigMapName.first;
    final String podTemplateName = podTemplateConfigMapName.second;

    // Attempt to locate ConfigMap with provided Pod Template name.
    try {
      V1ConfigMap configMap = getConfigMap(configMapName);
      if (configMap == null) {
        throw new ApiException(
            String.format("K8s client unable to locate ConfigMap '%s'", configMapName));
      }

      final Map<String, String> configMapData = configMap.getData();
      if (configMapData != null && configMapData.containsKey(podTemplateName)) {
        // NullPointerException when Pod Template is empty.
        V1PodTemplateSpec podTemplate = ((V1PodTemplate)
            Yaml.load(configMapData.get(podTemplateName))).getTemplate();
        LOG.log(Level.INFO, String.format("Configuring cluster with the %s.%s Pod Template",
            configMapName, podTemplateName));
        return podTemplate;
      }

      // Failure to locate Pod Template with provided name.
      throw new ApiException(String.format("Failed to locate Pod Template '%s' in ConfigMap '%s'",
          podTemplateName, configMapName));
    } catch (ApiException e) {
      KubernetesUtils.logExceptionWithDetails(LOG, e.getMessage(), e);
      throw new TopologySubmissionException(e.getMessage());
    } catch (IOException | ClassCastException | NullPointerException e) {
      final String message = String.format("Error parsing Pod Template '%s' in ConfigMap '%s'",
          podTemplateName, configMapName);
      KubernetesUtils.logExceptionWithDetails(LOG, message, e);
      throw new TopologySubmissionException(message);
    }
  }

  /**
   * Extracts the <code>ConfigMap</code> and <code>Pod Template</code> names from the CLI parameter.
   * @param isExecutor Flag to indicate loading of <code>Pod Template</code> for <code>Executor</code>
   *                   or <code>Manager</code>.
   * @return A pair of the form <code>(ConfigMap, Pod Template)</code>.
   */
  @VisibleForTesting
  protected Pair<String, String> getPodTemplateLocation(boolean isExecutor) {
    final String podTemplateConfigMapName = KubernetesContext
        .getPodTemplateConfigMapName(getConfiguration(), isExecutor);

    if (podTemplateConfigMapName == null) {
      return null;
    }

    try {
      final int splitPoint = podTemplateConfigMapName.indexOf(".");
      final String configMapName = podTemplateConfigMapName.substring(0, splitPoint);
      final String podTemplateName = podTemplateConfigMapName.substring(splitPoint + 1);

      if (configMapName.isEmpty() || podTemplateName.isEmpty()) {
        throw new IllegalArgumentException("Empty ConfigMap or Pod Template name");
      }

      return new Pair<>(configMapName, podTemplateName);
    } catch (IndexOutOfBoundsException | IllegalArgumentException e) {
      final String message = "Invalid ConfigMap and/or Pod Template name";
      KubernetesUtils.logExceptionWithDetails(LOG, message, e);
      throw new TopologySubmissionException(message);
    }
  }

  /**
   * Retrieves a <code>ConfigMap</code> from the K8s cluster in the API Server's namespace.
   * @param configMapName Name of the <code>ConfigMap</code> to retrieve.
   * @return The retrieved <code>ConfigMap</code>.
   */
  @VisibleForTesting
  protected V1ConfigMap getConfigMap(String configMapName) {
    try {
      return coreClient.readNamespacedConfigMap(
          configMapName,
          getNamespace(),
          null,
          null,
          null);
    } catch (ApiException e) {
      final String message = "Error retrieving ConfigMaps";
      KubernetesUtils.logExceptionWithDetails(LOG, message, e);
      throw new TopologySubmissionException(String.format("%s: %s", message, e.getMessage()));
    }
  }

  /**
   * Generates <code>Persistent Volume Claims Templates</code> from a mapping of <code>Volumes</code>
   * to <code>key-value</code> pairs of configuration options and values.
   * @param mapOfOpts <code>Volume</code> to configuration <code>key-value</code> mappings.
   * @return Fully populated list of only dynamically backed <code>Persistent Volume Claims</code>.
   */
  @VisibleForTesting
  protected List<V1PersistentVolumeClaim> createPersistentVolumeClaims(
      final Map<String, Map<KubernetesConstants.VolumeClaimTemplateConfigKeys, String>> mapOfOpts) {

    List<V1PersistentVolumeClaim> listOfPVCs = new LinkedList<>();

    // Iterate over all the PVC Volumes.
    for (Map.Entry<String, Map<KubernetesConstants.VolumeClaimTemplateConfigKeys, String>> pvc
        : mapOfOpts.entrySet()) {

      // Only create claims for `OnDemand` volumes.
      final String claimName = pvc.getValue()
          .get(KubernetesConstants.VolumeClaimTemplateConfigKeys.claimName);
      if (claimName != null && !KubernetesConstants.LABEL_ON_DEMAND.equalsIgnoreCase(claimName)) {
        continue;
      }

      V1PersistentVolumeClaim claim = new V1PersistentVolumeClaimBuilder()
          .withNewMetadata()
            .withName(pvc.getKey())
            .withLabels(getPersistentVolumeClaimLabels(getTopologyName()))
          .endMetadata()
          .withNewSpec()
          .endSpec()
          .build();

      // Populate PVC options.
      for (Map.Entry<KubernetesConstants.VolumeClaimTemplateConfigKeys, String> option
          : pvc.getValue().entrySet()) {
        String optionValue = option.getValue();
        switch(option.getKey()) {
          case storageClassName:
            claim.getSpec().setStorageClassName(optionValue);
            break;
          case sizeLimit:
            claim.getSpec().setResources(
                    new V1ResourceRequirements()
                        .putRequestsItem("storage", new Quantity(optionValue)));
            break;
          case accessModes:
            claim.getSpec().setAccessModes(Arrays.asList(optionValue.split(",")));
            break;
          case volumeMode:
            claim.getSpec().setVolumeMode(optionValue);
            break;
          // Valid ignored options not used in a PVC.
          case path: case subPath: case claimName:
            break;
          default:
            throw new TopologySubmissionException(
                String.format("Invalid Persistent Volume Claim type option for '%s'",
                    option.getKey()));
        }
      }
      listOfPVCs.add(claim);
    }
    return listOfPVCs;
  }

  /**
   * Generates the <code>Volume</code> and <code>Volume Mounts</code> to be placed in the <code>executor container</code>.
   * @param mapConfig Mapping of <code>Volumes</code> to <code>key-value</code> configuration pairs.
   * @return A pair of configured lists of <code>V1Volume</code> and <code>V1VolumeMount</code>.
   */
  @VisibleForTesting
  protected Pair<List<V1Volume>, List<V1VolumeMount>> createPersistentVolumeClaimVolumesAndMounts(
      final Map<String, Map<KubernetesConstants.VolumeClaimTemplateConfigKeys, String>> mapConfig) {
    List<V1Volume> volumeList = new LinkedList<>();
    List<V1VolumeMount> mountList = new LinkedList<>();
    for (Map.Entry<String, Map<KubernetesConstants.VolumeClaimTemplateConfigKeys, String>> configs
        : mapConfig.entrySet()) {
      final String volumeName = configs.getKey();
      final String path = configs.getValue()
          .get(KubernetesConstants.VolumeClaimTemplateConfigKeys.path);
      final String subPath = configs.getValue()
          .get(KubernetesConstants.VolumeClaimTemplateConfigKeys.subPath);

      if (path == null || path.isEmpty()) {
        throw new TopologySubmissionException(
            String.format("A mount path is required and missing from '%s'", volumeName));
      }

      // Do not create Volumes for `OnDemand`.
      final String claimName = configs.getValue()
          .get(KubernetesConstants.VolumeClaimTemplateConfigKeys.claimName);
      if (claimName != null && !KubernetesConstants.LABEL_ON_DEMAND.equalsIgnoreCase(claimName)) {
        final V1Volume volume = new V1VolumeBuilder()
            .withName(volumeName)
            .withNewPersistentVolumeClaim()
              .withClaimName(claimName)
            .endPersistentVolumeClaim()
            .build();
        volumeList.add(volume);
      }

      final V1VolumeMountBuilder volumeMount = new V1VolumeMountBuilder()
          .withName(volumeName)
          .withMountPath(path);
      if (subPath != null && !subPath.isEmpty()) {
        volumeMount.withSubPath(subPath);
      }
      mountList.add(volumeMount.build());
    }
    return new Pair<>(volumeList, mountList);
  }

  /**
   * Makes a call to generate <code>Volumes</code> and <code>Volume Mounts</code> and then inserts them.
   * @param podSpec All generated <code>V1Volume</code> will be placed in the <code>Pod Spec</code>.
   * @param executor All generated <code>V1VolumeMount</code> will be placed in the <code>Container</code>.
   */
  @VisibleForTesting
  protected void configurePodWithPersistentVolumeClaimVolumesAndMounts(final V1PodSpec podSpec,
                                                                       final V1Container executor) {
    Pair<List<V1Volume>, List<V1VolumeMount>> volumesAndMounts =
        createPersistentVolumeClaimVolumesAndMounts(persistentVolumeClaimConfigs);

    // Deduplicate on Names with Persistent Volume Claims taking precedence.

    KubernetesUtils.V1ControllerUtils<V1VolumeMount> utilsMounts =
        new KubernetesUtils.V1ControllerUtils<>();
    executor.setVolumeMounts(
        utilsMounts.mergeListsDedupe(volumesAndMounts.second, executor.getVolumeMounts(),
            Comparator.comparing(V1VolumeMount::getName),
            "Executor and Persistent Volume Claim Volume Mounts"));

    KubernetesUtils.V1ControllerUtils<V1Volume> utilsVolumes =
        new KubernetesUtils.V1ControllerUtils<>();
    podSpec.setVolumes(
        utilsVolumes.mergeListsDedupe(volumesAndMounts.first, podSpec.getVolumes(),
            Comparator.comparing(V1Volume::getName),
            "Pod and Persistent Volume Claim Volumes"));
  }

  /**
   * Removes all Persistent Volume Claims associated with a specific topology, if they exist.
   * It looks for the following:
   * metadata:
   *   labels:
   *     topology: <code>topology-name</code>
   *     onDemand: <code>true</code>
   */
  private void removePersistentVolumeClaims() {
    final String topologyName = getTopologyName();
    final StringBuilder selectorLabel = new StringBuilder();

    // Generate selector label.
    for (Map.Entry<String, String> label
        : getPersistentVolumeClaimLabels(topologyName).entrySet()) {
      if (selectorLabel.length() != 0) {
        selectorLabel.append(",");
      }
      selectorLabel.append(label.getKey()).append("=").append(label.getValue());
    }

    // Remove all dynamically backed Persistent Volume Claims.
    try {
      V1Status status = coreClient.deleteCollectionNamespacedPersistentVolumeClaim(
          getNamespace(),
          null,
          null,
          null,
          null,
          null,
          selectorLabel.toString(),
          null,
          null,
          null,
          null,
          null,
          null,
          null);

      LOG.log(Level.INFO,
          String.format("Removing automatically generated Persistent Volume Claims for `%s`:%n%s",
          topologyName, status.getMessage()));
    } catch (ApiException e) {
      final String message = String.format("Failed to connect to K8s cluster to delete Persistent "
              + "Volume Claims for topology `%s`. A manual clean-up is required.%n%s",
          topologyName, e.getMessage());
      LOG.log(Level.WARNING, message);
      throw new TopologyRuntimeManagementException(message);
    }
  }

  /**
   * Generates the <code>Label</code> which are attached to a Topology's Persistent Volume Claims.
   * @param topologyName Attached to the topology match label.
   * @return A map consisting of the <code>label-value</code> pairs to be used in <code>Label</code>s.
   */
  @VisibleForTesting
  protected static Map<String, String> getPersistentVolumeClaimLabels(String topologyName) {
    return new HashMap<String, String>() {
      {
        put(KubernetesConstants.LABEL_TOPOLOGY, topologyName);
        put(KubernetesConstants.LABEL_ON_DEMAND, "true");
      }
    };
  }

  /**
   * Generates the <code>StatefulSet</code> name depending on if it is a <code>Executor</code> or
   * <code>Manager</code>.
   * @param isExecutor Flag used to generate name for <code>Executor</code> or <code>Manager</code>.
   * @return String <code>"topology-name"-executors</code>.
   */
  private String getStatefulSetName(boolean isExecutor) {
    return String.format("%s-%s", getTopologyName(),
        isExecutor ? KubernetesConstants.EXECUTOR_NAME + "s" : KubernetesConstants.MANAGER_NAME);
  }

  /**
   * Generates the <code>Selector</code> match labels with which resources in this topology can be found.
   * @return A label of the form <code>app=heron,topology=topology-name</code>.
   */
  private String createTopologySelectorLabels() {
    return String.format("%s=%s,%s=%s",
        KubernetesConstants.LABEL_APP, KubernetesConstants.LABEL_APP_VALUE,
        KubernetesConstants.LABEL_TOPOLOGY, getTopologyName());
  }

  /**
   * Creates <code>Resource Requirements</code> from a Map of <code>Config</code> items for <code>CPU</code>
   * and <code>Memory</code>.
   * @param configs <code>Configs</code> to be parsed for configuration.
   * @return Configured <code>Resource Requirements</code>. <code>null</code> will be returned if there
   * are no <code>configs</code>.
   */
  @VisibleForTesting
  protected Map<String, Quantity> createResourcesRequirement(Map<String, String> configs) {
    if (configs == null || configs.isEmpty()) {
      return null;
    }

    final Map<String, Quantity> requirements = new HashMap<>();

    final String memoryLimit = configs.get(KubernetesConstants.MEMORY);
    if (memoryLimit != null && !memoryLimit.isEmpty()) {
      requirements.put(KubernetesConstants.MEMORY, Quantity.fromString(
          KubernetesUtils.Megabytes(ByteAmount.fromGigabytes(Long.parseLong(memoryLimit)))));
    }
    final String cpuLimit = configs.get(KubernetesConstants.CPU);
    if (cpuLimit != null && !cpuLimit.isEmpty()) {
      requirements.put(KubernetesConstants.CPU, Quantity.fromString(
          Double.toString(V1Controller.roundDecimal(Double.parseDouble(cpuLimit), 3))));
    }

    // Return an empty map if there are no viable requirements.
    if (requirements.isEmpty()) {
      return null;
    } else {
      return requirements;
    }
  }
}
