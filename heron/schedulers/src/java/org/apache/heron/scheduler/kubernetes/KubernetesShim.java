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
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.common.annotations.VisibleForTesting;

import org.apache.heron.common.basics.Pair;
import org.apache.heron.scheduler.TopologyRuntimeManagementException;
import org.apache.heron.scheduler.TopologySubmissionException;
import org.apache.heron.spi.common.Config;
import org.apache.heron.spi.packing.PackingPlan;
import org.apache.heron.spi.packing.Resource;

import io.kubernetes.client.custom.V1Patch;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.Configuration;
import io.kubernetes.client.openapi.apis.AppsV1Api;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1ConfigMap;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1PodTemplate;
import io.kubernetes.client.openapi.models.V1PodTemplateSpec;
import io.kubernetes.client.openapi.models.V1Service;
import io.kubernetes.client.openapi.models.V1ServiceSpec;
import io.kubernetes.client.openapi.models.V1StatefulSet;
import io.kubernetes.client.openapi.models.V1StatefulSetSpec;
import io.kubernetes.client.openapi.models.V1Status;
import io.kubernetes.client.util.PatchUtils;
import io.kubernetes.client.util.Yaml;
import okhttp3.Response;

import static java.net.HttpURLConnection.HTTP_NOT_FOUND;

public class KubernetesShim extends KubernetesController {

  private static final Logger LOG =
      Logger.getLogger(KubernetesShim.class.getName());

  private final boolean isPodTemplateDisabled;

  private final AppsV1Api appsClient;
  private final CoreV1Api coreClient;

  /**
   * Configures the Kubernetes API Application and Core communications clients.
   * @param configuration <code>topology</code> configurations.
   * @param runtimeConfiguration Kubernetes runtime configurations.
   */
  KubernetesShim(Config configuration, Config runtimeConfiguration) {
    super(configuration, runtimeConfiguration);

    isPodTemplateDisabled = KubernetesContext.getPodTemplateDisabled(configuration);
    LOG.log(Level.WARNING, String.format("Pod Template configuration is %s",
        isPodTemplateDisabled ? "DISABLED" : "ENABLED"));
    LOG.log(Level.WARNING, String.format("Volume configuration from CLI is %s",
        KubernetesContext.getVolumesFromCLIDisabled(configuration) ? "DISABLED" : "ENABLED"));

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

  /**
   * Configures all components required by a <code>topology</code> and submits it to the Kubernetes scheduler.
   * @param packingPlan Used to configure the StatefulSets <code>Resource</code>s and replica count.
   * @return Success indicator.
   */
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

    // Find the max number of instances in a container so that we can open
    // enough ports if remote debugging is enabled.
    int numberOfInstances = 0;
    for (PackingPlan.ContainerPlan containerPlan : packingPlan.getContainers()) {
      numberOfInstances = Math.max(numberOfInstances, containerPlan.getInstances().size());
    }

    final StatefulSet.Configs clusterConfigs = new StatefulSet.Configs(
        getConfiguration(),
        getRuntimeConfiguration(),
        loadPodFromTemplate(false),
        loadPodFromTemplate(true)
        );

    final V1StatefulSet executors = StatefulSet.get()
        .create(StatefulSet.Type.Executor, clusterConfigs, containerResource, numberOfInstances);
    final V1StatefulSet manager = StatefulSet.get()
        .create(StatefulSet.Type.Manager, clusterConfigs, containerResource, numberOfInstances);

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

  /**
   * Shuts down a <code>topology</code> by deleting all associated resources.
   * <ul>
   *   <li><code>Persistent Volume Claims</code> added by the <code>topology</code>.</li>
   *   <li><code>StatefulSet</code> for both the <code>Executors</code> and <code>Manager</code>.</li>
   *   <li>Headless <code>Service</code> which facilitates communication between all Pods.</li>
   * </ul>
   * @return Success indicator.
   */
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
      coreClient.deleteCollectionNamespacedPod(getNamespace(), null, null, null, null, 0,
          createTopologySelectorLabels(), null, null, null, null, null, null, null);
      LOG.log(Level.WARNING, String.format("Restarting topology '%s'...", getTopologyName()));
    } catch (ApiException e) {
      LOG.log(Level.SEVERE, String.format("Failed to restart topology '%s'...", getTopologyName()));
      return false;
    }
    return true;
  }

  /**
   * Adds a specified number of Pods to a <code>topology</code>'s <code>Executors</code>.
   * @param containersToAdd Set of containers to be added.
   * @return The passed in <code>Packing Plan</code>.
   */
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

  /**
   * Removes a specified number of Pods from a <code>topology</code>'s <code>Executors</code>.
   * @param containersToRemove Set of containers to be removed.
   */
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

  /**
   * Performs an in-place update of the replica count for a <code>topology</code>. This allows the
   * <code>topology</code> Pod count to be scaled up or down.
   * @param replicas The new number of Pod replicas required.
   * @throws ApiException in the event there is a failure patching the StatefulSet.
   */
  private void patchStatefulSetReplicas(int replicas) throws ApiException {
    final String body =
            String.format(KubernetesConstants.JSON_PATCH_STATEFUL_SET_REPLICAS_FORMAT,
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

  /**
   * Retrieves the <code>Executors</code> StatefulSet configurations for the Kubernetes cluster.
   * @return <code>Executors</code> StatefulSet configurations.
   * @throws ApiException in the event there is a failure retrieving the StatefulSet.
   */
  V1StatefulSet getStatefulSet() throws ApiException {
    return appsClient.readNamespacedStatefulSet(getStatefulSetName(true), getNamespace(),
        null);
  }

  /**
   * Deletes the headless <code>Service</code> for a <code>topology</code>'s <code>Executors</code>
   * and <code>Manager</code> using the <code>topology</code>'s name.
   */
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
        LOG.log(Level.SEVERE,
            String.format("Error when deleting the Service of the job [%s] in namespace [%s]",
            getTopologyName(), getNamespace()));
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
      throw new TopologyRuntimeManagementException(
        String.format("Error deleting topology [%s] Kubernetes service", getTopologyName()), e);
    } catch (IOException e) {
      throw new TopologyRuntimeManagementException(
        String.format("Error deleting topology [%s] Kubernetes service", getTopologyName()), e);
    }
    LOG.log(Level.INFO,
        String.format("Headless Service for the Job [%s] in namespace [%s] is deleted.",
        getTopologyName(), getNamespace()));
  }

  /**
   * Deletes the StatefulSets for a <code>topology</code>'s <code>Executors</code> and <code>Manager</code>
   * using <code>Label</code>s.
   */
  void deleteStatefulSets() {
    try (Response response = appsClient.deleteCollectionNamespacedStatefulSetCall(getNamespace(),
        null, null, null, null, null, createTopologySelectorLabels(), null, null, null, null, null,
            null, null, null)
        .execute()) {

      if (!response.isSuccessful()) {
        if (response.code() == HTTP_NOT_FOUND) {
          LOG.log(Level.WARNING, "Tried to delete a non-existent StatefulSets for Topology: "
                  + getTopologyName());
          return;
        }
        LOG.log(Level.SEVERE,
            String.format("Error when deleting the StatefulSets of the job [%s] in namespace [%s]",
            getTopologyName(), getNamespace()));
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
      throw new TopologyRuntimeManagementException(
        String.format("Error deleting topology [%s] Kubernetes StatefulSets", getTopologyName()),
        e);
    } catch (IOException e) {
      throw new TopologyRuntimeManagementException(
        String.format("Error deleting topology [%s] Kubernetes StatefulSets", getTopologyName()),
        e);
    }
    LOG.log(Level.INFO,
        String.format("StatefulSet for the Job [%s] in namespace [%s] is deleted.",
          getTopologyName(), getNamespace()));
  }

  /**
   * Creates a headless <code>Service</code> to facilitate communication between Pods in a <code>topology</code>.
   * @return A fully configured <code>Service</code> to be used by a <code>topology</code>.
   */
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
   * Extracts <code>Service Annotations</code> for configurations.
   * @return Key-value pairs of service <code>Annotation</code>s to be added to the Pod.
   */
  private Map<String, String> getServiceAnnotations() {
    return KubernetesContext.getServiceAnnotations(getConfiguration());
  }

  /**
   * Generates the <code>heron</code> and <code>topology</code> name <code>Match Label</code>s.
   * @param topologyName Name of the <code>topology</code>.
   * @return Key-value pairs of <code>Match Label</code>s to be added to the Pod.
   */
  private Map<String, String> getPodMatchLabels(String topologyName) {
    final Map<String, String> labels = new HashMap<>();
    labels.put(KubernetesConstants.LABEL_APP, KubernetesConstants.LABEL_APP_VALUE);
    labels.put(KubernetesConstants.LABEL_TOPOLOGY, topologyName);
    return labels;
  }

  /**
   * Extracts <code>Selector Labels</code> for<code>Service</code>s from configurations.
   * @return Key-value pairs of <code>Service Labels</code> to be added to the Pod.
   */
  private Map<String, String> getServiceLabels() {
    return KubernetesContext.getServiceLabels(getConfiguration());
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
          null);
    } catch (ApiException e) {
      final String message = "Error retrieving ConfigMaps";
      KubernetesUtils.logExceptionWithDetails(LOG, message, e);
      throw new TopologySubmissionException(String.format("%s: %s", message, e.getMessage()));
    }
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
    final String name = getTopologyName();
    final StringBuilder selectorLabel = new StringBuilder();

    // Generate selector label.
    for (Map.Entry<String, String> label : getPersistentVolumeClaimLabels(name).entrySet()) {
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
          name, status.getMessage()));
    } catch (ApiException e) {
      final String message = String.format("Failed to connect to K8s cluster to delete Persistent "
              + "Volume Claims for topology `%s`. A manual clean-up is required.%n%s",
          name, e.getMessage());
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
        isExecutor ? KubernetesConstants.EXECUTOR_NAME : KubernetesConstants.MANAGER_NAME);
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
}
