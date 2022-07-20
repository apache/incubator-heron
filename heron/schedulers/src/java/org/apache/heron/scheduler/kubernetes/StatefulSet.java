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
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.google.common.annotations.VisibleForTesting;

import org.apache.heron.api.utils.TopologyUtils;
import org.apache.heron.common.basics.Pair;
import org.apache.heron.scheduler.TopologyRuntimeManagementException;
import org.apache.heron.scheduler.TopologySubmissionException;
import org.apache.heron.scheduler.utils.Runtime;
import org.apache.heron.scheduler.utils.SchedulerUtils;
import org.apache.heron.scheduler.utils.SchedulerUtils.ExecutorPort;
import org.apache.heron.spi.common.Config;
import org.apache.heron.spi.packing.Resource;

import io.kubernetes.client.custom.Quantity;
import io.kubernetes.client.openapi.ApiException;
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
import io.kubernetes.client.openapi.models.V1PodSpec;
import io.kubernetes.client.openapi.models.V1PodTemplate;
import io.kubernetes.client.openapi.models.V1PodTemplateSpec;
import io.kubernetes.client.openapi.models.V1ResourceRequirements;
import io.kubernetes.client.openapi.models.V1SecretKeySelector;
import io.kubernetes.client.openapi.models.V1SecretVolumeSourceBuilder;
import io.kubernetes.client.openapi.models.V1Service;
import io.kubernetes.client.openapi.models.V1ServiceSpec;
import io.kubernetes.client.openapi.models.V1StatefulSet;
import io.kubernetes.client.openapi.models.V1StatefulSetSpec;
import io.kubernetes.client.openapi.models.V1Toleration;
import io.kubernetes.client.openapi.models.V1Volume;
import io.kubernetes.client.openapi.models.V1VolumeMount;
import io.kubernetes.client.util.Yaml;

final class StatefulSet {
  private final Map<Type, IStatefulSetFactory> statefulsets = new HashMap<>();

  public enum Type {
    Executor,
    Manager
  }

  private Configs clusterConfigs;

  private static final Logger LOG = Logger.getLogger(StatefulSet.class.getName());

  private static final String ENV_SHARD_ID = "SHARD_ID";

  /**
   * Container class of all the Kubernetes cluster configurations. The methods contained within
   * <code>KubernetesController</code> cannot be accessed externally since it is an abstract class.
   */
  static final class Configs {
    private final CoreV1Api coreClient;
    private final String namespace;
    private final String topologyName;
    private final Config configuration;
    private final Config runtimeConfiguration;
    private final boolean podTemplateDisabled;

    Configs(CoreV1Api coreClient, String namespace, Config configuration,
            Config runtimeConfiguration) {
      this.coreClient = coreClient;
      this.namespace = namespace;
      this.topologyName = Runtime.topologyName(runtimeConfiguration);
      this.configuration = configuration;
      this.runtimeConfiguration = runtimeConfiguration;
      this.podTemplateDisabled = KubernetesContext.getPodTemplateDisabled(configuration);
    }

    Config getConfiguration() {
      return configuration;
    }

    Config getRuntimeConfiguration() {
      return runtimeConfiguration;
    }

    String getNamespace() {
      return namespace;
    }

    String getTopologyName() {
      return topologyName;
    }

    boolean isPodTemplateDisabled() {
      return podTemplateDisabled;
    }

    public CoreV1Api getCoreClient() {
      return coreClient;
    }
  }


  private StatefulSet() {
    statefulsets.put(Type.Executor, new ExecutorFactory());
    statefulsets.put(Type.Manager, new ManagerFactory());
  }

  interface IStatefulSetFactory {
    V1StatefulSet create(Configs configs, Resource containerResources, int numberOfInstances);
  }

  /**
   * Creates configured <code>Executor</code> or <code>Manager</code> <code>Stateful Set</code>.
   * @param type One of <code>Executor</code> or <code>Manager</code>
   * @param configs Cluster configuration information container.
   * @param containerResources The container system resource configurations.
   * @param numberOfInstances The container count.
   * @return Fully configured <code>Stateful Set</code> or <code>null</code> on invalid <code>type</code>.
   */
  V1StatefulSet create(Type type, Configs configs, Resource containerResources,
                       int numberOfInstances) {
    if (statefulsets.containsKey(type)) {
      return statefulsets.get(type).create(configs, containerResources, numberOfInstances);
    }
    return null;
  }

  class ExecutorFactory implements IStatefulSetFactory {

    @Override
    public V1StatefulSet create(Configs configs, Resource containerResources,
                                int numberOfInstances) {
      clusterConfigs = configs;
      return null;
    }
  }

  class ManagerFactory implements IStatefulSetFactory {

    @Override
    public V1StatefulSet create(Configs configs, Resource containerResources,
                                int numberOfInstances) {
      clusterConfigs = configs;
      return null;
    }
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
    final Config configuration = clusterConfigs.getConfiguration();
    final Config runtimeConfiguration = clusterConfigs.getRuntimeConfiguration();
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

  /**
   * Creates a headless <code>Service</code> to facilitate communication between Pods in a <code>topology</code>.
   * @return A fully configured <code>Service</code> to be used by a <code>topology</code>.
   */
  private V1Service createTopologyService() {
    final String topologyName = clusterConfigs.getTopologyName();

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
   * Creates and configures the <code>StatefulSet</code> which the topology's <code>executor</code>s will run in.
   * @param containerResource Passed down to configure the <code>executor</code> resource limits.
   * @param numberOfInstances Used to configure the execution command and ports for the <code>executor</code>.
   * @param isExecutor Flag used to configure components specific to <code>executor</code> and <code>manager</code>.
   * @return A fully configured <code>V1StatefulSet</code> for the topology's <code>executors</code>.
   */
  private V1StatefulSet createStatefulSet(Resource containerResource, int numberOfInstances,
                                          boolean isExecutor) {
    final String topologyName = clusterConfigs.getTopologyName();
    final Config runtimeConfiguration = clusterConfigs.getRuntimeConfiguration();

    final List<V1Volume> volumes = new LinkedList<>();
    final List<V1VolumeMount> volumeMounts = new LinkedList<>();

    // Collect Persistent Volume Claim configurations from the CLI.
    final Map<String, Map<KubernetesConstants.VolumeConfigKeys, String>> configsPVC =
        KubernetesContext.getVolumeClaimTemplates(clusterConfigs.getConfiguration(), isExecutor);

    // Collect all Volume configurations from the CLI and generate Volumes and Volume Mounts.
    createVolumeAndMountsPersistentVolumeClaimCLI(configsPVC, volumes, volumeMounts);
    createVolumeAndMountsHostPathCLI(
        KubernetesContext.getVolumeHostPath(clusterConfigs.getConfiguration(), isExecutor),
        volumes, volumeMounts);
    createVolumeAndMountsEmptyDirCLI(
        KubernetesContext.getVolumeEmptyDir(clusterConfigs.getConfiguration(), isExecutor),
        volumes, volumeMounts);
    createVolumeAndMountsNFSCLI(
        KubernetesContext.getVolumeNFS(clusterConfigs.getConfiguration(), isExecutor),
        volumes, volumeMounts);

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

    configurePodSpec(podTemplateSpec, containerResource, numberOfInstances, isExecutor,
        volumes, volumeMounts);

    statefulSetSpec.setTemplate(podTemplateSpec);

    statefulSet.setSpec(statefulSetSpec);

    statefulSetSpec.setVolumeClaimTemplates(createPersistentVolumeClaims(configsPVC));

    return statefulSet;
  }

  /**
   * Extracts general Pod <code>Annotation</code>s from configurations.
   * @return Key-value pairs of general <code>Annotation</code>s to be added to the Pod.
   */
  private Map<String, String> getPodAnnotations() {
    return KubernetesContext.getPodAnnotations(clusterConfigs.getConfiguration());
  }

  /**
   * Extracts <code>Service Annotations</code> for configurations.
   * @return Key-value pairs of service <code>Annotation</code>s to be added to the Pod.
   */
  private Map<String, String> getServiceAnnotations() {
    return KubernetesContext.getServiceAnnotations(clusterConfigs.getConfiguration());
  }

  /**
   * Generates <code>Label</code>s to indicate Prometheus scraping and the exposed port.
   * @return Key-value pairs of Prometheus <code>Annotation</code>s to be added to the Pod.
   */
  private Map<String, String> getPrometheusAnnotations() {
    final Map<String, String> annotations = new HashMap<>();
    annotations.put(KubernetesConstants.ANNOTATION_PROMETHEUS_SCRAPE, "true");
    annotations.put(KubernetesConstants.ANNOTATION_PROMETHEUS_PORT,
        KubernetesConstants.PROMETHEUS_PORT);

    return annotations;
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
   * Extracts <code>Label</code>s from configurations, generates the <code>heron</code> and
   * <code>topology</code> name <code>Label</code>s.
   * @param topologyName Name of the <code>topology</code>.
   * @return Key-value pairs of <code>Label</code>s to be added to the Pod.
   */
  private Map<String, String> getPodLabels(String topologyName) {
    final Map<String, String> labels = new HashMap<>();
    labels.put(KubernetesConstants.LABEL_APP, KubernetesConstants.LABEL_APP_VALUE);
    labels.put(KubernetesConstants.LABEL_TOPOLOGY, topologyName);
    labels.putAll(KubernetesContext.getPodLabels(clusterConfigs.getConfiguration()));
    return labels;
  }

  /**
   * Extracts <code>Selector Labels</code> for<code>Service</code>s from configurations.
   * @return Key-value pairs of <code>Service Labels</code> to be added to the Pod.
   */
  private Map<String, String> getServiceLabels() {
    return KubernetesContext.getServiceLabels(clusterConfigs.getConfiguration());
  }

  /**
   * Configures the <code>Pod Spec</code> section of the <code>StatefulSet</code>. The <code>Heron</code> container
   * will be configured to allow it to function but other supplied containers are loaded verbatim.
   * @param podTemplateSpec The <code>Pod Template Spec</code> section to update.
   * @param resource Passed down to configure the resource limits.
   * @param numberOfInstances Passed down to configure the ports.
   * @param isExecutor Flag used to configure components specific to <code>Executor</code> and <code>Manager</code>.
   * @param volumes <code>Volumes</code> generated from configurations options.
   * @param volumeMounts <code>Volume Mounts</code> generated from configurations options.
   */
  private void configurePodSpec(final V1PodTemplateSpec podTemplateSpec, Resource resource,
      int numberOfInstances, boolean isExecutor, List<V1Volume> volumes,
      List<V1VolumeMount> volumeMounts) {
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

    if (!volumes.isEmpty() || !volumeMounts.isEmpty()) {
      configurePodWithVolumesAndMountsFromCLI(podSpec, heronContainer, volumes,
          volumeMounts);
    }

    configureHeronContainer(resource, numberOfInstances, heronContainer, isExecutor);

    podSpec.setContainers(containers);

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
   * Adds <code>Volume Mounts</code> for <code>Secrets</code> to a pod.
   * @param podSpec <code>Pod Spec</code> to add secrets to.
   */
  private void mountSecretsAsVolumes(V1PodSpec podSpec) {
    final Config config = clusterConfigs.getConfiguration();
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
    final Config configuration = clusterConfigs.getConfiguration();

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
    configureContainerResources(container, configuration, resource, isExecutor);

    // Set container ports.
    final boolean debuggingEnabled =
        TopologyUtils.getTopologyRemoteDebuggingEnabled(
            Runtime.topology(clusterConfigs.getRuntimeConfiguration()));
    configureContainerPorts(debuggingEnabled, numberOfInstances, container);

    // setup volume mounts
    mountVolumeIfPresent(container);
  }

  /**
   * Configures the resources in the <code>container</code> with values in the <code>config</code> taking precedence.
   * @param container The <code>container</code> to be configured.
   * @param configuration The <code>Config</code> object to check if a resource request needs to be set.
   * @param resource User defined resources limits from input.
   * @param isExecutor Flag to indicate configuration for an <code>executor</code> or <code>manager</code>.
   */
  @VisibleForTesting
  protected void configureContainerResources(final V1Container container,
                                             final Config configuration, final Resource resource,
                                             boolean isExecutor) {
    if (container.getResources() == null) {
      container.setResources(new V1ResourceRequirements());
    }
    final V1ResourceRequirements resourceRequirements = container.getResources();

    // Collect Limits and Requests from CLI.
    final Map<String, Quantity> limitsCLI = createResourcesRequirement(
        KubernetesContext.getResourceLimits(configuration, isExecutor));
    final Map<String, Quantity> requestsCLI = createResourcesRequirement(
        KubernetesContext.getResourceRequests(configuration, isExecutor));

    if (resourceRequirements.getLimits() == null) {
      resourceRequirements.setLimits(new HashMap<>());
    }

    // Set Limits and Resources from CLI <if> available, <else> use Configs. Deduplicate on name
    // with precedence [1] CLI, [2] Config.
    final Map<String, Quantity> limits = resourceRequirements.getLimits();
    final Quantity limitCPU = limitsCLI.getOrDefault(KubernetesConstants.CPU,
        Quantity.fromString(Double.toString(KubernetesUtils.roundDecimal(resource.getCpu(), 3))));
    final Quantity limitMEMORY = limitsCLI.getOrDefault(KubernetesConstants.MEMORY,
        Quantity.fromString(KubernetesUtils.Megabytes(resource.getRam())));

    limits.put(KubernetesConstants.MEMORY, limitMEMORY);
    limits.put(KubernetesConstants.CPU, limitCPU);

    // Set the Kubernetes container resource request.
    // Order: [1] CLI, [2] EQUAL_TO_LIMIT, [3] NOT_SET
    KubernetesContext.KubernetesResourceRequestMode requestMode =
        KubernetesContext.getKubernetesRequestMode(configuration);
    if (!requestsCLI.isEmpty()) {
      if (resourceRequirements.getRequests() == null) {
        resourceRequirements.setRequests(new HashMap<>());
      }
      final Map<String, Quantity> requests = resourceRequirements.getRequests();

      if (requestsCLI.containsKey(KubernetesConstants.MEMORY)) {
        requests.put(KubernetesConstants.MEMORY, requestsCLI.get(KubernetesConstants.MEMORY));
      }
      if (requestsCLI.containsKey(KubernetesConstants.CPU)) {
        requests.put(KubernetesConstants.CPU, requestsCLI.get(KubernetesConstants.CPU));
      }
    } else if (requestMode == KubernetesContext.KubernetesResourceRequestMode.EQUAL_TO_LIMIT) {
      LOG.log(Level.CONFIG, "Setting K8s Request equal to Limit");
      resourceRequirements.setRequests(limits);
    } else {
      LOG.log(Level.CONFIG, "Not setting K8s request because config was NOT_SET");
    }
    container.setResources(resourceRequirements);
  }

  /**
   * Creates <code>Resource Requirements</code> from a Map of <code>Config</code> items for <code>CPU</code>
   * and <code>Memory</code>.
   * @param configs <code>Configs</code> to be parsed for configuration.
   * @return Configured <code>Resource Requirements</code>. An <code>empty</code> map will be returned
   * if there are no <code>configs</code>.
   */
  @VisibleForTesting
  protected Map<String, Quantity> createResourcesRequirement(Map<String, String> configs) {
    final Map<String, Quantity> requirements = new HashMap<>();

    if (configs == null || configs.isEmpty()) {
      return requirements;
    }

    final String memoryLimit = configs.get(KubernetesConstants.MEMORY);
    if (memoryLimit != null && !memoryLimit.isEmpty()) {
      requirements.put(KubernetesConstants.MEMORY, Quantity.fromString(memoryLimit));
    }
    final String cpuLimit = configs.get(KubernetesConstants.CPU);
    if (cpuLimit != null && !cpuLimit.isEmpty()) {
      requirements.put(KubernetesConstants.CPU, Quantity.fromString(cpuLimit));
    }

    return requirements;
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
    final Config config = clusterConfigs.getConfiguration();
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

  /**
   * Adds <code>Secret Key</code> references to a <code>container</code>.
   * @param container <code>container</code> to be configured.
   */
  private void setSecretKeyRefs(V1Container container) {
    final Config config = clusterConfigs.getConfiguration();
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

    if (clusterConfigs.isPodTemplateDisabled()) {
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
        .getPodTemplateConfigMapName(clusterConfigs.getConfiguration(), isExecutor);

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
      return clusterConfigs.getCoreClient().readNamespacedConfigMap(
          configMapName,
          clusterConfigs.getNamespace(),
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
      final Map<String, Map<KubernetesConstants.VolumeConfigKeys, String>> mapOfOpts) {

    List<V1PersistentVolumeClaim> listOfPVCs = new LinkedList<>();

    // Iterate over all the PVC Volumes.
    for (Map.Entry<String, Map<KubernetesConstants.VolumeConfigKeys, String>> pvc
        : mapOfOpts.entrySet()) {

      // Only create claims for `OnDemand` volumes.
      final String claimName = pvc.getValue().get(KubernetesConstants.VolumeConfigKeys.claimName);
      if (claimName != null && !KubernetesConstants.LABEL_ON_DEMAND.equalsIgnoreCase(claimName)) {
        continue;
      }

      listOfPVCs.add(Volumes.get()
          .createPersistentVolumeClaim(pvc.getKey(),
              getPersistentVolumeClaimLabels(clusterConfigs.getTopologyName()), pvc.getValue()));
    }
    return listOfPVCs;
  }

  /**
   * Generates the <code>Volume</code>s and <code>Volume Mounts</code> for <code>Persistent Volume Claims</code>s
   *  to be placed in the <code>Executor</code> and <code>Manager</code> from options on the CLI.
   * @param mapConfig Mapping of <code>Volume</code> option <code>key-value</code> configuration pairs.
   * @param volumes A list of <code>Volume</code> to append to.
   * @param volumeMounts A list of <code>Volume Mounts</code> to append to.
   */
  @VisibleForTesting
  protected void createVolumeAndMountsPersistentVolumeClaimCLI(
      final Map<String, Map<KubernetesConstants.VolumeConfigKeys, String>> mapConfig,
      final List<V1Volume> volumes, final List<V1VolumeMount> volumeMounts) {
    for (Map.Entry<String, Map<KubernetesConstants.VolumeConfigKeys, String>> configs
        : mapConfig.entrySet()) {
      final String volumeName = configs.getKey();

      // Do not create Volumes for `OnDemand`.
      final String claimName = configs.getValue()
          .get(KubernetesConstants.VolumeConfigKeys.claimName);
      if (claimName != null && !KubernetesConstants.LABEL_ON_DEMAND.equalsIgnoreCase(claimName)) {
        volumes.add(Volumes.get().createPersistentVolumeClaim(claimName, volumeName));
      }
      volumeMounts.add(Volumes.get().createMount(volumeName, configs.getValue()));
    }
  }

  /**
   * Generates the <code>Volume</code>s and <code>Volume Mounts</code> for <code>emptyDir</code>s to be
   * placed in the <code>Executor</code> and <code>Manager</code> from options on the CLI.
   * @param mapOfOpts Mapping of <code>Volume</code> option <code>key-value</code> configuration pairs.
   * @param volumes A list of <code>Volume</code> to append to.
   * @param volumeMounts A list of <code>Volume Mounts</code> to append to.
   */
  @VisibleForTesting
  protected void createVolumeAndMountsEmptyDirCLI(
      final Map<String, Map<KubernetesConstants.VolumeConfigKeys, String>> mapOfOpts,
      final List<V1Volume> volumes, final List<V1VolumeMount> volumeMounts) {
    for (Map.Entry<String, Map<KubernetesConstants.VolumeConfigKeys, String>> configs
        : mapOfOpts.entrySet()) {
      final String volumeName = configs.getKey();
      final V1Volume volume = Volumes.get()
          .createVolume(Volumes.VolumeType.EmptyDir, volumeName, configs.getValue());
      volumes.add(volume);
      volumeMounts.add(Volumes.get().createMount(volumeName, configs.getValue()));
    }
  }

  /**
   * Generates the <code>Volume</code>s and <code>Volume Mounts</code> for <code>Host Path</code>s to be
   * placed in the <code>Executor</code> and <code>Manager</code> from options on the CLI.
   * @param mapOfOpts Mapping of <code>Volume</code> option <code>key-value</code> configuration pairs.
   * @param volumes A list of <code>Volume</code> to append to.
   * @param volumeMounts A list of <code>Volume Mounts</code> to append to.
   */
  @VisibleForTesting
  protected void createVolumeAndMountsHostPathCLI(
      final Map<String, Map<KubernetesConstants.VolumeConfigKeys, String>> mapOfOpts,
      final List<V1Volume> volumes, final List<V1VolumeMount> volumeMounts) {
    for (Map.Entry<String, Map<KubernetesConstants.VolumeConfigKeys, String>> configs
        : mapOfOpts.entrySet()) {
      final String volumeName = configs.getKey();
      final V1Volume volume = Volumes.get()
          .createVolume(Volumes.VolumeType.HostPath, volumeName, configs.getValue());
      volumes.add(volume);
      volumeMounts.add(Volumes.get().createMount(volumeName, configs.getValue()));
    }
  }

  /**
   * Generates the <code>Volume</code>s and <code>Volume Mounts</code> for <code>NFS</code>s to be
   * placed in the <code>Executor</code> and <code>Manager</code> from options on the CLI.
   * @param mapOfOpts Mapping of <code>Volume</code> option <code>key-value</code> configuration pairs.
   * @param volumes A list of <code>Volume</code> to append to.
   * @param volumeMounts A list of <code>Volume Mounts</code> to append to.
   */
  @VisibleForTesting
  protected void createVolumeAndMountsNFSCLI(
      final Map<String, Map<KubernetesConstants.VolumeConfigKeys, String>> mapOfOpts,
      final List<V1Volume> volumes, final List<V1VolumeMount> volumeMounts) {
    for (Map.Entry<String, Map<KubernetesConstants.VolumeConfigKeys, String>> configs
        : mapOfOpts.entrySet()) {
      final String volumeName = configs.getKey();
      final V1Volume volume = Volumes.get()
          .createVolume(Volumes.VolumeType.NetworkFileSystem, volumeName, configs.getValue());
      volumes.add(volume);
      volumeMounts.add(Volumes.get().createMount(volumeName, configs.getValue()));
    }
  }

  /**
   * Configures the Pod Spec and Heron container with <code>Volumes</code> and <code>Volume Mounts</code>.
   * @param podSpec All generated <code>V1Volume</code> will be placed in the <code>Pod Spec</code>.
   * @param executor All generated <code>V1VolumeMount</code> will be placed in the <code>Container</code>.
   * @param volumes <code>Volumes</code> to be inserted in the Pod Spec.
   * @param volumeMounts <code>Volumes Mounts</code> to be inserted in the Heron container.
   */
  @VisibleForTesting
  protected void configurePodWithVolumesAndMountsFromCLI(final V1PodSpec podSpec,
      final V1Container executor, List<V1Volume> volumes, List<V1VolumeMount> volumeMounts) {

    // Deduplicate on Names with Persistent Volume Claims taking precedence.

    KubernetesUtils.V1ControllerUtils<V1Volume> utilsVolumes =
        new KubernetesUtils.V1ControllerUtils<>();
    podSpec.setVolumes(
        utilsVolumes.mergeListsDedupe(volumes, podSpec.getVolumes(),
            Comparator.comparing(V1Volume::getName),
            "Pod with Volumes"));

    KubernetesUtils.V1ControllerUtils<V1VolumeMount> utilsMounts =
        new KubernetesUtils.V1ControllerUtils<>();
    executor.setVolumeMounts(
        utilsMounts.mergeListsDedupe(volumeMounts, executor.getVolumeMounts(),
            Comparator.comparing(V1VolumeMount::getName),
            "Heron container with Volume Mounts"));
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
    return String.format("%s-%s", clusterConfigs.getTopologyName(),
        isExecutor ? KubernetesConstants.EXECUTOR_NAME : KubernetesConstants.MANAGER_NAME);
  }

  /**
   * Generates the <code>Selector</code> match labels with which resources in this topology can be found.
   * @return A label of the form <code>app=heron,topology=topology-name</code>.
   */
  private String createTopologySelectorLabels() {
    return String.format("%s=%s,%s=%s",
        KubernetesConstants.LABEL_APP, KubernetesConstants.LABEL_APP_VALUE,
        KubernetesConstants.LABEL_TOPOLOGY, clusterConfigs.getTopologyName());
  }
}
