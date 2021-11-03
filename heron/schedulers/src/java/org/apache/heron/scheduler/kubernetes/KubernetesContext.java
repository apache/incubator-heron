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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.heron.scheduler.TopologySubmissionException;
import org.apache.heron.spi.common.Config;
import org.apache.heron.spi.common.Context;

public final class KubernetesContext extends Context {
  public static final String HERON_EXECUTOR_DOCKER_IMAGE = "heron.executor.docker.image";

  public static final String HERON_KUBERNETES_SCHEDULER_URI = "heron.kubernetes.scheduler.uri";

  public static final String HERON_KUBERNETES_SCHEDULER_NAMESPACE =
      "heron.kubernetes.scheduler.namespace";

  public static final String HERON_KUBERNETES_SCHEDULER_IMAGE_PULL_POLICY =
      "heron.kubernetes.scheduler.imagePullPolicy";

  public enum KubernetesResourceRequestMode {
    /**
     * The Kubernetes Request will not be set by the Heron Kubernetes Scheduler.
     * The generated pods will use the Kubernetes default values.
     */
    NOT_SET,
    /**
     * The Kubernetes Pod Resource Request will be set to the same values as
     * provided in the Resource Limit. This mode effectively guarantees the
     * cpu and memory will be reserved.
     */
    EQUAL_TO_LIMIT
  }
  /**
   * This config item is used to determine how to configure the K8s Resource Request.
   * The format of this flag is the string encoded values of the
   * underlying KubernetesRequestMode value.
   */
  public static final String KUBERNETES_RESOURCE_REQUEST_MODE =
          "heron.kubernetes.resource.request.mode";

  public static final String KUBERNETES_VOLUME_NAME = "heron.kubernetes.volume.name";
  public static final String KUBERNETES_VOLUME_TYPE = "heron.kubernetes.volume.type";


  // HostPath volume keys
  // https://kubernetes.io/docs/concepts/storage/volumes/#hostpath
  public static final String KUBERNETES_VOLUME_HOSTPATH_PATH =
      "heron.kubernetes.volume.hostPath.path";

  // nfs volume keys
  // https://kubernetes.io/docs/concepts/storage/volumes/#nfs
  public static final String KUBERNETES_VOLUME_NFS_PATH =
      "heron.kubernetes.volume.nfs.path";
  public static final String KUBERNETES_VOLUME_NFS_SERVER =
      "heron.kubernetes.volume.nfs.server";

  // awsElasticBlockStore volume keys
  // https://kubernetes.io/docs/concepts/storage/volumes/#awselasticblockstore
  public static final String KUBERNETES_VOLUME_AWS_EBS_VOLUME_ID =
      "heron.kubernetes.volume.awsElasticBlockStore.volumeID";
  public static final String KUBERNETES_VOLUME_AWS_EBS_FS_TYPE =
      "heron.kubernetes.volume.awsElasticBlockStore.fsType";

  // pod template configmap
  public static final String KUBERNETES_POD_TEMPLATE_CONFIGMAP_NAME =
      "heron.kubernetes.pod.template.configmap.name";
  public static final String KUBERNETES_POD_TEMPLATE_CONFIGMAP_DISABLED =
      "heron.kubernetes.pod.template.configmap.disabled";

  // container mount volume mount keys
  public static final String KUBERNETES_CONTAINER_VOLUME_MOUNT_NAME =
      "heron.kubernetes.container.volumeMount.name";
  public static final String KUBERNETES_CONTAINER_VOLUME_MOUNT_PATH =
      "heron.kubernetes.container.volumeMount.path";

  public static final String KUBERNETES_POD_ANNOTATION_PREFIX =
      "heron.kubernetes.pod.annotation.";
  public static final String KUBERNETES_SERVICE_ANNOTATION_PREFIX =
      "heron.kubernetes.service.annotation.";
  public static final String KUBERNETES_POD_LABEL_PREFIX =
      "heron.kubernetes.pod.label.";
  public static final String KUBERNETES_SERVICE_LABEL_PREFIX =
      "heron.kubernetes.service.label.";
  // heron.kubernetes.pod.secret.heron-secret=/etc/secrets
  public static final String KUBERNETES_POD_SECRET_PREFIX =
      "heron.kubernetes.pod.secret.";
  // heron.kubernetes.pod.secretKeyRef.ENV_NAME=name:key
  public static final String KUBERNETES_POD_SECRET_KEY_REF_PREFIX =
      "heron.kubernetes.pod.secretKeyRef.";

  // Persistent Volume Claims
  public static final String KUBERNETES_PERSISTENT_VOLUME_CLAIMS_CLI_DISABLED =
      "heron.kubernetes.persistent.volume.claims.cli.disabled";
  // heron.kubernetes.volumes.persistentVolumeClaim.VOLUME_NAME.OPTION=OPTION_VALUE
  public static final String KUBERNETES_PERSISTENT_VOLUME_CLAIM_PREFIX =
      "heron.kubernetes.volumes.persistentVolumeClaim.";

  private KubernetesContext() {
  }

  public static String getExecutorDockerImage(Config config) {
    return config.getStringValue(HERON_EXECUTOR_DOCKER_IMAGE);
  }

  public static String getSchedulerURI(Config config) {
    return config.getStringValue(HERON_KUBERNETES_SCHEDULER_URI);
  }

  public static String getKubernetesNamespace(Config config) {
    return config.getStringValue(HERON_KUBERNETES_SCHEDULER_NAMESPACE);
  }

  public static String getKubernetesImagePullPolicy(Config config) {
    return config.getStringValue(HERON_KUBERNETES_SCHEDULER_IMAGE_PULL_POLICY);
  }

  public static boolean hasImagePullPolicy(Config config) {
    final String imagePullPolicy = getKubernetesImagePullPolicy(config);
    return isNotEmpty(imagePullPolicy);
  }

  public static KubernetesResourceRequestMode getKubernetesRequestMode(Config config) {
    return KubernetesResourceRequestMode.valueOf(
            config.getStringValue(KUBERNETES_RESOURCE_REQUEST_MODE));
  }

  static String getVolumeType(Config config) {
    return config.getStringValue(KUBERNETES_VOLUME_TYPE);
  }

  static String getVolumeName(Config config) {
    return config.getStringValue(KUBERNETES_VOLUME_NAME);
  }

  static String getHostPathVolumePath(Config config) {
    return config.getStringValue(KUBERNETES_VOLUME_HOSTPATH_PATH);
  }

  static String getNfsVolumePath(Config config) {
    return config.getStringValue(KUBERNETES_VOLUME_NFS_PATH);
  }

  static String getNfsServer(Config config) {
    return config.getStringValue(KUBERNETES_VOLUME_NFS_SERVER);
  }

  static String getAwsEbsVolumeId(Config config) {
    return config.getStringValue(KUBERNETES_VOLUME_AWS_EBS_VOLUME_ID);
  }

  static String getAwsEbsFsType(Config config) {
    return config.getStringValue(KUBERNETES_VOLUME_AWS_EBS_FS_TYPE);
  }

  static boolean hasVolume(Config config) {
    return isNotEmpty(getVolumeType(config));
  }

  static String getContainerVolumeName(Config config) {
    return config.getStringValue(KUBERNETES_CONTAINER_VOLUME_MOUNT_NAME);
  }

  static String getContainerVolumeMountPath(Config config) {
    return config.getStringValue(KUBERNETES_CONTAINER_VOLUME_MOUNT_PATH);
  }

  public static String getPodTemplateConfigMapName(Config config) {
    return config.getStringValue(KUBERNETES_POD_TEMPLATE_CONFIGMAP_NAME);
  }

  public static boolean getPodTemplateConfigMapDisabled(Config config) {
    final String disabled = config.getStringValue(KUBERNETES_POD_TEMPLATE_CONFIGMAP_DISABLED);
    return "true".equalsIgnoreCase(disabled);
  }

  public static Map<String, String> getPodLabels(Config config) {
    return getConfigItemsByPrefix(config, KUBERNETES_POD_LABEL_PREFIX);
  }

  public static Map<String, String> getServiceLabels(Config config) {
    return getConfigItemsByPrefix(config, KUBERNETES_SERVICE_LABEL_PREFIX);
  }

  public static Map<String, String> getPodAnnotations(Config config) {
    return getConfigItemsByPrefix(config, KUBERNETES_POD_ANNOTATION_PREFIX);
  }

  public static Map<String, String> getServiceAnnotations(Config config) {
    return getConfigItemsByPrefix(config, KUBERNETES_SERVICE_ANNOTATION_PREFIX);
  }

  public static Map<String, String> getPodSecretsToMount(Config config) {
    return getConfigItemsByPrefix(config, KUBERNETES_POD_SECRET_PREFIX);
  }

  public static Map<String, String> getPodSecretKeyRefs(Config config) {
    return getConfigItemsByPrefix(config, KUBERNETES_POD_SECRET_KEY_REF_PREFIX);
  }

  public static boolean getPersistentVolumeClaimDisabled(Config config) {
    final String disabled = config.getStringValue(KUBERNETES_PERSISTENT_VOLUME_CLAIMS_CLI_DISABLED);
    return "true".equalsIgnoreCase(disabled);
  }

  public static Map<String, Map<KubernetesConstants.PersistentVolumeClaimOptions, String>>
        getPersistentVolumeClaims(Config config) {
    final Logger LOG = Logger.getLogger(V1Controller.class.getName());

    final Set<String> completeConfigParam =
        getConfigKeys(config, KUBERNETES_PERSISTENT_VOLUME_CLAIM_PREFIX);
    final int prefixLength = KUBERNETES_PERSISTENT_VOLUME_CLAIM_PREFIX.length();
    final int volumeNameIdx = 0;
    final int optionIdx = 1;

    final Map<String, Map<KubernetesConstants.PersistentVolumeClaimOptions, String>> volumes
        = new HashMap<>();

    try {
      for (String param : completeConfigParam) {
        final String[] tokens = param.substring(prefixLength).split("\\.");
        final String volumeName = tokens[volumeNameIdx];
        final String option = tokens[optionIdx];
        final String value = config.getStringValue(param);

        volumes.computeIfAbsent(volumeName, val -> new HashMap<>());
        volumes.get(volumeName)
            .put(KubernetesConstants.PersistentVolumeClaimOptions.valueOf(option), value);
      }
    } catch (IndexOutOfBoundsException | IllegalArgumentException e) {
      final String message = "Invalid Persistent Volume Claim CLI parameter provided";
      LOG.log(Level.CONFIG, message);
      throw new TopologySubmissionException(message);
    }

    return volumes;
  }

  static Set<String> getConfigKeys(Config config, String keyPrefix) {
    Set<String> annotations = new HashSet<>();
    for (String s : config.getKeySet()) {
      if (s.startsWith(keyPrefix)) {
        annotations.add(s);
      }
    }
    return annotations;
  }

  private static Map<String, String> getConfigItemsByPrefix(Config config, String keyPrefix) {
    final Map<String, String> results = new HashMap<>();
    final Set<String> keys = getConfigKeys(config, keyPrefix);
    for (String s : keys) {
      String value = config.getStringValue(s);
      results.put(s.replaceFirst(keyPrefix, ""), value);
    }
    return results;
  }

  public static boolean hasContainerVolume(Config config) {
    final String name = getContainerVolumeName(config);
    final String path = getContainerVolumeMountPath(config);
    return isNotEmpty(name) && isNotEmpty(path);
  }

  private static boolean isNotEmpty(String s) {
    return s != null && !s.isEmpty();
  }
}
