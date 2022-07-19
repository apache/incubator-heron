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
import java.util.regex.Matcher;

import com.google.common.annotations.VisibleForTesting;

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


  // Pod Template ConfigMap: heron.kubernetes.[executor | manager].pod.template
  public static final String KUBERNETES_POD_TEMPLATE_LOCATION =
      "heron.kubernetes.%s.pod.template";
  public static final String KUBERNETES_POD_TEMPLATE_DISABLED =
      "heron.kubernetes.pod.template.disabled";

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
  public static final String KUBERNETES_VOLUME_FROM_CLI_DISABLED =
      "heron.kubernetes.volume.from.cli.disabled";
  // heron.kubernetes.[executor | manager].volumes.persistentVolumeClaim.VOLUME_NAME.OPTION=VALUE
  public static final String KUBERNETES_VOLUME_CLAIM_PREFIX =
      "heron.kubernetes.%s.volumes.persistentVolumeClaim.";
  // heron.kubernetes.[executor | manager].volumes.emptyDir.VOLUME_NAME.OPTION=VALUE
  public static final String KUBERNETES_VOLUME_EMPTYDIR_PREFIX =
      "heron.kubernetes.%s.volumes.emptyDir.";
  // heron.kubernetes.[executor | manager].volumes.hostPath.VOLUME_NAME.OPTION=VALUE
  public static final String KUBERNETES_VOLUME_HOSTPATH_PREFIX =
      "heron.kubernetes.%s.volumes.hostPath.";
  // heron.kubernetes.[executor | manager].volumes.nfs.VOLUME_NAME.OPTION=VALUE
  public static final String KUBERNETES_VOLUME_NFS_PREFIX =
      "heron.kubernetes.%s.volumes.nfs.";
  // heron.kubernetes.[executor | manager].limits.OPTION=VALUE
  public static final String KUBERNETES_RESOURCE_LIMITS_PREFIX =
      "heron.kubernetes.%s.limits.";
  // heron.kubernetes.[executor | manager].requests.OPTION=VALUE
  public static final String KUBERNETES_RESOURCE_REQUESTS_PREFIX =
      "heron.kubernetes.%s.requests.";

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

  static String getContainerVolumeName(Config config) {
    return config.getStringValue(KUBERNETES_CONTAINER_VOLUME_MOUNT_NAME);
  }

  static String getContainerVolumeMountPath(Config config) {
    return config.getStringValue(KUBERNETES_CONTAINER_VOLUME_MOUNT_PATH);
  }

  public static String getPodTemplateConfigMapName(Config config, boolean isExecutor) {
    final String key = String.format(KUBERNETES_POD_TEMPLATE_LOCATION,
        isExecutor ? KubernetesConstants.EXECUTOR_NAME : KubernetesConstants.MANAGER_NAME);
    return config.getStringValue(key);
  }

  public static boolean getPodTemplateDisabled(Config config) {
    final String disabled = config.getStringValue(KUBERNETES_POD_TEMPLATE_DISABLED);
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

  public static Map<String, String> getResourceLimits(Config config, boolean isExecutor) {
    final String key = String.format(KUBERNETES_RESOURCE_LIMITS_PREFIX,
        isExecutor ? KubernetesConstants.EXECUTOR_NAME : KubernetesConstants.MANAGER_NAME);
    return getConfigItemsByPrefix(config, key);
  }

  public static Map<String, String> getResourceRequests(Config config, boolean isExecutor) {
    final String key = String.format(KUBERNETES_RESOURCE_REQUESTS_PREFIX,
        isExecutor ? KubernetesConstants.EXECUTOR_NAME : KubernetesConstants.MANAGER_NAME);
    return getConfigItemsByPrefix(config, key);
  }

  public static boolean getVolumesFromCLIDisabled(Config config) {
    final String disabled = config.getStringValue(KUBERNETES_VOLUME_FROM_CLI_DISABLED);
    return "true".equalsIgnoreCase(disabled);
  }

  /**
   * Collects parameters form the <code>CLI</code> and generates a mapping between <code>Volumes</code>
   * and their configuration <code>key-value</code> pairs.
   * @param config Contains the configuration options collected from the <code>CLI</code>.
   * @param prefix Configuration key to lookup for options.
   * @param isExecutor Flag used to switch CLI commands for the <code>Executor</code> and <code>Manager</code>.
   * @return A mapping between <code>Volumes</code> and their configuration <code>key-value</code> pairs.
   * Will return an empty list if there are no Volume Claim Templates to be generated.
   */
  @VisibleForTesting
  protected static Map<String, Map<KubernetesConstants.VolumeConfigKeys, String>>
      getVolumeConfigs(final Config config, final String prefix, final boolean isExecutor) {
    final Logger LOG = Logger.getLogger(KubernetesShim.class.getName());

    final String prefixKey = String.format(prefix,
        isExecutor ? KubernetesConstants.EXECUTOR_NAME : KubernetesConstants.MANAGER_NAME);
    final Set<String> completeConfigParam = getConfigKeys(config, prefixKey);
    final int prefixLength = prefixKey.length();
    final int volumeNameIdx = 0;
    final int optionIdx = 1;
    final Matcher matcher = KubernetesConstants.VALID_LOWERCASE_RFC_1123_REGEX.matcher("");

    final Map<String, Map<KubernetesConstants.VolumeConfigKeys, String>> volumes = new HashMap<>();

    try {
      for (String param : completeConfigParam) {
        final String[] tokens = param.substring(prefixLength).split("\\.");
        final String volumeName = tokens[volumeNameIdx];
        final KubernetesConstants.VolumeConfigKeys key =
            KubernetesConstants.VolumeConfigKeys.valueOf(tokens[optionIdx]);
        final String value = config.getStringValue(param);

        Map<KubernetesConstants.VolumeConfigKeys, String> volume = volumes.get(volumeName);
        if (volume == null) {
          // Validate new Volume Names.
          if (!matcher.reset(volumeName).matches()) {
            throw new TopologySubmissionException(
                String.format("Volume name `%s` does not match lowercase RFC-1123 pattern",
                    volumeName));
          }
          volume = new HashMap<>();
          volumes.put(volumeName, volume);
        }

        volume.put(key, value);
      }
    } catch (IndexOutOfBoundsException | IllegalArgumentException e) {
      final String message = "Invalid Volume configuration option provided on CLI";
      LOG.log(Level.CONFIG, message);
      throw new TopologySubmissionException(message);
    }

    // All Volumes must contain a path.
    for (Map.Entry<String, Map<KubernetesConstants.VolumeConfigKeys, String>> volume
        : volumes.entrySet()) {
      final String path = volume.getValue().get(KubernetesConstants.VolumeConfigKeys.path);
      if (path == null || path.isEmpty()) {
        throw new TopologySubmissionException(String.format("Volume `%s`: All Volumes require a"
            + " 'path'.", volume.getKey()));
      }
    }

    // Check to see if functionality is disabled.
    if (KubernetesContext.getVolumesFromCLIDisabled(config) && !volumes.isEmpty()) {
      final String message = "Configuring Volumes from the CLI is disabled.";
      LOG.log(Level.WARNING, message);
      throw new TopologySubmissionException(message);
    }

    return volumes;
  }

  /**
   * Collects parameters form the <code>CLI</code> and validates options for <code>PVC</code>s.
   * @param config Contains the configuration options collected from the <code>CLI</code>.
   * @param isExecutor Flag used to collect CLI commands for the <code>Executor</code> and <code>Manager</code>.
   * @return A mapping between <code>Volumes</code> and their configuration <code>key-value</code> pairs.
   * Will return an empty list if there are no Volume Claim Templates to be generated.
   */
  public static Map<String, Map<KubernetesConstants.VolumeConfigKeys, String>>
      getVolumeClaimTemplates(final Config config, final boolean isExecutor) {
    final Matcher matcher = KubernetesConstants.VALID_LOWERCASE_RFC_1123_REGEX.matcher("");

    final Map<String, Map<KubernetesConstants.VolumeConfigKeys, String>> volumes =
        getVolumeConfigs(config, KubernetesContext.KUBERNETES_VOLUME_CLAIM_PREFIX, isExecutor);

    for (Map.Entry<String, Map<KubernetesConstants.VolumeConfigKeys, String>> volume
        : volumes.entrySet()) {

      // Claim name is required.
      if (!volume.getValue().containsKey(KubernetesConstants.VolumeConfigKeys.claimName)) {
        throw new TopologySubmissionException(String.format("Volume `%s`: Persistent Volume"
            + " Claims require a `claimName`.", volume.getKey()));
      }

      for (Map.Entry<KubernetesConstants.VolumeConfigKeys, String> volumeConfig
          : volume.getValue().entrySet()) {
        final KubernetesConstants.VolumeConfigKeys key = volumeConfig.getKey();
        final String value = volumeConfig.getValue();

        switch (key) {
          case claimName:
            // Claim names which are not OnDemand should be lowercase RFC-1123.
            if (!matcher.reset(value).matches()
                && !KubernetesConstants.LABEL_ON_DEMAND.equalsIgnoreCase(value)) {
              throw new TopologySubmissionException(String.format("Volume `%s`: `claimName` does"
                  + " not match lowercase RFC-1123 pattern", volume.getKey()));
            }
            break;
          case storageClassName:
            if (!matcher.reset(value).matches()) {
              throw new TopologySubmissionException(String.format("Volume `%s`: `storageClassName`"
                  + " does not match lowercase RFC-1123 pattern", volume.getKey()));
            }
            break;
          case sizeLimit: case accessModes: case volumeMode: case readOnly: case path: case subPath:
            break;
          default:
            throw new TopologySubmissionException(String.format("Volume `%s`: Invalid Persistent"
                + " Volume Claim type option for '%s'", volume.getKey(), key));
        }
      }
    }

    return volumes;
  }

  /**
   * Collects parameters form the <code>CLI</code> and validates options for <code>Empty Directory</code>s.
   * @param config Contains the configuration options collected from the <code>CLI</code>.
   * @param isExecutor Flag used to collect CLI commands for the <code>Executor</code> and <code>Manager</code>.
   * @return A mapping between <code>Volumes</code> and their configuration <code>key-value</code> pairs.
   * Will return an empty list if there are no Volume Claim Templates to be generated.
   */
  public static Map<String, Map<KubernetesConstants.VolumeConfigKeys, String>>
      getVolumeEmptyDir(final Config config, final boolean isExecutor) {
    final Map<String, Map<KubernetesConstants.VolumeConfigKeys, String>> volumes =
        getVolumeConfigs(config, KubernetesContext.KUBERNETES_VOLUME_EMPTYDIR_PREFIX, isExecutor);

    for (Map.Entry<String, Map<KubernetesConstants.VolumeConfigKeys, String>> volume
        : volumes.entrySet()) {
      final String medium = volume.getValue().get(KubernetesConstants.VolumeConfigKeys.medium);

      if (medium != null && !medium.isEmpty() && !"Memory".equals(medium)) {
        throw new TopologySubmissionException(String.format("Volume `%s`: Empty Directory"
            + " 'medium' must be 'Memory' or empty.", volume.getKey()));
      }
      for (Map.Entry<KubernetesConstants.VolumeConfigKeys, String> volumeConfig
          : volume.getValue().entrySet()) {
        final KubernetesConstants.VolumeConfigKeys key = volumeConfig.getKey();

        switch (key) {
          case sizeLimit: case medium: case readOnly: case path: case subPath:
            break;
          default:
            throw new TopologySubmissionException(String.format("Volume `%s`: Invalid Empty"
                + " Directory type option for '%s'", volume.getKey(), key));
        }
      }
    }

    return volumes;
  }

  /**
   * Collects parameters form the <code>CLI</code> and validates options for <code>Host Path</code>s.
   * @param config Contains the configuration options collected from the <code>CLI</code>.
   * @param isExecutor Flag used to collect CLI commands for the <code>Executor</code> and <code>Manager</code>.
   * @return A mapping between <code>Volumes</code> and their configuration <code>key-value</code> pairs.
   * Will return an empty list if there are no Volume Claim Templates to be generated.
   */
  public static Map<String, Map<KubernetesConstants.VolumeConfigKeys, String>>
      getVolumeHostPath(final Config config, final boolean isExecutor) {
    final Map<String, Map<KubernetesConstants.VolumeConfigKeys, String>> volumes =
        getVolumeConfigs(config, KubernetesContext.KUBERNETES_VOLUME_HOSTPATH_PREFIX, isExecutor);

    for (Map.Entry<String, Map<KubernetesConstants.VolumeConfigKeys, String>> volume
        : volumes.entrySet()) {
      final String type = volume.getValue().get(KubernetesConstants.VolumeConfigKeys.type);
      if (type != null && !KubernetesConstants.VALID_VOLUME_HOSTPATH_TYPES.contains(type)) {
        throw new TopologySubmissionException(String.format("Volume `%s`: Host Path"
            + " 'type' of '%s' is invalid.", volume.getKey(), type));
      }
      final String hostOnPath =
          volume.getValue().get(KubernetesConstants.VolumeConfigKeys.pathOnHost);
      if (hostOnPath == null || hostOnPath.isEmpty()) {
        throw new TopologySubmissionException(String.format("Volume `%s`: Host Path  requires a"
            + " path on the host.", volume.getKey()));
      }

      for (Map.Entry<KubernetesConstants.VolumeConfigKeys, String> volumeConfig
          : volume.getValue().entrySet()) {
        final KubernetesConstants.VolumeConfigKeys key = volumeConfig.getKey();

        switch (key) {
          case type: case pathOnHost: case readOnly: case path: case subPath:
            break;
          default:
            throw new TopologySubmissionException(String.format("Volume `%s`: Invalid Host Path"
                + " option for '%s'", volume.getKey(), key));
        }
      }
    }

    return volumes;
  }

  /**
   * Collects parameters form the <code>CLI</code> and validates options for <code>NFS</code>s.
   * @param config Contains the configuration options collected from the <code>CLI</code>.
   * @param isExecutor Flag used to collect CLI commands for the <code>Executor</code> and <code>Manager</code>.
   * @return A mapping between <code>Volumes</code> and their configuration <code>key-value</code> pairs.
   * Will return an empty list if there are no Volume Claim Templates to be generated.
   */
  public static Map<String, Map<KubernetesConstants.VolumeConfigKeys, String>>
      getVolumeNFS(final Config config, final boolean isExecutor) {
    final Map<String, Map<KubernetesConstants.VolumeConfigKeys, String>> volumes =
        getVolumeConfigs(config, KubernetesContext.KUBERNETES_VOLUME_NFS_PREFIX, isExecutor);

    for (Map.Entry<String, Map<KubernetesConstants.VolumeConfigKeys, String>> volume
        : volumes.entrySet()) {
      final String server = volume.getValue().get(KubernetesConstants.VolumeConfigKeys.server);
      if (server == null || server.isEmpty()) {
        throw new TopologySubmissionException(String.format("Volume `%s`: `NFS` volumes require a"
            + " `server` to be specified", volume.getKey()));
      }
      final String hostOnNFS =
          volume.getValue().get(KubernetesConstants.VolumeConfigKeys.pathOnNFS);
      if (hostOnNFS == null || hostOnNFS.isEmpty()) {
        throw new TopologySubmissionException(String.format("Volume `%s`: NFS requires a path on"
            + " the NFS server.", volume.getKey()));
      }

      for (Map.Entry<KubernetesConstants.VolumeConfigKeys, String> volumeConfig
          : volume.getValue().entrySet()) {
        final KubernetesConstants.VolumeConfigKeys key = volumeConfig.getKey();

        switch (key) {
          case server: case pathOnNFS: case readOnly: case path: case subPath:
            break;
          default:
            throw new TopologySubmissionException(String.format("Volume `%s`: Invalid NFS option"
                + " for '%s'", volume.getKey(), key));
        }
      }
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
