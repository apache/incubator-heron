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

import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Context;

public final class KubernetesContext extends Context {
  public static final String HERON_EXECUTOR_DOCKER_IMAGE = "heron.executor.docker.image";

  public static final String HERON_KUBERNETES_SCHEDULER_URI = "heron.kubernetes.scheduler.uri";

  public static final String HERON_KUBERNETES_SCHEDULER_NAMESPACE =
      "heron.kubernetes.scheduler.namespace";

  public static final String HERON_KUBERNETES_SCHEDULER_IMAGE_PULL_POLICY =
      "heron.kubernetes.scheduler.imagePullPolicy";


  public static final String HERON_KUBERNETES_VOLUME_NAME = "heron.kubernetes.volume.name";
  public static final String HERON_KUBERNETES_VOLUME_TYPE = "heron.kubernetes.volume.type";
  public static final String HERON_KUBERNETES_VOLUME_HOSTPATH_PATH =
      "heron.kubernetes.volume.hostPath.path";

  public static final String HERON_KUBERNETES_CONTAINER_VOLUME_MOUNT_NAME =
      "heron.kubernetes.container.volumeMount.name";
  public static final String HERON_KUBERNETES_CONTAINER_VOLUME_MOUNT_PATH =
      "heron.kubernetes.container.volumeMount.path";

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

  static String getVolumeType(Config config) {
    return config.getStringValue(HERON_KUBERNETES_VOLUME_TYPE);
  }

  static String getVolumeName(Config config) {
    return config.getStringValue(HERON_KUBERNETES_VOLUME_NAME);
  }

  static String getHostPathVolumePath(Config config) {
    return config.getStringValue(HERON_KUBERNETES_VOLUME_HOSTPATH_PATH);
  }

  static boolean hasVolume(Config config) {
    return isNotEmpty(getVolumeType(config));
  }

  static String getContainerVolumeName(Config config) {
    return config.getStringValue(HERON_KUBERNETES_CONTAINER_VOLUME_MOUNT_NAME);
  }

  static String getContainerVolumeMountPath(Config config) {
    return config.getStringValue(HERON_KUBERNETES_CONTAINER_VOLUME_MOUNT_PATH);
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
