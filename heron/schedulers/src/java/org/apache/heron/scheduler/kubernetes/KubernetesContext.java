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

import org.apache.heron.spi.common.Config;
import org.apache.heron.spi.common.Context;

public final class KubernetesContext extends Context {
  public static final String HERON_EXECUTOR_DOCKER_IMAGE = "heron.executor.docker.image";

  public static final String HERON_KUBERNETES_SCHEDULER_URI = "heron.kubernetes.scheduler.uri";

  public static final String HERON_KUBERNETES_SCHEDULER_NAMESPACE =
      "heron.kubernetes.scheduler.namespace";

  public static final String HERON_KUBERNETES_SCHEDULER_IMAGE_PULL_POLICY =
      "heron.kubernetes.scheduler.imagePullPolicy";


  public static final String HERON_KUBERNETES_VOLUME_NAME = "heron.kubernetes.volume.name";
  public static final String HERON_KUBERNETES_VOLUME_TYPE = "heron.kubernetes.volume.type";


  // HostPath volume keys
  // https://kubernetes.io/docs/concepts/storage/volumes/#hostpath
  public static final String HERON_KUBERNETES_VOLUME_HOSTPATH_PATH =
      "heron.kubernetes.volume.hostPath.path";

  // nfs volume keys
  // https://kubernetes.io/docs/concepts/storage/volumes/#nfs
  public static final String HERON_KUBERNETES_VOLUME_NFS_PATH =
      "heron.kubernetes.volume.nfs.path";
  public static final String HERON_KUBERNETES_VOLUME_NFS_SERVER =
      "heron.kubernetes.volume.nfs.server";

  // awsElasticBlockStore volume keys
  // https://kubernetes.io/docs/concepts/storage/volumes/#awselasticblockstore
  public static final String HERON_KUBERNETES_VOLUME_AWS_EBS_VOLUME_ID =
      "heron.kubernetes.volume.awsElasticBlockStore.volumeID";
  public static final String HERON_KUBERNETES_VOLUME_AWS_EBS_FS_TYPE =
      "heron.kubernetes.volume.awsElasticBlockStore.fsType";

  // container mount volume mount keys
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

  static String getNfsVolumePath(Config config) {
    return config.getStringValue(HERON_KUBERNETES_VOLUME_NFS_PATH);
  }

  static String getNfsServer(Config config) {
    return config.getStringValue(HERON_KUBERNETES_VOLUME_NFS_SERVER);
  }

  static String getAwsEbsVolumeId(Config config) {
    return config.getStringValue(HERON_KUBERNETES_VOLUME_AWS_EBS_VOLUME_ID);
  }

  static String getAwsEbsFsType(Config config) {
    return config.getStringValue(HERON_KUBERNETES_VOLUME_AWS_EBS_FS_TYPE);
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
