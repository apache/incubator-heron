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
import java.util.Map;

import org.apache.heron.spi.common.Config;

import io.kubernetes.client.openapi.models.V1AWSElasticBlockStoreVolumeSource;
import io.kubernetes.client.openapi.models.V1HostPathVolumeSource;
import io.kubernetes.client.openapi.models.V1NFSVolumeSource;
import io.kubernetes.client.openapi.models.V1Volume;

final class Volumes {

  static final String AWS_EBS = "awsElasticBlockStore";
  static final String HOST_PATH = "hostPath";
  static final String NFS = "nfs";

  private final Map<String, VolumeFactory> volumes = new HashMap<>();

  private Volumes() {
    volumes.put(HOST_PATH, new HostPathVolumeFactory());
    volumes.put(NFS, new NfsVolumeFactory());
    volumes.put(AWS_EBS, new AwsEbsVolumeFactory());
  }

  static Volumes get() {
    return new Volumes();
  }

  V1Volume create(Config config) {
    final String volumeType = KubernetesContext.getVolumeType(config);
    if (volumes.containsKey(volumeType)) {
      return volumes.get(volumeType).create(config);
    }
    return null;
  }

  interface VolumeFactory {
    V1Volume create(Config config);
  }

  private static V1Volume newVolume(Config config) {
    final String volumeName = KubernetesContext.getVolumeName(config);
    return new V1Volume().name(volumeName);
  }

  static class HostPathVolumeFactory implements VolumeFactory {
    @Override
    public V1Volume create(Config config) {
      final V1Volume volume = newVolume(config);

      final String path = KubernetesContext.getHostPathVolumePath(config);
      final V1HostPathVolumeSource hostPathVolume =
          new V1HostPathVolumeSource()
              .path(path);
      volume.hostPath(hostPathVolume);

      return volume;
    }
  }

  static class NfsVolumeFactory implements VolumeFactory {
    @Override
    public V1Volume create(Config config) {
      final V1Volume volume = newVolume(config);

      final String path = KubernetesContext.getNfsVolumePath(config);
      final String server = KubernetesContext.getNfsServer(config);
      V1NFSVolumeSource nfsVolumeSource =
          new V1NFSVolumeSource()
              .path(path)
              .server(server);
      volume.setNfs(nfsVolumeSource);

      return volume;
    }
  }

  static class AwsEbsVolumeFactory implements VolumeFactory {
    @Override
    public V1Volume create(Config config) {
      final V1Volume volume = newVolume(config);

      final String volumeId = KubernetesContext.getAwsEbsVolumeId(config);
      final String fsType = KubernetesContext.getAwsEbsFsType(config);
      V1AWSElasticBlockStoreVolumeSource awsElasticBlockStoreVolumeSource =
          new V1AWSElasticBlockStoreVolumeSource()
              .volumeID(volumeId)
              .fsType(fsType);
      volume.setAwsElasticBlockStore(awsElasticBlockStoreVolumeSource);

      return volume;
    }
  }
}
