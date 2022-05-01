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

import io.kubernetes.client.custom.Quantity;
import io.kubernetes.client.openapi.models.V1Volume;
import io.kubernetes.client.openapi.models.V1VolumeBuilder;

final class Volumes {

  public enum VolumeType {
    EmptyDir,
    HostPath,
    NetworkFileSystem,
    PersistentVolumeClaim,
    VolumeMount
  }
  private final Map<VolumeType, VolumeFactory> volumes = new HashMap<>();

  private Volumes() {
    volumes.put(VolumeType.EmptyDir, new EmptyDirVolumeFactory());
    volumes.put(VolumeType.HostPath, new HostPathVolumeFactory());
    volumes.put(VolumeType.NetworkFileSystem, new NetworkFileSystemVolumeFactory());
  }

  static Volumes get() {
    return new Volumes();
  }

  /**
   * Creates <code>Generic</code>, <code>Empty Directory</code>, <code>Host Path</code>, and
   * <code>Network File System</code> volumes.
   * @param volumeType One of the supported volume types.
   * @param volumeName The name of the volume to generate.
   * @param configs A map of configurations.
   * @return Fully configured <code>V1Volume</code>.
   */
  V1Volume create(VolumeType volumeType, String volumeName,
                  Map<KubernetesConstants.VolumeConfigKeys, String> configs) {
    if (volumes.containsKey(volumeType)) {
      return volumes.get(volumeType).create(volumeName, configs);
    }
    return null;
  }

  interface VolumeFactory {
    V1Volume create(String volumeName, Map<KubernetesConstants.VolumeConfigKeys, String> configs);
  }

  static class EmptyDirVolumeFactory implements VolumeFactory {

    /**
     * Generates an <code>Empty Directory</code> <code>V1 Volume</code>.
     * @param volumeName The name of the volume to generate.
     * @param configs    A map of configurations.
     * @return A fully configured <code>Empty Directory</code> volume.
     */
    @Override
    public V1Volume create(String volumeName,
                           Map<KubernetesConstants.VolumeConfigKeys, String> configs) {
      final V1Volume volume = new V1VolumeBuilder()
          .withName(volumeName)
          .withNewEmptyDir()
          .endEmptyDir()
          .build();

      for (Map.Entry<KubernetesConstants.VolumeConfigKeys, String> config : configs.entrySet()) {
        switch(config.getKey()) {
          case medium:
            volume.getEmptyDir().medium(config.getValue());
            break;
          case sizeLimit:
            volume.getEmptyDir().sizeLimit(new Quantity(config.getValue()));
            break;
          default:
            break;
        }
      }

      return volume;
    }
  }

  static class HostPathVolumeFactory implements VolumeFactory {

    /**
     * Generates a <code>Host Path</code> <code>V1 Volume</code>.
     * @param volumeName The name of the volume to generate.
     * @param configs    A map of configurations.
     * @return A fully configured <code>Host Path</code> volume.
     */
    @Override
    public V1Volume create(String volumeName,
                           Map<KubernetesConstants.VolumeConfigKeys, String> configs) {
      final V1Volume volume = new V1VolumeBuilder()
          .withName(volumeName)
          .withNewHostPath()
          .endHostPath()
          .build();

      for (Map.Entry<KubernetesConstants.VolumeConfigKeys, String> config : configs.entrySet()) {
        switch (config.getKey()) {
          case type:
            volume.getHostPath().setType(config.getValue());
            break;
          case pathOnHost:
            volume.getHostPath().setPath(config.getValue());
            break;
          default:
            break;
        }
      }
      return volume;
    }
  }

  static class NetworkFileSystemVolumeFactory implements VolumeFactory {

    /**
     * Generates a <code>Network File System</code> <code>V1 Volume</code>.
     *
     * @param volumeName The name of the volume to generate.
     * @param configs    A map of configurations.
     * @return A fully configured <code>Network File System</code> volume.
     */
    @Override
    public V1Volume create(String volumeName,
                           Map<KubernetesConstants.VolumeConfigKeys, String> configs) {
      final V1Volume volume = new V1VolumeBuilder()
          .withName(volumeName)
          .withNewNfs()
          .endNfs()
          .build();

      for (Map.Entry<KubernetesConstants.VolumeConfigKeys, String> config : configs.entrySet()) {
        switch (config.getKey()) {
          case server:
            volume.getNfs().setServer(config.getValue());
            break;
          case pathOnNFS:
            volume.getNfs().setPath(config.getValue());
            break;
          case readOnly:
            volume.getNfs().setReadOnly(Boolean.parseBoolean(config.getValue()));
            break;
          default:
            break;
        }
      }
      return volume;
    }
  }
}
