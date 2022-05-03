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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import io.kubernetes.client.custom.Quantity;
import io.kubernetes.client.openapi.models.V1PersistentVolumeClaim;
import io.kubernetes.client.openapi.models.V1PersistentVolumeClaimBuilder;
import io.kubernetes.client.openapi.models.V1ResourceRequirements;
import io.kubernetes.client.openapi.models.V1Volume;
import io.kubernetes.client.openapi.models.V1VolumeBuilder;
import io.kubernetes.client.openapi.models.V1VolumeMount;
import io.kubernetes.client.openapi.models.V1VolumeMountBuilder;

final class Volumes {

  public enum VolumeType {
    EmptyDir,
    HostPath,
    NetworkFileSystem
  }
  private final Map<VolumeType, IVolumeFactory> volumes = new HashMap<>();

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
  V1Volume createVolume(VolumeType volumeType, String volumeName,
                        Map<KubernetesConstants.VolumeConfigKeys, String> configs) {
    if (volumes.containsKey(volumeType)) {
      return volumes.get(volumeType).create(volumeName, configs);
    }
    return null;
  }

  /**
   * Generates a <code>Volume Mount</code> from specifications.
   * @param volumeName Name of the <code>Volume</code>.
   * @param configs Mapping of <code>Volume</code> option <code>key-value</code> configuration pairs.
   * @return A configured <code>V1VolumeMount</code>.
   */
  V1VolumeMount createMount(String volumeName,
                            Map<KubernetesConstants.VolumeConfigKeys, String> configs) {
    final V1VolumeMount volumeMount = new V1VolumeMountBuilder()
        .withName(volumeName)
        .build();
    for (Map.Entry<KubernetesConstants.VolumeConfigKeys, String> config : configs.entrySet()) {
      switch (config.getKey()) {
        case path:
          volumeMount.mountPath(config.getValue());
          break;
        case subPath:
          volumeMount.subPath(config.getValue());
          break;
        case readOnly:
          volumeMount.readOnly(Boolean.parseBoolean(config.getValue()));
          break;
        default:
          break;
      }
    }
    return volumeMount;
  }

  /**
   * Generates <code>Persistent Volume Claims Templates</code> from a mapping of <code>Volumes</code>
   * to <code>key-value</code> pairs of configuration options and values.
   * @param claimName Name to be assigned to <code>Persistent Volume Claims Template</code>.
   * @param labels Labels to be attached to the <code>Persistent Volume Claims Template</code>.
   * @param configs <code>Volume</code> to configuration <code>key-value</code> mappings.
   * @return Fully populated dynamically backed <code>Persistent Volume Claims</code>.
   */
  V1PersistentVolumeClaim createPersistentVolumeClaim(String claimName, Map<String, String> labels,
                                        Map<KubernetesConstants.VolumeConfigKeys, String> configs) {
    V1PersistentVolumeClaim claim = new V1PersistentVolumeClaimBuilder()
        .withNewMetadata()
          .withName(claimName)
          .withLabels(labels)
        .endMetadata()
        .withNewSpec()
          .withStorageClassName("")
        .endSpec()
        .build();

    // Populate PVC options.
    for (Map.Entry<KubernetesConstants.VolumeConfigKeys, String> option : configs.entrySet()) {
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
        default:
          break;
      }
    }
    return claim;
  }

  /**
   * Generates a <code>Volume</code> with a <code>Persistent Volume Claim</code> inserted.
   * @param claimName Name of the <code>Persistent Volume Claim</code>.
   * @param volumeName Name of the <code>Volume</code> to place the <code>Persistent Volume Claim</code> in.
   * @return Fully configured <code>Volume</code> with <code>Persistent Volume Claim</code> in it.
   */
  V1Volume createPersistentVolumeClaim(String claimName, String volumeName) {
    return new V1VolumeBuilder()
        .withName(volumeName)
        .withNewPersistentVolumeClaim()
          .withClaimName(claimName)
        .endPersistentVolumeClaim()
        .build();
  }

  interface IVolumeFactory {
    V1Volume create(String volumeName, Map<KubernetesConstants.VolumeConfigKeys, String> configs);
  }

  static class EmptyDirVolumeFactory implements IVolumeFactory {

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

  static class HostPathVolumeFactory implements IVolumeFactory {

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

  static class NetworkFileSystemVolumeFactory implements IVolumeFactory {

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
