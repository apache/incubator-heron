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
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableMap;

import org.junit.Assert;
import org.junit.Test;

import org.apache.heron.common.basics.Pair;

import io.kubernetes.client.custom.Quantity;
import io.kubernetes.client.openapi.models.V1PersistentVolumeClaim;
import io.kubernetes.client.openapi.models.V1PersistentVolumeClaimBuilder;
import io.kubernetes.client.openapi.models.V1Volume;
import io.kubernetes.client.openapi.models.V1VolumeBuilder;
import io.kubernetes.client.openapi.models.V1VolumeMount;
import io.kubernetes.client.openapi.models.V1VolumeMountBuilder;

public class VolumesTests {

  @Test
  public void testEmptyDir() {
    final String volumeName = "volume-name-empty-dir";
    final String medium = "Memory";
    final String sizeLimit = "1Gi";
    final String path = "/path/to/mount";
    final String subPath = "/sub/path/to/mount";
    final Map<KubernetesConstants.VolumeConfigKeys, String> config =
        ImmutableMap.<KubernetesConstants.VolumeConfigKeys, String>builder()
            .put(KubernetesConstants.VolumeConfigKeys.sizeLimit, sizeLimit)
            .put(KubernetesConstants.VolumeConfigKeys.medium, medium)
            .put(KubernetesConstants.VolumeConfigKeys.path, path)
            .put(KubernetesConstants.VolumeConfigKeys.subPath, subPath)
            .build();
    final V1Volume expectedVolume = new V1VolumeBuilder()
        .withName(volumeName)
        .withNewEmptyDir()
          .withMedium(medium)
          .withNewSizeLimit(sizeLimit)
        .endEmptyDir()
        .build();

    final V1Volume actualVolume = Volumes.get()
        .createVolume(Volumes.VolumeType.EmptyDir, volumeName, config);

    Assert.assertEquals("Volume Factory Empty Directory", expectedVolume, actualVolume);
  }

  @Test
  public void testHostPath() {
    final String volumeName = "volume-name-host-path";
    final String type = "DirectoryOrCreate";
    final String pathOnHost = "path.on.host";
    final String path = "/path/to/mount";
    final String subPath = "/sub/path/to/mount";
    final Map<KubernetesConstants.VolumeConfigKeys, String> config =
        ImmutableMap.<KubernetesConstants.VolumeConfigKeys, String>builder()
            .put(KubernetesConstants.VolumeConfigKeys.type, type)
            .put(KubernetesConstants.VolumeConfigKeys.pathOnHost, pathOnHost)
            .put(KubernetesConstants.VolumeConfigKeys.path, path)
            .put(KubernetesConstants.VolumeConfigKeys.subPath, subPath)
            .build();
    final V1Volume expectedVolume = new V1VolumeBuilder()
        .withName(volumeName)
        .withNewHostPath()
          .withNewType(type)
          .withNewPath(pathOnHost)
        .endHostPath()
        .build();

    final V1Volume actualVolume = Volumes.get()
        .createVolume(Volumes.VolumeType.HostPath, volumeName, config);

    Assert.assertEquals("Volume Factory Host Path", expectedVolume, actualVolume);
  }

  @Test
  public void testNetworkFileSystem() {
    final String volumeName = "volume-name-nfs";
    final String server = "nfs.server.address";
    final String pathOnNFS = "path.on.host";
    final String readOnly = "true";
    final String path = "/path/to/mount";
    final String subPath = "/sub/path/to/mount";
    final Map<KubernetesConstants.VolumeConfigKeys, String> config =
        ImmutableMap.<KubernetesConstants.VolumeConfigKeys, String>builder()
            .put(KubernetesConstants.VolumeConfigKeys.server, server)
            .put(KubernetesConstants.VolumeConfigKeys.readOnly, readOnly)
            .put(KubernetesConstants.VolumeConfigKeys.pathOnNFS, pathOnNFS)
            .put(KubernetesConstants.VolumeConfigKeys.path, path)
            .put(KubernetesConstants.VolumeConfigKeys.subPath, subPath)
            .build();
    final V1Volume expectedVolume = new V1VolumeBuilder()
        .withName(volumeName)
        .withNewNfs()
          .withServer(server)
          .withPath(pathOnNFS)
          .withReadOnly(Boolean.parseBoolean(readOnly))
        .endNfs()
        .build();

    final V1Volume actualVolume = Volumes.get()
        .createVolume(Volumes.VolumeType.NetworkFileSystem, volumeName, config);

    Assert.assertEquals("Volume Factory Network File System", expectedVolume, actualVolume);
  }

  @Test
  public void testVolumeMount() {
    final String volumeNamePVC = "volume-name-pvc";
    final String volumeNameHostPath = "volume-name-host-path";
    final String volumeNameEmptyDir = "volume-name-empty-dir";
    final String volumeNameNFS = "volume-name-nfs";
    final String value = "inserted-value";

    // Test case container.
    // Input: [0] volume name, [1] volume options
    // Output: The expected <V1VolumeMount>.
    final List<KubernetesUtils.TestTuple<
        Pair<String, Map<KubernetesConstants.VolumeConfigKeys, String>>, V1VolumeMount>> testCases =
        new LinkedList<>();

    // PVC.
    final Map<KubernetesConstants.VolumeConfigKeys, String> configPVC =
        ImmutableMap.<KubernetesConstants.VolumeConfigKeys, String>builder()
            .put(KubernetesConstants.VolumeConfigKeys.claimName, value)
            .put(KubernetesConstants.VolumeConfigKeys.storageClassName, value)
            .put(KubernetesConstants.VolumeConfigKeys.sizeLimit, value)
            .put(KubernetesConstants.VolumeConfigKeys.accessModes, value)
            .put(KubernetesConstants.VolumeConfigKeys.volumeMode, value)
            .put(KubernetesConstants.VolumeConfigKeys.path, value)
            .put(KubernetesConstants.VolumeConfigKeys.subPath, value)
            .put(KubernetesConstants.VolumeConfigKeys.readOnly, "true")
            .build();
    final V1VolumeMount volumeMountPVC = new V1VolumeMountBuilder()
        .withName(volumeNamePVC)
        .withMountPath(value)
        .withSubPath(value)
        .withReadOnly(true)
        .build();
    testCases.add(new KubernetesUtils.TestTuple<>("PVC volume mount",
        new Pair<>(volumeNamePVC, configPVC), volumeMountPVC));

    // Host Path.
    final Map<KubernetesConstants.VolumeConfigKeys, String> configHostPath =
        ImmutableMap.<KubernetesConstants.VolumeConfigKeys, String>builder()
            .put(KubernetesConstants.VolumeConfigKeys.type, "DirectoryOrCreate")
            .put(KubernetesConstants.VolumeConfigKeys.pathOnHost, value)
            .put(KubernetesConstants.VolumeConfigKeys.path, value)
            .put(KubernetesConstants.VolumeConfigKeys.subPath, value)
            .put(KubernetesConstants.VolumeConfigKeys.readOnly, "true")
            .build();
    final V1VolumeMount volumeMountHostPath = new V1VolumeMountBuilder()
        .withName(volumeNameHostPath)
        .withMountPath(value)
        .withSubPath(value)
        .withReadOnly(true)
        .build();
    testCases.add(new KubernetesUtils.TestTuple<>("Host Path volume mount",
        new Pair<>(volumeNameHostPath, configHostPath), volumeMountHostPath));

    // Empty Dir.
    final Map<KubernetesConstants.VolumeConfigKeys, String> configEmptyDir =
        ImmutableMap.<KubernetesConstants.VolumeConfigKeys, String>builder()
            .put(KubernetesConstants.VolumeConfigKeys.sizeLimit, value)
            .put(KubernetesConstants.VolumeConfigKeys.medium, "Memory")
            .put(KubernetesConstants.VolumeConfigKeys.path, value)
            .put(KubernetesConstants.VolumeConfigKeys.subPath, value)
            .put(KubernetesConstants.VolumeConfigKeys.readOnly, "true")
            .build();
    final V1VolumeMount volumeMountEmptyDir = new V1VolumeMountBuilder()
        .withName(volumeNameEmptyDir)
        .withMountPath(value)
        .withSubPath(value)
        .withReadOnly(true)
        .build();
    testCases.add(new KubernetesUtils.TestTuple<>("Empty Dir volume mount",
        new Pair<>(volumeNameEmptyDir, configEmptyDir), volumeMountEmptyDir));

    // NFS.
    final Map<KubernetesConstants.VolumeConfigKeys, String> configNFS =
        ImmutableMap.<KubernetesConstants.VolumeConfigKeys, String>builder()
            .put(KubernetesConstants.VolumeConfigKeys.server, "nfs.server.address")
            .put(KubernetesConstants.VolumeConfigKeys.readOnly, "true")
            .put(KubernetesConstants.VolumeConfigKeys.pathOnNFS, value)
            .put(KubernetesConstants.VolumeConfigKeys.path, value)
            .put(KubernetesConstants.VolumeConfigKeys.subPath, value)
            .build();
    final V1VolumeMount volumeMountNFS = new V1VolumeMountBuilder()
        .withName(volumeNameNFS)
        .withMountPath(value)
        .withSubPath(value)
        .withReadOnly(true)
        .build();
    testCases.add(new KubernetesUtils.TestTuple<>("NFS volume mount",
        new Pair<>(volumeNameNFS, configNFS), volumeMountNFS));

    // Test loop.
    for (KubernetesUtils.TestTuple<Pair<String, Map<KubernetesConstants.VolumeConfigKeys, String>>,
             V1VolumeMount> testCase : testCases) {
      V1VolumeMount actual = Volumes.get().createMount(testCase.input.first, testCase.input.second);
      Assert.assertEquals(testCase.description, testCase.expected, actual);
    }
  }

  @Test
  public void testPersistentVolumeClaim() {
    final String topologyName = "topology-name";
    final String volumeNameOne = "volume-name-one";
    final String volumeNameTwo = "volume-name-two";
    final String volumeNameStatic = "volume-name-static";
    final String claimNameOne = "OnDemand";
    final String claimNameTwo = "claim-name-two";
    final String claimNameStatic = "OnDEmaND";
    final String storageClassName = "storage-class-name";
    final String sizeLimit = "555Gi";
    final String accessModesList = "ReadWriteOnce,ReadOnlyMany,ReadWriteMany";
    final String accessModes = "ReadOnlyMany";
    final String volumeMode = "VolumeMode";
    final String path = "/path/to/mount/";
    final String subPath = "/sub/path/to/mount/";
    final Map<String, String> labels = KubernetesShim.getPersistentVolumeClaimLabels(topologyName);

    final Map<KubernetesConstants.VolumeConfigKeys, String> volOneConfig =
        new HashMap<KubernetesConstants.VolumeConfigKeys, String>() {
        {
          put(KubernetesConstants.VolumeConfigKeys.claimName, claimNameOne);
          put(KubernetesConstants.VolumeConfigKeys.storageClassName, storageClassName);
          put(KubernetesConstants.VolumeConfigKeys.sizeLimit, sizeLimit);
          put(KubernetesConstants.VolumeConfigKeys.accessModes, accessModesList);
          put(KubernetesConstants.VolumeConfigKeys.volumeMode, volumeMode);
          put(KubernetesConstants.VolumeConfigKeys.path, path);
        }
      };

    final Map<KubernetesConstants.VolumeConfigKeys, String> volTwoConfig =
        new HashMap<KubernetesConstants.VolumeConfigKeys, String>() {
          {
            put(KubernetesConstants.VolumeConfigKeys.claimName, claimNameTwo);
            put(KubernetesConstants.VolumeConfigKeys.storageClassName, storageClassName);
            put(KubernetesConstants.VolumeConfigKeys.sizeLimit, sizeLimit);
            put(KubernetesConstants.VolumeConfigKeys.accessModes, accessModesList);
            put(KubernetesConstants.VolumeConfigKeys.volumeMode, volumeMode);
            put(KubernetesConstants.VolumeConfigKeys.path, path);
            put(KubernetesConstants.VolumeConfigKeys.subPath, subPath);
          }
        };

    final Map<KubernetesConstants.VolumeConfigKeys, String> volStaticConfig =
        new HashMap<KubernetesConstants.VolumeConfigKeys, String>() {
          {
            put(KubernetesConstants.VolumeConfigKeys.claimName, claimNameStatic);
            put(KubernetesConstants.VolumeConfigKeys.sizeLimit, sizeLimit);
            put(KubernetesConstants.VolumeConfigKeys.accessModes, accessModes);
            put(KubernetesConstants.VolumeConfigKeys.volumeMode, volumeMode);
            put(KubernetesConstants.VolumeConfigKeys.path, path);
            put(KubernetesConstants.VolumeConfigKeys.subPath, subPath);
          }
        };

    final V1PersistentVolumeClaim claimOne = new V1PersistentVolumeClaimBuilder()
        .withNewMetadata()
          .withName(volumeNameOne)
          .withLabels(labels)
        .endMetadata()
        .withNewSpec()
          .withStorageClassName(storageClassName)
          .withAccessModes(Arrays.asList(accessModesList.split(",")))
          .withVolumeMode(volumeMode)
          .withNewResources()
            .addToRequests("storage", new Quantity(sizeLimit))
          .endResources()
        .endSpec()
        .build();

    final V1PersistentVolumeClaim claimTwo = new V1PersistentVolumeClaimBuilder()
        .withNewMetadata()
          .withName(volumeNameTwo)
          .withLabels(labels)
        .endMetadata()
        .withNewSpec()
          .withStorageClassName(storageClassName)
          .withAccessModes(Arrays.asList(accessModesList.split(",")))
          .withVolumeMode(volumeMode)
          .withNewResources()
            .addToRequests("storage", new Quantity(sizeLimit))
          .endResources()
        .endSpec()
        .build();

    final V1PersistentVolumeClaim claimStatic = new V1PersistentVolumeClaimBuilder()
        .withNewMetadata()
          .withName(volumeNameStatic)
          .withLabels(labels)
        .endMetadata()
        .withNewSpec()
          .withStorageClassName("")
          .withAccessModes(Collections.singletonList(accessModes))
          .withVolumeMode(volumeMode)
          .withNewResources()
            .addToRequests("storage", new Quantity(sizeLimit))
          .endResources()
        .endSpec()
        .build();

    final V1PersistentVolumeClaim actualPVCOne = Volumes.get()
        .createPersistentVolumeClaim(volumeNameOne, labels, volOneConfig);
    Assert.assertEquals("Volume one PVC", claimOne, actualPVCOne);

    final V1PersistentVolumeClaim actualPVCTwo = Volumes.get()
        .createPersistentVolumeClaim(volumeNameTwo, labels, volTwoConfig);
    Assert.assertEquals("Volume two PVC", claimTwo, actualPVCTwo);

    final V1PersistentVolumeClaim actualPVCStatic = Volumes.get()
        .createPersistentVolumeClaim(volumeNameStatic, labels, volStaticConfig);
    Assert.assertEquals("Volume static PVC", claimStatic, actualPVCStatic);
  }

  @Test
  public void testVolumeWithPersistentVolumeClaim() {
    final String claimName = "claim-name";
    final String volumeName = "volume-name";
    final V1Volume expected = new V1VolumeBuilder()
        .withName(volumeName)
          .withNewPersistentVolumeClaim()
        .withClaimName(claimName)
        .endPersistentVolumeClaim()
        .build();

    final V1Volume actual = Volumes.get().createPersistentVolumeClaim(claimName, volumeName);

    Assert.assertEquals("Volume with Persistent Volume Claim configured", expected, actual);
  }
}
