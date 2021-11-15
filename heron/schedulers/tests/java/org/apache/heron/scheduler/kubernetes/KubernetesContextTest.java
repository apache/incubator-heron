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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import org.apache.heron.scheduler.TopologySubmissionException;
import org.apache.heron.scheduler.kubernetes.KubernetesUtils.TestTuple;
import org.apache.heron.spi.common.Config;

import static org.apache.heron.scheduler.kubernetes.KubernetesConstants.VolumeClaimTemplateConfigKeys;

public class KubernetesContextTest {

  private static final String TOPOLOGY_NAME = "Topology-Name";
  private static final String KUBERNETES_POD_TEMPLATE_CONFIGMAP_NAME =
      "heron.kubernetes.pod.template.configmap.name";
  private static final String POD_TEMPLATE_CONFIGMAP_NAME = "pod-template-configmap-name";
  private final Config config = Config.newBuilder().build();
  private final Config configWithPodTemplateConfigMap = Config.newBuilder()
      .put(KubernetesContext.KUBERNETES_POD_TEMPLATE_CONFIGMAP_NAME,
          POD_TEMPLATE_CONFIGMAP_NAME)
      .build();

  @Test
  public void testPodTemplateConfigMapName() {
    Assert.assertEquals(KubernetesContext.KUBERNETES_POD_TEMPLATE_CONFIGMAP_NAME,
        KUBERNETES_POD_TEMPLATE_CONFIGMAP_NAME);
    Assert.assertEquals(
        KubernetesContext.getPodTemplateConfigMapName(configWithPodTemplateConfigMap),
        POD_TEMPLATE_CONFIGMAP_NAME);
    Assert.assertNull(KubernetesContext.getPodTemplateConfigMapName(config));
  }

  @Test
  public void testPodTemplateConfigMapDisabled() {
    Assert.assertFalse(KubernetesContext.getPodTemplateConfigMapDisabled(config));
    Assert.assertFalse(KubernetesContext
        .getPodTemplateConfigMapDisabled(configWithPodTemplateConfigMap));

    final Config configWithPodTemplateConfigMapOff = Config.newBuilder()
        .put(KubernetesContext.KUBERNETES_POD_TEMPLATE_CONFIGMAP_NAME,
            POD_TEMPLATE_CONFIGMAP_NAME)
        .put(KubernetesContext.KUBERNETES_POD_TEMPLATE_CONFIGMAP_DISABLED, "TRUE")
        .build();
    Assert.assertTrue(KubernetesContext
        .getPodTemplateConfigMapDisabled(configWithPodTemplateConfigMapOff));
  }

  @Test
  public void testPersistentVolumeClaimDisabled() {
    Assert.assertFalse(KubernetesContext.getPersistentVolumeClaimDisabled(config));
    Assert.assertFalse(KubernetesContext
        .getPersistentVolumeClaimDisabled(configWithPodTemplateConfigMap));

    final Config configWithPodTemplateConfigMapOff = Config.newBuilder()
        .put(KubernetesContext.KUBERNETES_POD_TEMPLATE_CONFIGMAP_NAME,
            POD_TEMPLATE_CONFIGMAP_NAME)
        .put(KubernetesContext.KUBERNETES_PERSISTENT_VOLUME_CLAIMS_CLI_DISABLED, "TRUE")
        .build();
    Assert.assertTrue(KubernetesContext
        .getPersistentVolumeClaimDisabled(configWithPodTemplateConfigMapOff));
  }

  @Test
  public void testGetVolumeClaimTemplates() {
    final String volumeNameOne = "volume-name-one";
    final String volumeNameTwo = "volume-name-two";
    final String claimName = "OnDeMaNd";
    final String keyPattern = KubernetesContext.KUBERNETES_VOLUME_CLAIM_PREFIX + "%s.%s";

    final String storageClassField = VolumeClaimTemplateConfigKeys.storageClassName.name();
    final String pathField = VolumeClaimTemplateConfigKeys.path.name();
    final String claimNameField = VolumeClaimTemplateConfigKeys.claimName.name();
    final String expectedStorageClass = "expected-storage-class";
    final String storageClassKeyOne = String.format(keyPattern, volumeNameOne, storageClassField);
    final String storageClassKeyTwo = String.format(keyPattern, volumeNameTwo, storageClassField);
    final String expectedPath = "/path/for/volume/expected";
    final String pathKeyOne = String.format(keyPattern, volumeNameOne, pathField);
    final String pathKeyTwo = String.format(keyPattern, volumeNameTwo, pathField);
    final String claimNameKeyOne = String.format(keyPattern, volumeNameOne, claimNameField);
    final String claimNameKeyTwo = String.format(keyPattern, volumeNameTwo, claimNameField);

    final Config configPVC = Config.newBuilder()
        .put(pathKeyOne, expectedPath)
        .put(pathKeyTwo, expectedPath)
        .put(claimNameKeyOne, claimName)
        .put(claimNameKeyTwo, claimName)
        .put(storageClassKeyOne, expectedStorageClass)
        .put(storageClassKeyTwo, expectedStorageClass)
        .build();

    final List<String> expectedKeys = Arrays.asList(volumeNameOne, volumeNameTwo);
    final List<VolumeClaimTemplateConfigKeys> expectedOptionsKeys =
        Arrays.asList(VolumeClaimTemplateConfigKeys.path,
            VolumeClaimTemplateConfigKeys.storageClassName,
            VolumeClaimTemplateConfigKeys.claimName);
    final List<String> expectedOptionsValues =
        Arrays.asList(expectedPath, expectedStorageClass, claimName);

    // List of provided PVC options.
    final Map<String, Map<VolumeClaimTemplateConfigKeys, String>> mapOfPVC =
        KubernetesContext.getVolumeClaimTemplates(configPVC);

    Assert.assertTrue("Contains all provided Volumes",
        mapOfPVC.keySet().containsAll(expectedKeys));
    for (Map<VolumeClaimTemplateConfigKeys, String> items : mapOfPVC.values()) {
      Assert.assertTrue("Contains all provided option keys",
          items.keySet().containsAll(expectedOptionsKeys));
      Assert.assertTrue("Contains all provided option values",
          items.values().containsAll(expectedOptionsValues));
    }

    // Empty PVC.
    final Map<String, Map<VolumeClaimTemplateConfigKeys, String>> emptyPVC =
        KubernetesContext.getVolumeClaimTemplates(Config.newBuilder().build());
    Assert.assertTrue("Empty PVC is returned when no options provided", emptyPVC.isEmpty());
  }

  @Test
  public void testGetPersistentVolumeClaimsErrors() {
    final String volumeNameValid = "volume-name-valid";
    final String volumeNameInvalid = "volume-Name-Invalid";
    final String failureValue = "Should-Fail";
    final String generalFailureMessage = "Invalid Persistent Volume";
    final String keyPattern = KubernetesContext.KUBERNETES_VOLUME_CLAIM_PREFIX
        + "%s.%s";
    final List<TestTuple<Config, String>> testCases = new LinkedList<>();

    // Invalid option key test.
    final Config configInvalidOption = Config.newBuilder()
        .put(String.format(keyPattern, volumeNameValid, "NonExistentKey"), failureValue)
        .build();
    testCases.add(new TestTuple<>("Invalid option key should trigger exception",
        configInvalidOption, generalFailureMessage));

    // Just the prefix.
    final Config configJustPrefix = Config.newBuilder()
        .put(KubernetesContext.KUBERNETES_VOLUME_CLAIM_PREFIX, failureValue)
        .build();
    testCases.add(new TestTuple<>("Only a key prefix should trigger exception",
        configJustPrefix, generalFailureMessage));

    // Invalid Volume Name.
    final Config configInvalidVolumeName = Config.newBuilder()
        .put(String.format(keyPattern, volumeNameInvalid, "path"), failureValue)
        .build();
    testCases.add(new TestTuple<>("Invalid Volume Name should trigger exception",
        configInvalidVolumeName, "lowercase RFC-1123"));

    // Invalid Claim Name.
    final Config configInvalidClaimName = Config.newBuilder()
        .put(String.format(keyPattern, volumeNameValid, "claimName"), failureValue)
        .build();
    testCases.add(new TestTuple<>("Invalid Claim Name should trigger exception",
        configInvalidClaimName, "Option `claimName`"));

    // Invalid Storage Class Name.
    final Config configInvalidStorageClassName = Config.newBuilder()
        .put(String.format(keyPattern, volumeNameValid, "storageClassName"), failureValue)
        .build();
    testCases.add(new TestTuple<>("Invalid Storage Class Name should trigger exception",
        configInvalidStorageClassName, "Option `storageClassName`"));

    // Testing loop.
    for (TestTuple<Config, String> testCase : testCases) {
      try {
        KubernetesContext.getVolumeClaimTemplates(testCase.input);
      } catch (TopologySubmissionException e) {
        Assert.assertTrue(testCase.description, e.getMessage().contains(testCase.expected));
      }
    }
  }
}
