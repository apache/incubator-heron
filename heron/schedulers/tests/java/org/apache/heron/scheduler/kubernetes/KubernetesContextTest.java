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

import org.apache.heron.common.basics.Pair;
import org.apache.heron.scheduler.TopologySubmissionException;
import org.apache.heron.scheduler.kubernetes.KubernetesUtils.TestTuple;
import org.apache.heron.spi.common.Config;

import static org.apache.heron.scheduler.kubernetes.KubernetesConstants.VolumeConfigKeys;

public class KubernetesContextTest {

  private static final String TOPOLOGY_NAME = "Topology-Name";
  private static final String POD_TEMPLATE_CONFIGMAP_NAME = "pod-template-configmap-name";
  private final Config config = Config.newBuilder().build();
  private final Config configWithPodTemplateConfigMap = Config.newBuilder()
      .put(KubernetesContext.KUBERNETES_POD_TEMPLATE_LOCATION,
          POD_TEMPLATE_CONFIGMAP_NAME)
      .build();

  @Test
  public void testPodTemplateConfigMapName() {
    final String executorKey = String.format(KubernetesContext.KUBERNETES_POD_TEMPLATE_LOCATION,
        KubernetesConstants.EXECUTOR_NAME);
    final Config configExecutor = Config.newBuilder()
        .put(executorKey, POD_TEMPLATE_CONFIGMAP_NAME)
        .build();
    Assert.assertEquals("Executor location", POD_TEMPLATE_CONFIGMAP_NAME,
        KubernetesContext.getPodTemplateConfigMapName(configExecutor, true));

    final String managerKey = String.format(KubernetesContext.KUBERNETES_POD_TEMPLATE_LOCATION,
        KubernetesConstants.MANAGER_NAME);
    final Config configManager = Config.newBuilder()
        .put(managerKey, POD_TEMPLATE_CONFIGMAP_NAME)
        .build();
    Assert.assertEquals("Manager location", POD_TEMPLATE_CONFIGMAP_NAME,
        KubernetesContext.getPodTemplateConfigMapName(configManager, false));

    Assert.assertNull("No Pod Template",
        KubernetesContext.getPodTemplateConfigMapName(config, true));
  }

  @Test
  public void testPodTemplateDisabled() {
    Assert.assertFalse(KubernetesContext.getPodTemplateDisabled(config));
    Assert.assertFalse(KubernetesContext
        .getPodTemplateDisabled(configWithPodTemplateConfigMap));

    final Config configWithPodTemplateConfigMapOff = Config.newBuilder()
        .put(KubernetesContext.KUBERNETES_POD_TEMPLATE_LOCATION,
            POD_TEMPLATE_CONFIGMAP_NAME)
        .put(KubernetesContext.KUBERNETES_POD_TEMPLATE_DISABLED, "TRUE")
        .build();
    Assert.assertTrue(KubernetesContext
        .getPodTemplateDisabled(configWithPodTemplateConfigMapOff));
  }

  @Test
  public void testPersistentVolumeClaimDisabled() {
    Assert.assertFalse(KubernetesContext.getPersistentVolumeClaimDisabled(config));
    Assert.assertFalse(KubernetesContext
        .getPersistentVolumeClaimDisabled(configWithPodTemplateConfigMap));

    final Config configWithPodTemplateConfigMapOff = Config.newBuilder()
        .put(KubernetesContext.KUBERNETES_POD_TEMPLATE_LOCATION,
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
    final String keyPattern = KubernetesContext.KUBERNETES_VOLUME_CLAIM_PREFIX + "%%s.%%s";
    final String keyExecutor = String.format(keyPattern, KubernetesConstants.EXECUTOR_NAME);
    final String keyManager = String.format(keyPattern, KubernetesConstants.MANAGER_NAME);

    final String storageClassField = VolumeConfigKeys.storageClassName.name();
    final String pathField = VolumeConfigKeys.path.name();
    final String claimNameField = VolumeConfigKeys.claimName.name();
    final String expectedStorageClass = "expected-storage-class";
    final String expectedPath = "/path/for/volume/expected";


    // Test case container.
    // Input: Config to extract options from, Boolean to indicate Manager/Executor.
    // Output: [0] expectedKeys, [1] expectedOptionsKeys, [2] expectedOptionsValues.
    final List<TestTuple<Pair<Config, Boolean>, Object[]>> testCases = new LinkedList<>();

    // Create test cases for Executor/Manager on even/odd indices respectively.
    for (int idx = 0; idx < 2; ++idx) {

      // Manager case is default.
      boolean isExecutor = false;
      String key = keyManager;
      String description = KubernetesConstants.MANAGER_NAME;

      // Executor case.
      if (idx % 2 == 0) {
        isExecutor = true;
        key = keyExecutor;
        description = KubernetesConstants.EXECUTOR_NAME;
      }

      final String storageClassKeyOne = String.format(key, volumeNameOne, storageClassField);
      final String storageClassKeyTwo = String.format(key, volumeNameTwo, storageClassField);
      final String pathKeyOne = String.format(key, volumeNameOne, pathField);
      final String pathKeyTwo = String.format(key, volumeNameTwo, pathField);
      final String claimNameKeyOne = String.format(key, volumeNameOne, claimNameField);
      final String claimNameKeyTwo = String.format(key, volumeNameTwo, claimNameField);

      final Config configPVC = Config.newBuilder()
          .put(pathKeyOne, expectedPath)
          .put(pathKeyTwo, expectedPath)
          .put(claimNameKeyOne, claimName)
          .put(claimNameKeyTwo, claimName)
          .put(storageClassKeyOne, expectedStorageClass)
          .put(storageClassKeyTwo, expectedStorageClass)
          .build();

      final List<String> expectedKeys = Arrays.asList(volumeNameOne, volumeNameTwo);
      final List<VolumeConfigKeys> expectedOptionsKeys =
          Arrays.asList(VolumeConfigKeys.path,
              VolumeConfigKeys.storageClassName,
              VolumeConfigKeys.claimName);
      final List<String> expectedOptionsValues =
          Arrays.asList(expectedPath, expectedStorageClass, claimName);

      testCases.add(new TestTuple<>(description,
          new Pair<>(configPVC, isExecutor),
          new Object[]{expectedKeys, expectedOptionsKeys, expectedOptionsValues}));
    }

    // Test loop.
    for (TestTuple<Pair<Config, Boolean>, Object[]> testCase : testCases) {
      final Map<String, Map<VolumeConfigKeys, String>> mapOfPVC =
          KubernetesContext.getVolumeClaimTemplates(testCase.input.first, testCase.input.second);

      Assert.assertTrue(testCase.description + ": Contains all provided Volumes",
          mapOfPVC.keySet().containsAll((List<String>) testCase.expected[0]));
      for (Map<VolumeConfigKeys, String> items : mapOfPVC.values()) {
        Assert.assertTrue(testCase.description + ": Contains all provided option keys",
            items.keySet().containsAll((List<VolumeConfigKeys>) testCase.expected[1]));
        Assert.assertTrue(testCase.description + ": Contains all provided option values",
            items.values().containsAll((List<String>) testCase.expected[2]));
      }
    }

    // Empty PVC.
    final Boolean[] emptyPVCTestCases = new Boolean[] {true, false};
    for (boolean testCase : emptyPVCTestCases) {
      final Map<String, Map<VolumeConfigKeys, String>> emptyPVC =
          KubernetesContext.getVolumeClaimTemplates(Config.newBuilder().build(), testCase);
      Assert.assertTrue("Empty PVC is returned when no options provided", emptyPVC.isEmpty());
    }
  }

  @Test
  public void testGetPersistentVolumeClaimsErrors() {
    final String volumeNameValid = "volume-name-valid";
    final String volumeNameInvalid = "volume-Name-Invalid";
    final String failureValue = "Should-Fail";
    final String generalFailureMessage = "Invalid Persistent Volume";
    final String keyPattern = String.format(KubernetesContext.KUBERNETES_VOLUME_CLAIM_PREFIX
        + "%%s.%%s", KubernetesConstants.EXECUTOR_NAME);
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
    final Boolean[] executorFlags = new Boolean[] {true, false};
    for (TestTuple<Config, String> testCase : testCases) {
      // Test for both Executor and Manager.
      for (boolean isExecutor : executorFlags) {
        try {
          KubernetesContext.getVolumeClaimTemplates(testCase.input, isExecutor);
        } catch (TopologySubmissionException e) {
          Assert.assertTrue(testCase.description, e.getMessage().contains(testCase.expected));
        }
      }
    }
  }
}
