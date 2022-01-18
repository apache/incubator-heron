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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableMap;

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
  public void testVolumesFromCLIDisabled() {
    Assert.assertFalse(KubernetesContext.getVolumesFromCLIDisabled(config));
    Assert.assertFalse(KubernetesContext
        .getVolumesFromCLIDisabled(configWithPodTemplateConfigMap));

    final Config configWithPodTemplateConfigMapOff = Config.newBuilder()
        .put(KubernetesContext.KUBERNETES_POD_TEMPLATE_LOCATION,
            POD_TEMPLATE_CONFIGMAP_NAME)
        .put(KubernetesContext.KUBERNETES_VOLUME_FROM_CLI_DISABLED, "TRUE")
        .build();
    Assert.assertTrue(KubernetesContext
        .getVolumesFromCLIDisabled(configWithPodTemplateConfigMapOff));
  }

  /**
   * Generate <code>Volume</code> Configs for testing.
   * @param testCases Test case container.
   *                  Input: [0] Config, [1] Boolean to indicate Manager/Executor.
   *                  Output: [0] expectedKeys, [1] expectedOptionsKeys, [2] expectedOptionsValues.
   * @param prefix Configuration prefix key to use in lookup.
   */
  private void createVolumeConfigs(List<TestTuple<Pair<Config, Boolean>, Object[]>> testCases,
                                   String prefix) {
    final String keyPattern = prefix + "%%s.%%s";
    final String keyExecutor = String.format(keyPattern, KubernetesConstants.EXECUTOR_NAME);
    final String keyManager = String.format(keyPattern, KubernetesConstants.MANAGER_NAME);

    final String volumeNameOne = "volume-name-one";
    final String volumeNameTwo = "volume-name-two";
    final String claimName = "OnDeMaNd";

    final String storageClassField = VolumeConfigKeys.storageClassName.name();
    final String pathField = VolumeConfigKeys.path.name();
    final String claimNameField = VolumeConfigKeys.claimName.name();
    final String expectedStorageClass = "expected-storage-class";
    final String expectedPath = "/path/for/volume/expected";

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

      final Config configPVCDisabled = Config.newBuilder()
          .put(KubernetesContext.KUBERNETES_VOLUME_FROM_CLI_DISABLED, "true")
          .put(pathKeyOne, expectedPath)
          .put(pathKeyTwo, expectedPath)
          .put(claimNameKeyOne, claimName)
          .put(claimNameKeyTwo, claimName)
          .put(storageClassKeyOne, expectedStorageClass)
          .put(storageClassKeyTwo, expectedStorageClass)
          .build();

      testCases.add(new TestTuple<>(description + " Disabled should not error",
          new Pair<>(configPVCDisabled, !isExecutor),
          new Object[]{new LinkedList<String>(), new LinkedList<VolumeConfigKeys>(),
              new LinkedList<String>()}));
    }
  }

  @Test
  public void testGetVolumeConfigs() {
    final String prefix = KubernetesContext.KUBERNETES_VOLUME_CLAIM_PREFIX;

    // Test case container.
    // Input: [0] Config, [1] Boolean to indicate Manager/Executor.
    // Output: [0] expectedKeys, [1] expectedOptionsKeys, [2] expectedOptionsValues.
    final List<TestTuple<Pair<Config, Boolean>, Object[]>> testCases = new LinkedList<>();
    createVolumeConfigs(testCases, prefix);

    // Test loop.
    for (TestTuple<Pair<Config, Boolean>, Object[]> testCase : testCases) {
      final Map<String, Map<VolumeConfigKeys, String>> mapOfPVC =
          KubernetesContext.getVolumeConfigs(testCase.input.first, prefix, testCase.input.second);

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
  public void testGetVolumeConfigsErrors() {
    final String prefix = KubernetesContext.KUBERNETES_VOLUME_CLAIM_PREFIX;
    final String volumeNameValid = "volume-name-valid";
    final String volumeNameInvalid = "volume-Name-Invalid";
    final String passingValue = "should-pass";
    final String failureValue = "Should-Fail";
    final String generalFailureMessage = "Invalid Volume configuration";
    final String keyPattern = String.format(KubernetesContext.KUBERNETES_VOLUME_CLAIM_PREFIX
        + "%%s.%%s", KubernetesConstants.EXECUTOR_NAME);
    final List<TestTuple<Config, String>> testCases = new LinkedList<>();

    // Invalid option key test.
    final Config configInvalidOption = Config.newBuilder()
        .put(String.format(keyPattern, volumeNameValid, "claimName"), passingValue)
        .put(String.format(keyPattern, volumeNameValid, "storageClassName"), passingValue)
        .put(String.format(keyPattern, volumeNameValid, "sizeLimit"), passingValue)
        .put(String.format(keyPattern, volumeNameValid, "accessModes"), passingValue)
        .put(String.format(keyPattern, volumeNameValid, "volumeMode"), passingValue)
        .put(String.format(keyPattern, volumeNameValid, "path"), passingValue)
        .put(String.format(keyPattern, volumeNameValid, "subPath"), passingValue)
        .put(String.format(keyPattern, volumeNameValid, "server"), passingValue)
        .put(String.format(keyPattern, volumeNameValid, "readOnly"), passingValue)
        .put(String.format(keyPattern, volumeNameValid, "type"), passingValue)
        .put(String.format(keyPattern, volumeNameValid, "medium"), passingValue)
        .put(String.format(keyPattern, volumeNameValid, "NonExistentKey"), failureValue)
        .build();
    testCases.add(new TestTuple<>("Invalid option key should trigger exception",
        configInvalidOption, generalFailureMessage));

    // Invalid Volume Name.
    final Config configInvalidVolumeName = Config.newBuilder()
        .put(String.format(keyPattern, volumeNameInvalid, "path"), failureValue)
        .build();
    testCases.add(new TestTuple<>("Invalid Volume Name should trigger exception",
        configInvalidVolumeName, "lowercase RFC-1123"));

    // Required Path.
    final Config configRequiredPath = Config.newBuilder()
        .put(String.format(keyPattern, volumeNameValid, "claimName"), passingValue)
        .put(String.format(keyPattern, volumeNameValid, "storageClassName"), passingValue)
        .put(String.format(keyPattern, volumeNameValid, "sizeLimit"), passingValue)
        .put(String.format(keyPattern, volumeNameValid, "accessModes"), passingValue)
        .put(String.format(keyPattern, volumeNameValid, "volumeMode"), passingValue)
        .put(String.format(keyPattern, volumeNameValid, "subPath"), passingValue)
        .put(String.format(keyPattern, volumeNameValid, "server"), passingValue)
        .put(String.format(keyPattern, volumeNameValid, "readOnly"), passingValue)
        .put(String.format(keyPattern, volumeNameValid, "type"), passingValue)
        .put(String.format(keyPattern, volumeNameValid, "medium"), passingValue)
        .build();
    testCases.add(new TestTuple<>("Missing path should trigger exception",
        configRequiredPath, "All Volumes require a 'path'."));

    // Disabled.
    final Config configDisabled = Config.newBuilder()
        .put(KubernetesContext.KUBERNETES_VOLUME_FROM_CLI_DISABLED, "true")
        .put(String.format(keyPattern, volumeNameValid, "claimName"), passingValue)
        .put(String.format(keyPattern, volumeNameValid, "storageClassName"), passingValue)
        .put(String.format(keyPattern, volumeNameValid, "sizeLimit"), passingValue)
        .put(String.format(keyPattern, volumeNameValid, "accessModes"), passingValue)
        .put(String.format(keyPattern, volumeNameValid, "volumeMode"), passingValue)
        .put(String.format(keyPattern, volumeNameValid, "path"), passingValue)
        .put(String.format(keyPattern, volumeNameValid, "subPath"), passingValue)
        .put(String.format(keyPattern, volumeNameValid, "server"), passingValue)
        .put(String.format(keyPattern, volumeNameValid, "readOnly"), passingValue)
        .put(String.format(keyPattern, volumeNameValid, "type"), passingValue)
        .put(String.format(keyPattern, volumeNameValid, "medium"), passingValue)
        .build();
    testCases.add(new TestTuple<>("Disabled functionality should trigger exception",
        configDisabled, "Configuring Volumes from the CLI is disabled."));

    // Testing loop.
    for (TestTuple<Config, String> testCase : testCases) {
      String message = "";
      try {
        KubernetesContext.getVolumeConfigs(testCase.input, prefix, true);
      } catch (TopologySubmissionException e) {
        message = e.getMessage();
      }
      Assert.assertTrue(testCase.description, message.contains(testCase.expected));
    }
  }

  /**
   * Create test cases for <code>Volume Claim Templates</code>.
   * @param testCases Test case container.
   *                  Input: [0] Config, [1] Boolean to indicate Manager/Executor.
   *                  Output: <code>Map<String, Map<VolumeConfigKeys, String></code>
   * @param isExecutor Boolean to indicate Manager/Executor test case generation.
   */
  private void createVolumeClaimTemplates(
      List<TestTuple<Pair<Config, Boolean>, Map<String, Map<VolumeConfigKeys, String>>>> testCases,
      boolean isExecutor) {
    final String volumeNameValid = "volume-name-valid";
    final String passingValue = "should-pass";
    final String processName = isExecutor ? KubernetesConstants.EXECUTOR_NAME
        : KubernetesConstants.MANAGER_NAME;
    final String keyPattern = String.format(KubernetesContext.KUBERNETES_VOLUME_CLAIM_PREFIX
        + "%%s.%%s", processName);

    // With Storage Class Name.
    final Map<String, Map<VolumeConfigKeys, String>> expectedWithStorageClassName =
        ImmutableMap.of(volumeNameValid, new HashMap<VolumeConfigKeys, String>() {
          {
            put(VolumeConfigKeys.claimName, passingValue);
            put(VolumeConfigKeys.storageClassName, passingValue);
            put(VolumeConfigKeys.sizeLimit, passingValue);
            put(VolumeConfigKeys.accessModes, passingValue);
            put(VolumeConfigKeys.volumeMode, passingValue);
            put(VolumeConfigKeys.path, passingValue);
            put(VolumeConfigKeys.subPath, passingValue);
            put(VolumeConfigKeys.readOnly, passingValue);
          }
        });
    final Config configWithStorageClass = Config.newBuilder()
        .put(String.format(keyPattern, volumeNameValid, "claimName"), passingValue)
        .put(String.format(keyPattern, volumeNameValid, "storageClassName"), passingValue)
        .put(String.format(keyPattern, volumeNameValid, "sizeLimit"), passingValue)
        .put(String.format(keyPattern, volumeNameValid, "accessModes"), passingValue)
        .put(String.format(keyPattern, volumeNameValid, "volumeMode"), passingValue)
        .put(String.format(keyPattern, volumeNameValid, "path"), passingValue)
        .put(String.format(keyPattern, volumeNameValid, "subPath"), passingValue)
        .put(String.format(keyPattern, volumeNameValid, "readOnly"), passingValue)
        .build();
    testCases.add(new TestTuple<>(processName + ": PVC with Storage Class name",
        new Pair<>(configWithStorageClass, isExecutor), expectedWithStorageClassName));

    // Without Storage Class Name.
    final Map<String, Map<VolumeConfigKeys, String>> expectedWithoutStorageClassName =
        ImmutableMap.of(volumeNameValid, new HashMap<VolumeConfigKeys, String>() {
          {
            put(VolumeConfigKeys.claimName, passingValue);
            put(VolumeConfigKeys.sizeLimit, passingValue);
            put(VolumeConfigKeys.accessModes, passingValue);
            put(VolumeConfigKeys.volumeMode, passingValue);
            put(VolumeConfigKeys.path, passingValue);
            put(VolumeConfigKeys.subPath, passingValue);
            put(VolumeConfigKeys.readOnly, passingValue);
          }
        });
    final Config configWithoutStorageClass = Config.newBuilder()
        .put(String.format(keyPattern, volumeNameValid, "claimName"), passingValue)
        .put(String.format(keyPattern, volumeNameValid, "sizeLimit"), passingValue)
        .put(String.format(keyPattern, volumeNameValid, "accessModes"), passingValue)
        .put(String.format(keyPattern, volumeNameValid, "volumeMode"), passingValue)
        .put(String.format(keyPattern, volumeNameValid, "path"), passingValue)
        .put(String.format(keyPattern, volumeNameValid, "subPath"), passingValue)
        .put(String.format(keyPattern, volumeNameValid, "readOnly"), passingValue)
        .build();
    testCases.add(new TestTuple<>(processName + ": PVC with Storage Class name",
        new Pair<>(configWithoutStorageClass, isExecutor), expectedWithoutStorageClassName));

    // Ignored.
    final Config configIgnored = Config.newBuilder()
        .put(String.format(keyPattern, volumeNameValid, "claimName"), passingValue)
        .put(String.format(keyPattern, volumeNameValid, "storageClassName"), passingValue)
        .put(String.format(keyPattern, volumeNameValid, "sizeLimit"), passingValue)
        .put(String.format(keyPattern, volumeNameValid, "accessModes"), passingValue)
        .put(String.format(keyPattern, volumeNameValid, "volumeMode"), passingValue)
        .put(String.format(keyPattern, volumeNameValid, "path"), passingValue)
        .put(String.format(keyPattern, volumeNameValid, "subPath"), passingValue)
        .put(String.format(keyPattern, volumeNameValid, "readOnly"), passingValue)
        .build();
    testCases.add(new TestTuple<>(processName + ": PVC with ignored keys",
        new Pair<>(configIgnored, !isExecutor), new HashMap<>()));
  }

  @Test
  public void testGetVolumeClaimTemplates() {
    final List<TestTuple<Pair<Config, Boolean>, Map<String, Map<VolumeConfigKeys, String>>>>
        testCases = new LinkedList<>();
    createVolumeClaimTemplates(testCases, true);
    createVolumeClaimTemplates(testCases, false);

    // Testing loop.
    for (TestTuple<Pair<Config, Boolean>, Map<String, Map<VolumeConfigKeys, String>>> testCase
        : testCases) {
      Map<String, Map<VolumeConfigKeys, String>> actual =
          KubernetesContext.getVolumeClaimTemplates(testCase.input.first, testCase.input.second);
      Assert.assertEquals(testCase.description, testCase.expected, actual);
    }
  }

  /**
   * Create test cases for <code>Volume Claim Templates</code> errors.
   * @param testCases Test case container.
   *                  Input: [0] Config, [1] Boolean to indicate Manager/Executor.
   *                  Output: Error message
   * @param isExecutor Boolean to indicate Manager/Executor test case generation.
   */
  private void createVolumeClaimTemplatesErrors(
      List<TestTuple<Pair<Config, Boolean>, String>> testCases, boolean isExecutor) {
    final String volumeNameValid = "volume-name-valid";
    final String passingValue = "should-pass";
    final String failureValue = "Should-Fail";
    final String processName = isExecutor ? KubernetesConstants.EXECUTOR_NAME
        : KubernetesConstants.MANAGER_NAME;
    final String keyPattern = String.format(KubernetesContext.KUBERNETES_VOLUME_CLAIM_PREFIX
        + "%%s.%%s", processName);

    // Required Claim Name.
    final Config configRequiredClaimName = Config.newBuilder()
        .put(String.format(keyPattern, volumeNameValid, "path"), passingValue)
        .build();
    testCases.add(new TestTuple<>(processName + ": Missing Claim Name should trigger exception",
        new Pair<>(configRequiredClaimName, isExecutor), "require a `claimName`"));

    // Invalid Claim Name.
    final Config configInvalidClaimName = Config.newBuilder()
        .put(String.format(keyPattern, volumeNameValid, "path"), passingValue)
        .put(String.format(keyPattern, volumeNameValid, "claimName"), failureValue)
        .build();
    testCases.add(new TestTuple<>(processName + ": Invalid Claim Name should trigger exception",
        new Pair<>(configInvalidClaimName, isExecutor),
        String.format("Volume `%s`: `claimName`", volumeNameValid)));

    // Invalid Storage Class Name.
    final Config configInvalidStorageClassName = Config.newBuilder()
        .put(String.format(keyPattern, volumeNameValid, "claimName"), passingValue)
        .put(String.format(keyPattern, volumeNameValid, "path"), passingValue)
        .put(String.format(keyPattern, volumeNameValid, "storageClassName"), failureValue)
        .build();
    testCases.add(new TestTuple<>(processName
        + ": Invalid Storage Class Name should trigger exception",
        new Pair<>(configInvalidStorageClassName, isExecutor),
        String.format("Volume `%s`: `storageClassName`", volumeNameValid)));

    // Invalid Storage Class Name.
    final Config configInvalidOption = Config.newBuilder()
        .put(String.format(keyPattern, volumeNameValid, "claimName"), passingValue)
        .put(String.format(keyPattern, volumeNameValid, "storageClassName"), passingValue)
        .put(String.format(keyPattern, volumeNameValid, "sizeLimit"), passingValue)
        .put(String.format(keyPattern, volumeNameValid, "accessModes"), passingValue)
        .put(String.format(keyPattern, volumeNameValid, "volumeMode"), passingValue)
        .put(String.format(keyPattern, volumeNameValid, "path"), passingValue)
        .put(String.format(keyPattern, volumeNameValid, "subPath"), passingValue)
        .put(String.format(keyPattern, volumeNameValid, "server"), passingValue)
        .build();
    testCases.add(new TestTuple<>(processName + ": Invalid option should trigger exception",
        new Pair<>(configInvalidOption, isExecutor),
        String.format("Volume `%s`: Invalid Persistent", volumeNameValid)));
  }

  @Test
  public void testGetVolumeClaimTemplatesErrors() {
    final List<TestTuple<Pair<Config, Boolean>, String>> testCases = new LinkedList<>();
    createVolumeClaimTemplatesErrors(testCases, true);
    createVolumeClaimTemplatesErrors(testCases, false);

    // Testing loop.
    for (TestTuple<Pair<Config, Boolean>, String> testCase : testCases) {
      String message = "";
      try {
        KubernetesContext.getVolumeClaimTemplates(testCase.input.first, testCase.input.second);
      } catch (TopologySubmissionException e) {
        message = e.getMessage();
      }
      Assert.assertTrue(testCase.description, message.contains(testCase.expected));
    }
  }

  /**
   * Create test cases for <code>Empty Directory</code>.
   * @param testCases Test case container.
   *                  Input: [0] Config, [1] Boolean to indicate Manager/Executor.
   *                  Output: <code>Map<String, Map<VolumeConfigKeys, String></code>
   * @param isExecutor Boolean to indicate Manager/Executor test case generation.
   */
  private void createVolumeEmptyDir(
      List<TestTuple<Pair<Config, Boolean>, Map<String, Map<VolumeConfigKeys, String>>>> testCases,
      boolean isExecutor) {
    final String volumeNameValid = "volume-name-valid";
    final String passingValue = "should-pass";
    final String processName = isExecutor ? KubernetesConstants.EXECUTOR_NAME
        : KubernetesConstants.MANAGER_NAME;
    final String keyPattern = String.format(KubernetesContext.KUBERNETES_VOLUME_EMPTYDIR_PREFIX
        + "%%s.%%s", processName);

    // With Medium.
    final Map<String, Map<VolumeConfigKeys, String>> expectedWithMedium =
        ImmutableMap.of(volumeNameValid, new HashMap<VolumeConfigKeys, String>() {
          {
            put(VolumeConfigKeys.sizeLimit, passingValue);
            put(VolumeConfigKeys.medium, "Memory");
            put(VolumeConfigKeys.path, passingValue);
            put(VolumeConfigKeys.subPath, passingValue);
            put(VolumeConfigKeys.readOnly, passingValue);
          }
        });
    final Config configWithMedium = Config.newBuilder()
        .put(String.format(keyPattern, volumeNameValid, "sizeLimit"), passingValue)
        .put(String.format(keyPattern, volumeNameValid, "medium"), "Memory")
        .put(String.format(keyPattern, volumeNameValid, "path"), passingValue)
        .put(String.format(keyPattern, volumeNameValid, "subPath"), passingValue)
        .put(String.format(keyPattern, volumeNameValid, "readOnly"), passingValue)
        .build();
    testCases.add(new TestTuple<>(processName + ": `emptyDir` with `medium`",
        new Pair<>(configWithMedium, isExecutor), expectedWithMedium));

    // With empty Medium.
    final Map<String, Map<VolumeConfigKeys, String>> expectedEmptyMedium =
        ImmutableMap.of(volumeNameValid, new HashMap<VolumeConfigKeys, String>() {
          {
            put(VolumeConfigKeys.sizeLimit, passingValue);
            put(VolumeConfigKeys.medium, "");
            put(VolumeConfigKeys.path, passingValue);
            put(VolumeConfigKeys.subPath, passingValue);
            put(VolumeConfigKeys.readOnly, passingValue);
          }
        });
    final Config configEmptyMedium = Config.newBuilder()
        .put(String.format(keyPattern, volumeNameValid, "sizeLimit"), passingValue)
        .put(String.format(keyPattern, volumeNameValid, "medium"), "")
        .put(String.format(keyPattern, volumeNameValid, "path"), passingValue)
        .put(String.format(keyPattern, volumeNameValid, "subPath"), passingValue)
        .put(String.format(keyPattern, volumeNameValid, "readOnly"), passingValue)
        .build();
    testCases.add(new TestTuple<>(processName + ": `emptyDir` with empty `medium`",
        new Pair<>(configEmptyMedium, isExecutor), expectedEmptyMedium));

    // Without Medium.
    final Map<String, Map<VolumeConfigKeys, String>> expectedNoMedium =
        ImmutableMap.of(volumeNameValid, new HashMap<VolumeConfigKeys, String>() {
          {
            put(VolumeConfigKeys.sizeLimit, passingValue);
            put(VolumeConfigKeys.path, passingValue);
            put(VolumeConfigKeys.subPath, passingValue);
            put(VolumeConfigKeys.readOnly, passingValue);
          }
        });
    final Config configNoMedium = Config.newBuilder()
        .put(String.format(keyPattern, volumeNameValid, "sizeLimit"), passingValue)
        .put(String.format(keyPattern, volumeNameValid, "path"), passingValue)
        .put(String.format(keyPattern, volumeNameValid, "subPath"), passingValue)
        .put(String.format(keyPattern, volumeNameValid, "readOnly"), passingValue)
        .build();
    testCases.add(new TestTuple<>(processName + ": `emptyDir` without `medium`",
        new Pair<>(configNoMedium, isExecutor), expectedNoMedium));

    // Ignored.
    final Config configIgnored = Config.newBuilder()
        .put(String.format(keyPattern, volumeNameValid, "sizeLimit"), passingValue)
        .put(String.format(keyPattern, volumeNameValid, "medium"), "")
        .put(String.format(keyPattern, volumeNameValid, "path"), passingValue)
        .put(String.format(keyPattern, volumeNameValid, "subPath"), passingValue)
        .put(String.format(keyPattern, volumeNameValid, "readOnly"), passingValue)
        .build();
    testCases.add(new TestTuple<>(processName + ": `emptyDir` ignored",
        new Pair<>(configIgnored, !isExecutor), new HashMap<>()));
  }

  @Test
  public void testGetVolumeEmptyDir() {
    final List<TestTuple<Pair<Config, Boolean>, Map<String, Map<VolumeConfigKeys, String>>>>
        testCases = new LinkedList<>();
    createVolumeEmptyDir(testCases, true);
    createVolumeEmptyDir(testCases, false);

    // Testing loop.
    for (TestTuple<Pair<Config, Boolean>, Map<String, Map<VolumeConfigKeys, String>>> testCase
        : testCases) {
      Map<String, Map<VolumeConfigKeys, String>> actual =
          KubernetesContext.getVolumeEmptyDir(testCase.input.first, testCase.input.second);
      Assert.assertEquals(testCase.description, testCase.expected, actual);
    }
  }

  /**
   * Create test cases for <code>Empty Directory</code> errors.
   * @param testCases Test case container.
   *                  Input: [0] Config, [1] Boolean to indicate Manager/Executor.
   *                  Output: Error message
   * @param isExecutor Boolean to indicate Manager/Executor test case generation.
   */
  private void createVolumeEmptyDirError(
      List<TestTuple<Pair<Config, Boolean>, String>> testCases, boolean isExecutor) {
    final String volumeNameValid = "volume-name-valid";
    final String passingValue = "should-pass";
    final String failureValue = "Should-Fail";
    final String processName = isExecutor ? KubernetesConstants.EXECUTOR_NAME
        : KubernetesConstants.MANAGER_NAME;
    final String keyPattern = String.format(KubernetesContext.KUBERNETES_VOLUME_EMPTYDIR_PREFIX
        + "%%s.%%s", processName);

    // Medium is invalid.
    final Config configInvalidMedium = Config.newBuilder()
        .put(String.format(keyPattern, volumeNameValid, "path"), passingValue)
        .put(String.format(keyPattern, volumeNameValid, "sizeLimit"), passingValue)
        .put(String.format(keyPattern, volumeNameValid, "medium"), failureValue)
        .build();
    testCases.add(new TestTuple<>(processName + ": Invalid 'medium' should trigger exception",
        new Pair<>(configInvalidMedium, isExecutor), "must be 'Memory' or empty."));

    // Invalid option.
    final Config configInvalidOption = Config.newBuilder()
        .put(String.format(keyPattern, volumeNameValid, "path"), passingValue)
        .put(String.format(keyPattern, volumeNameValid, "subPath"), passingValue)
        .put(String.format(keyPattern, volumeNameValid, "sizeLimit"), passingValue)
        .put(String.format(keyPattern, volumeNameValid, "medium"), "Memory")
        .put(String.format(keyPattern, volumeNameValid, "accessModes"), passingValue)
        .build();
    testCases.add(new TestTuple<>(processName + ": Invalid option should trigger exception",
        new Pair<>(configInvalidOption, isExecutor), "Directory type option"));
  }

  @Test
  public void testGetVolumeEmptyDirErrors() {
    final List<TestTuple<Pair<Config, Boolean>, String>> testCases = new LinkedList<>();
    createVolumeEmptyDirError(testCases, true);
    createVolumeEmptyDirError(testCases, false);

    // Testing loop.
    for (TestTuple<Pair<Config, Boolean>, String> testCase : testCases) {
      String message = "";
      try {
        KubernetesContext.getVolumeEmptyDir(testCase.input.first, testCase.input.second);
      } catch (TopologySubmissionException e) {
        message = e.getMessage();
      }
      Assert.assertTrue(testCase.description, message.contains(testCase.expected));
    }
  }

  /**
   * Create test cases for <code>Host Path</code>.
   * @param testCases Test case container.
   *                  Input: [0] Config, [1] Boolean to indicate Manager/Executor.
   *                  Output: <code>Map<String, Map<VolumeConfigKeys, String></code>
   * @param isExecutor Boolean to indicate Manager/Executor test case generation.
   */
  private void createVolumeHostPath(
      List<TestTuple<Pair<Config, Boolean>, Map<String, Map<VolumeConfigKeys, String>>>> testCases,
      boolean isExecutor) {
    final String volumeNameValid = "volume-name-valid";
    final String passingValue = "should-pass";
    final String processName = isExecutor ? KubernetesConstants.EXECUTOR_NAME
        : KubernetesConstants.MANAGER_NAME;
    final String keyPattern = String.format(KubernetesContext.KUBERNETES_VOLUME_HOSTPATH_PREFIX
        + "%%s.%%s", processName);

    // With type.
    final Map<String, Map<VolumeConfigKeys, String>> expectedWithType =
        ImmutableMap.of(volumeNameValid, new HashMap<VolumeConfigKeys, String>() {
          {
            put(VolumeConfigKeys.type, "DirectoryOrCreate");
            put(VolumeConfigKeys.path, passingValue);
            put(VolumeConfigKeys.pathOnHost, passingValue);
            put(VolumeConfigKeys.subPath, passingValue);
            put(VolumeConfigKeys.readOnly, passingValue);
          }
        });
    final Config configWithType = Config.newBuilder()
        .put(String.format(keyPattern, volumeNameValid, "type"), "DirectoryOrCreate")
        .put(String.format(keyPattern, volumeNameValid, "pathOnHost"), passingValue)
        .put(String.format(keyPattern, volumeNameValid, "path"), passingValue)
        .put(String.format(keyPattern, volumeNameValid, "subPath"), passingValue)
        .put(String.format(keyPattern, volumeNameValid, "readOnly"), passingValue)
        .build();
    testCases.add(new TestTuple<>(processName + ": 'hostPath' with 'type'",
        new Pair<>(configWithType, isExecutor), expectedWithType));

    // Without type.
    final Map<String, Map<VolumeConfigKeys, String>> expectedWithoutType =
        ImmutableMap.of(volumeNameValid, new HashMap<VolumeConfigKeys, String>() {
          {
            put(VolumeConfigKeys.pathOnHost, passingValue);
            put(VolumeConfigKeys.path, passingValue);
            put(VolumeConfigKeys.subPath, passingValue);
            put(VolumeConfigKeys.readOnly, passingValue);
          }
        });
    final Config configWithoutType = Config.newBuilder()
        .put(String.format(keyPattern, volumeNameValid, "pathOnHost"), passingValue)
        .put(String.format(keyPattern, volumeNameValid, "path"), passingValue)
        .put(String.format(keyPattern, volumeNameValid, "subPath"), passingValue)
        .put(String.format(keyPattern, volumeNameValid, "readOnly"), passingValue)
        .build();
    testCases.add(new TestTuple<>(processName + ": 'hostPath' without 'type'",
        new Pair<>(configWithoutType, isExecutor), expectedWithoutType));

    // Ignored.
    final Config configIgnored = Config.newBuilder()
        .put(String.format(keyPattern, volumeNameValid, "type"), "BlockDevice")
        .put(String.format(keyPattern, volumeNameValid, "pathOnHost"), passingValue)
        .put(String.format(keyPattern, volumeNameValid, "path"), passingValue)
        .put(String.format(keyPattern, volumeNameValid, "subPath"), passingValue)
        .put(String.format(keyPattern, volumeNameValid, "readOnly"), passingValue)
        .build();
    testCases.add(new TestTuple<>(processName + ": 'hostPath' ignored",
        new Pair<>(configIgnored, !isExecutor), new HashMap<>()));
  }

  @Test
  public void testGetVolumeHostPath() {
    final List<TestTuple<Pair<Config, Boolean>, Map<String, Map<VolumeConfigKeys, String>>>>
        testCases = new LinkedList<>();
    createVolumeHostPath(testCases, true);
    createVolumeHostPath(testCases, false);

    // Testing loop.
    for (TestTuple<Pair<Config, Boolean>, Map<String, Map<VolumeConfigKeys, String>>> testCase
        : testCases) {
      Map<String, Map<VolumeConfigKeys, String>> actual =
          KubernetesContext.getVolumeHostPath(testCase.input.first, testCase.input.second);
      Assert.assertEquals(testCase.description, testCase.expected, actual);
    }
  }

  /**
   * Create test cases for <code>Host Path</code> errors.
   * @param testCases Test case container.
   *                  Input: [0] Config, [1] Boolean to indicate Manager/Executor.
   *                  Output: Error message
   * @param isExecutor Boolean to indicate Manager/Executor test case generation.
   */
  private void createVolumeHostPathError(
      List<TestTuple<Pair<Config, Boolean>, String>> testCases, boolean isExecutor) {
    final String volumeNameValid = "volume-name-valid";
    final String passingValue = "should-pass";
    final String failureValue = "Should-Fail";
    final String processName = isExecutor ? KubernetesConstants.EXECUTOR_NAME
        : KubernetesConstants.MANAGER_NAME;
    final String keyPattern = String.format(KubernetesContext.KUBERNETES_VOLUME_HOSTPATH_PREFIX
        + "%%s.%%s", processName);

    // Type is invalid.
    final Config configInvalidMedium = Config.newBuilder()
        .put(String.format(keyPattern, volumeNameValid, "type"), failureValue)
        .put(String.format(keyPattern, volumeNameValid, "pathOnHost"), passingValue)
        .put(String.format(keyPattern, volumeNameValid, "path"), passingValue)
        .put(String.format(keyPattern, volumeNameValid, "subPath"), passingValue)
        .build();
    testCases.add(new TestTuple<>(processName + ": Invalid 'type' should trigger exception",
        new Pair<>(configInvalidMedium, isExecutor), "Host Path 'type' of"));

    // Path on Host is missing.
    final Config configNoHostOnPath = Config.newBuilder()
        .put(String.format(keyPattern, volumeNameValid, "type"), "BlockDevice")
        .put(String.format(keyPattern, volumeNameValid, "path"), passingValue)
        .put(String.format(keyPattern, volumeNameValid, "subPath"), passingValue)
        .build();
    testCases.add(new TestTuple<>(processName + ": No 'hostOnPath' should trigger exception",
        new Pair<>(configNoHostOnPath, isExecutor), "requires a path on the host"));

    // Path on Host is empty.
    final Config configEmptyHostOnPath = Config.newBuilder()
        .put(String.format(keyPattern, volumeNameValid, "type"), "BlockDevice")
        .put(String.format(keyPattern, volumeNameValid, "pathOnHost"), "")
        .put(String.format(keyPattern, volumeNameValid, "path"), passingValue)
        .put(String.format(keyPattern, volumeNameValid, "subPath"), passingValue)
        .build();
    testCases.add(new TestTuple<>(processName + ": Empty 'hostOnPath' should trigger exception",
        new Pair<>(configEmptyHostOnPath, isExecutor), "requires a path on the host"));

    // Invalid option.
    final Config configInvalidOption = Config.newBuilder()
        .put(String.format(keyPattern, volumeNameValid, "type"), "BlockDevice")
        .put(String.format(keyPattern, volumeNameValid, "pathOnHost"), passingValue)
        .put(String.format(keyPattern, volumeNameValid, "path"), passingValue)
        .put(String.format(keyPattern, volumeNameValid, "subPath"), passingValue)
        .put(String.format(keyPattern, volumeNameValid, "accessModes"), passingValue)
        .build();
    testCases.add(new TestTuple<>(processName + ": Invalid option should trigger exception",
        new Pair<>(configInvalidOption, isExecutor), "Invalid Host Path option for"));
  }

  @Test
  public void testGetVolumeHostPathErrors() {
    final List<TestTuple<Pair<Config, Boolean>, String>> testCases = new LinkedList<>();
    createVolumeHostPathError(testCases, true);
    createVolumeHostPathError(testCases, false);

    // Testing loop.
    for (TestTuple<Pair<Config, Boolean>, String> testCase : testCases) {
      String message = "";
      try {
        KubernetesContext.getVolumeHostPath(testCase.input.first, testCase.input.second);
      } catch (TopologySubmissionException e) {
        message = e.getMessage();
      }
      Assert.assertTrue(testCase.description, message.contains(testCase.expected));
    }
  }

  /**
   * Create test cases for <code>NFS</code>.
   * @param testCases Test case container.
   *                  Input: [0] Config, [1] Boolean to indicate Manager/Executor.
   *                  Output: <code>Map<String, Map<VolumeConfigKeys, String></code>
   * @param isExecutor Boolean to indicate Manager/Executor test case generation.
   */
  private void createVolumeNFS(
      List<TestTuple<Pair<Config, Boolean>, Map<String, Map<VolumeConfigKeys, String>>>> testCases,
      boolean isExecutor) {
    final String volumeNameValid = "volume-name-valid";
    final String passingValue = "should-pass";
    final String processName = isExecutor ? KubernetesConstants.EXECUTOR_NAME
        : KubernetesConstants.MANAGER_NAME;
    final String keyPattern = String.format(KubernetesContext.KUBERNETES_VOLUME_NFS_PREFIX
        + "%%s.%%s", processName);

    // With readOnly.
    final Map<String, Map<VolumeConfigKeys, String>> expectedWithReadOnly =
        ImmutableMap.of(volumeNameValid, new HashMap<VolumeConfigKeys, String>() {
          {
            put(VolumeConfigKeys.server, "nfs-server.default.local");
            put(VolumeConfigKeys.readOnly, "true");
            put(VolumeConfigKeys.pathOnNFS, passingValue);
            put(VolumeConfigKeys.path, passingValue);
            put(VolumeConfigKeys.subPath, passingValue);
            put(VolumeConfigKeys.readOnly, passingValue);
          }
        });
    final Config configWithReadOnly = Config.newBuilder()
        .put(String.format(keyPattern, volumeNameValid, "server"), "nfs-server.default.local")
        .put(String.format(keyPattern, volumeNameValid, "readOnly"), "true")
        .put(String.format(keyPattern, volumeNameValid, "pathOnNFS"), passingValue)
        .put(String.format(keyPattern, volumeNameValid, "path"), passingValue)
        .put(String.format(keyPattern, volumeNameValid, "subPath"), passingValue)
        .put(String.format(keyPattern, volumeNameValid, "readOnly"), passingValue)
        .build();
    testCases.add(new TestTuple<>(processName + ": `NFS` with `readOnly`",
        new Pair<>(configWithReadOnly, isExecutor), expectedWithReadOnly));

    // With readOnly.
    final Map<String, Map<VolumeConfigKeys, String>> expectedWithoutReadOnly =
        ImmutableMap.of(volumeNameValid, new HashMap<VolumeConfigKeys, String>() {
          {
            put(VolumeConfigKeys.server, "nfs-server.default.local");
            put(VolumeConfigKeys.pathOnNFS, passingValue);
            put(VolumeConfigKeys.path, passingValue);
            put(VolumeConfigKeys.subPath, passingValue);
            put(VolumeConfigKeys.readOnly, passingValue);
          }
        });
    final Config configWithoutReadOnly = Config.newBuilder()
        .put(String.format(keyPattern, volumeNameValid, "server"), "nfs-server.default.local")
        .put(String.format(keyPattern, volumeNameValid, "pathOnNFS"), passingValue)
        .put(String.format(keyPattern, volumeNameValid, "path"), passingValue)
        .put(String.format(keyPattern, volumeNameValid, "subPath"), passingValue)
        .put(String.format(keyPattern, volumeNameValid, "readOnly"), passingValue)
        .build();
    testCases.add(new TestTuple<>(processName + ": `NFS` without `readOnly`",
        new Pair<>(configWithoutReadOnly, isExecutor), expectedWithoutReadOnly));

    // Ignored.
    final Config configIgnored = Config.newBuilder()
        .put(String.format(keyPattern, volumeNameValid, "server"), "nfs-server.default.local")
        .put(String.format(keyPattern, volumeNameValid, "readOnly"), "true")
        .put(String.format(keyPattern, volumeNameValid, "pathOnNFS"), passingValue)
        .put(String.format(keyPattern, volumeNameValid, "path"), passingValue)
        .put(String.format(keyPattern, volumeNameValid, "subPath"), passingValue)
        .put(String.format(keyPattern, volumeNameValid, "readOnly"), passingValue)
        .build();
    testCases.add(new TestTuple<>(processName + ": `NFS` ignored",
        new Pair<>(configIgnored, !isExecutor), new HashMap<>()));
  }

  @Test
  public void testGetVolumeNFS() {
    final List<TestTuple<Pair<Config, Boolean>, Map<String, Map<VolumeConfigKeys, String>>>>
        testCases = new LinkedList<>();
    createVolumeNFS(testCases, true);
    createVolumeNFS(testCases, false);

    // Testing loop.
    for (TestTuple<Pair<Config, Boolean>, Map<String, Map<VolumeConfigKeys, String>>> testCase
        : testCases) {
      Map<String, Map<VolumeConfigKeys, String>> actual =
          KubernetesContext.getVolumeNFS(testCase.input.first, testCase.input.second);
      Assert.assertEquals(testCase.description, testCase.expected, actual);
    }
  }
  /**
   * Create test cases for <code>NFS</code> errors.
   * @param testCases Test case container.
   *                  Input: [0] Config, [1] Boolean to indicate Manager/Executor.
   *                  Output: Error message
   * @param isExecutor Boolean to indicate Manager/Executor test case generation.
   */
  private void createVolumeNFSError(
      List<TestTuple<Pair<Config, Boolean>, String>> testCases, boolean isExecutor) {
    final String volumeNameValid = "volume-name-valid";
    final String passingValue = "should-pass";
    final String failureValue = "Should-Fail";
    final String processName = isExecutor ? KubernetesConstants.EXECUTOR_NAME
        : KubernetesConstants.MANAGER_NAME;
    final String keyPattern = String.format(KubernetesContext.KUBERNETES_VOLUME_NFS_PREFIX
        + "%%s.%%s", processName);

    // Server is missing.
    final Config configNoServer = Config.newBuilder()
        .put(String.format(keyPattern, volumeNameValid, "readOnly"), "false")
        .put(String.format(keyPattern, volumeNameValid, "path"), passingValue)
        .put(String.format(keyPattern, volumeNameValid, "pathOnNFS"), passingValue)
        .put(String.format(keyPattern, volumeNameValid, "subPath"), passingValue)
        .build();
    testCases.add(new TestTuple<>(processName + ": No `server` should trigger exception",
        new Pair<>(configNoServer, isExecutor), "`NFS` volumes require a"));

    // Server is invalid.
    final Config configInvalidServer = Config.newBuilder()
        .put(String.format(keyPattern, volumeNameValid, "server"), "")
        .put(String.format(keyPattern, volumeNameValid, "readOnly"), "false")
        .put(String.format(keyPattern, volumeNameValid, "pathOnNFS"), passingValue)
        .put(String.format(keyPattern, volumeNameValid, "path"), passingValue)
        .put(String.format(keyPattern, volumeNameValid, "subPath"), passingValue)
        .build();
    testCases.add(new TestTuple<>(processName + ": Invalid `server` should trigger exception",
        new Pair<>(configInvalidServer, isExecutor), "`NFS` volumes require a"));

    // Path on NFS missing.
    final Config configNoNFSPath = Config.newBuilder()
        .put(String.format(keyPattern, volumeNameValid, "server"), "nfs-server.default.local")
        .put(String.format(keyPattern, volumeNameValid, "readOnly"), "false")
        .put(String.format(keyPattern, volumeNameValid, "path"), passingValue)
        .put(String.format(keyPattern, volumeNameValid, "subPath"), passingValue)
        .build();
    testCases.add(new TestTuple<>(processName + ": No path on NFS should trigger exception",
        new Pair<>(configNoNFSPath, isExecutor), "NFS requires a path on"));

    // Path on NFS is empty.
    final Config configEmptyNFSPath = Config.newBuilder()
        .put(String.format(keyPattern, volumeNameValid, "server"), "nfs-server.default.local")
        .put(String.format(keyPattern, volumeNameValid, "readOnly"), "false")
        .put(String.format(keyPattern, volumeNameValid, "pathOnNFS"), "")
        .put(String.format(keyPattern, volumeNameValid, "path"), passingValue)
        .put(String.format(keyPattern, volumeNameValid, "subPath"), passingValue)
        .build();
    testCases.add(new TestTuple<>(processName + ": No path on NFS should trigger exception",
        new Pair<>(configEmptyNFSPath, isExecutor), "NFS requires a path on"));

    // Invalid option.
    final Config configInvalidOption = Config.newBuilder()
        .put(String.format(keyPattern, volumeNameValid, "server"), "nfs-server.default.local")
        .put(String.format(keyPattern, volumeNameValid, "readOnly"), "false")
        .put(String.format(keyPattern, volumeNameValid, "pathOnNFS"), passingValue)
        .put(String.format(keyPattern, volumeNameValid, "path"), passingValue)
        .put(String.format(keyPattern, volumeNameValid, "subPath"), passingValue)
        .put(String.format(keyPattern, volumeNameValid, "accessModes"), passingValue)
        .build();
    testCases.add(new TestTuple<>(processName + ": Invalid option should trigger exception",
        new Pair<>(configInvalidOption, isExecutor), "Invalid NFS option"));
  }

  @Test
  public void testGetVolumeNFSErrors() {
    final List<TestTuple<Pair<Config, Boolean>, String>> testCases = new LinkedList<>();
    createVolumeNFSError(testCases, true);
    createVolumeNFSError(testCases, false);

    // Testing loop.
    for (TestTuple<Pair<Config, Boolean>, String> testCase : testCases) {
      String message = "";
      try {
        KubernetesContext.getVolumeNFS(testCase.input.first, testCase.input.second);
      } catch (TopologySubmissionException e) {
        message = e.getMessage();
      }
      Assert.assertTrue(testCase.description, message.contains(testCase.expected));
    }
  }
}
