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

import java.util.LinkedList;
import java.util.List;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Spy;
import org.mockito.runners.MockitoJUnitRunner;

import org.apache.heron.common.basics.Pair;
import org.apache.heron.scheduler.TopologySubmissionException;
import org.apache.heron.scheduler.kubernetes.KubernetesUtils.TestTuple;
import org.apache.heron.spi.common.Config;
import org.apache.heron.spi.common.Key;

import io.kubernetes.client.openapi.models.V1ConfigMap;
import io.kubernetes.client.openapi.models.V1ConfigMapBuilder;
import io.kubernetes.client.openapi.models.V1PodTemplateSpec;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doReturn;

@RunWith(MockitoJUnitRunner.class)
public class KubernetesShimTest {

  private static final String TOPOLOGY_NAME = "topology-name";
  private static final String CONFIGMAP_NAME = "CONFIG-MAP-NAME";
  private static final String POD_TEMPLATE_NAME = "POD-TEMPLATE-NAME";
  private static final String CONFIGMAP_POD_TEMPLATE_NAME =
      String.format("%s.%s", CONFIGMAP_NAME, POD_TEMPLATE_NAME);
  private static final String POD_TEMPLATE_VALID =
      "apiVersion: apps/v1\n"
          + "kind: PodTemplate\n"
          + "metadata:\n"
          + "  name: heron-tracker\n"
          + "  namespace: default\n"
          + "template:\n"
          + "  metadata:\n"
          + "    labels:\n"
          + "      app: heron-tracker\n"
          + "  spec:\n"
          + "    containers:\n"
          + "      - name: heron-tracker\n"
          + "        image: apache/heron:latest\n"
          + "        ports:\n"
          + "          - containerPort: 8888\n"
          + "            name: api-port\n"
          + "        resources:\n"
          + "          requests:\n"
          + "            cpu: \"100m\"\n"
          + "            memory: \"200M\"\n"
          + "          limits:\n"
          + "            cpu: \"400m\"\n"
          + "            memory: \"512M\"";
  private static final String POD_TEMPLATE_LOCATION_EXECUTOR =
      String.format(KubernetesContext.KUBERNETES_POD_TEMPLATE_LOCATION,
          KubernetesConstants.EXECUTOR_NAME);
  private static final String POD_TEMPLATE_LOCATION_MANAGER =
      String.format(KubernetesContext.KUBERNETES_POD_TEMPLATE_LOCATION,
          KubernetesConstants.MANAGER_NAME);

  private static final Config CONFIG = Config.newBuilder().build();
  private static final Config CONFIG_WITH_POD_TEMPLATE = Config.newBuilder()
      .put(POD_TEMPLATE_LOCATION_EXECUTOR, CONFIGMAP_POD_TEMPLATE_NAME)
      .put(POD_TEMPLATE_LOCATION_MANAGER, CONFIGMAP_POD_TEMPLATE_NAME)
      .build();
  private static final Config RUNTIME = Config.newBuilder()
      .put(Key.TOPOLOGY_NAME, TOPOLOGY_NAME)
      .build();
  private final Config configDisabledPodTemplate = Config.newBuilder()
      .put(POD_TEMPLATE_LOCATION_EXECUTOR, CONFIGMAP_POD_TEMPLATE_NAME)
      .put(POD_TEMPLATE_LOCATION_MANAGER, CONFIGMAP_POD_TEMPLATE_NAME)
      .put(KubernetesContext.KUBERNETES_POD_TEMPLATE_DISABLED, "true")
      .build();

  @Spy
  private final KubernetesShim v1ControllerWithPodTemplate =
      new KubernetesShim(CONFIG_WITH_POD_TEMPLATE, RUNTIME);

  @Spy
  private final KubernetesShim v1ControllerPodTemplate =
      new KubernetesShim(configDisabledPodTemplate, RUNTIME);

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Test
  public void testLoadPodFromTemplateDefault() {
    final KubernetesShim v1ControllerNoPodTemplate = new KubernetesShim(CONFIG, RUNTIME);
    final V1PodTemplateSpec defaultPodSpec = new V1PodTemplateSpec();

    final V1PodTemplateSpec podSpecExecutor = v1ControllerNoPodTemplate.loadPodFromTemplate(true);
    Assert.assertEquals("Default Pod Spec for Executor", defaultPodSpec, podSpecExecutor);

    final V1PodTemplateSpec podSpecManager = v1ControllerNoPodTemplate.loadPodFromTemplate(false);
    Assert.assertEquals("Default Pod Spec for Manager", defaultPodSpec, podSpecManager);
  }

  @Test
  public void testLoadPodFromTemplateNullConfigMap() {
    final List<TestTuple<Boolean, String>> testCases = new LinkedList<>();
    testCases.add(new TestTuple<>("Executor not found", true, "unable to locate"));
    testCases.add(new TestTuple<>("Manager not found", false, "unable to locate"));

    for (TestTuple<Boolean, String> testCase : testCases) {
      doReturn(null)
          .when(v1ControllerWithPodTemplate)
          .getConfigMap(anyString());

      String message = "";
      try {
        v1ControllerWithPodTemplate.loadPodFromTemplate(testCase.input);
      } catch (TopologySubmissionException e) {
        message = e.getMessage();
      }
      Assert.assertTrue(testCase.description, message.contains(testCase.expected));
    }
  }

  @Test
  public void testLoadPodFromTemplateNoConfigMap() {
    final List<TestTuple<Boolean, String>> testCases = new LinkedList<>();
    testCases.add(new TestTuple<>("Executor no ConfigMap", true, "Failed to locate Pod Template"));
    testCases.add(new TestTuple<>("Manager no ConfigMap", false, "Failed to locate Pod Template"));

    for (TestTuple<Boolean, String> testCase : testCases) {
      doReturn(new V1ConfigMap())
          .when(v1ControllerWithPodTemplate)
          .getConfigMap(anyString());

      String message = "";
      try {
        v1ControllerWithPodTemplate.loadPodFromTemplate(testCase.input);
      } catch (TopologySubmissionException e) {
        message = e.getMessage();
      }
      Assert.assertTrue(testCase.description, message.contains(testCase.expected));
    }
  }

  @Test
  public void testLoadPodFromTemplateNoTargetConfigMap() {
    final List<TestTuple<Boolean, String>> testCases = new LinkedList<>();
    testCases.add(new TestTuple<>("Executor no target ConfigMap",
        true, "Failed to locate Pod Template"));
    testCases.add(new TestTuple<>("Manager no target ConfigMap",
        false, "Failed to locate Pod Template"));

    final V1ConfigMap configMapNoTargetData = new V1ConfigMapBuilder()
        .withNewMetadata()
          .withName(CONFIGMAP_NAME)
        .endMetadata()
        .addToData("Dummy Key", "Dummy Value")
        .build();

    for (TestTuple<Boolean, String> testCase : testCases) {
      doReturn(configMapNoTargetData)
          .when(v1ControllerWithPodTemplate)
          .getConfigMap(anyString());

      String message = "";
      try {
        v1ControllerWithPodTemplate.loadPodFromTemplate(testCase.input);
      } catch (TopologySubmissionException e) {
        message = e.getMessage();
      }
      Assert.assertTrue(testCase.description, message.contains(testCase.expected));
    }
  }

  @Test
  public void testLoadPodFromTemplateBadTargetConfigMap() {
    // ConfigMap with target ConfigMap and an invalid Pod Template.
    final V1ConfigMap configMapInvalidPod = new V1ConfigMapBuilder()
        .withNewMetadata()
          .withName(CONFIGMAP_NAME)
        .endMetadata()
        .addToData(POD_TEMPLATE_NAME, "Dummy Value")
        .build();

    // ConfigMap with target ConfigMaps and an empty Pod Template.
    final V1ConfigMap configMapEmptyPod = new V1ConfigMapBuilder()
        .withNewMetadata()
        .withName(CONFIGMAP_NAME)
        .endMetadata()
        .addToData(POD_TEMPLATE_NAME, "")
        .build();

    // Test case container.
    // Input: ConfigMap to setup mock KubernetesShim, Boolean flag for executor/manager switch.
    // Output: The expected error message.
    final List<TestTuple<Pair<V1ConfigMap, Boolean>, String>> testCases = new LinkedList<>();
    testCases.add(new TestTuple<>("Executor invalid Pod Template",
        new Pair<>(configMapInvalidPod, true), "Error parsing"));
    testCases.add(new TestTuple<>("Manager invalid Pod Template",
        new Pair<>(configMapInvalidPod, false), "Error parsing"));
    testCases.add(new TestTuple<>("Executor empty Pod Template",
        new Pair<>(configMapEmptyPod, true), "Error parsing"));
    testCases.add(new TestTuple<>("Manager empty Pod Template",
        new Pair<>(configMapEmptyPod, false), "Error parsing"));

    // Test loop.
    for (TestTuple<Pair<V1ConfigMap, Boolean>, String> testCase : testCases) {
      doReturn(testCase.input.first)
          .when(v1ControllerWithPodTemplate)
          .getConfigMap(anyString());

      String message = "";
      try {
        v1ControllerWithPodTemplate.loadPodFromTemplate(testCase.input.second);
      } catch (TopologySubmissionException e) {
        message = e.getMessage();
      }
      Assert.assertTrue(testCase.description, message.contains(testCase.expected));
    }
  }

  @Test
  public void testLoadPodFromTemplateValidConfigMap() {
    final String expected =
        "        containers: [class V1Container {\n"
        + "            args: null\n"
        + "            command: null\n"
        + "            env: null\n"
        + "            envFrom: null\n"
        + "            image: apache/heron:latest\n"
        + "            imagePullPolicy: null\n"
        + "            lifecycle: null\n"
        + "            livenessProbe: null\n"
        + "            name: heron-tracker\n"
        + "            ports: [class V1ContainerPort {\n"
        + "                containerPort: 8888\n"
        + "                hostIP: null\n"
        + "                hostPort: null\n"
        + "                name: api-port\n"
        + "                protocol: null\n"
        + "            }]\n"
        + "            readinessProbe: null\n"
        + "            resources: class V1ResourceRequirements {\n"
        + "                limits: {cpu=Quantity{number=0.400, format=DECIMAL_SI}, "
        + "memory=Quantity{number=512000000, format=DECIMAL_SI}}\n"
        + "                requests: {cpu=Quantity{number=0.100, format=DECIMAL_SI}, "
        + "memory=Quantity{number=200000000, format=DECIMAL_SI}}\n"
        + "            }\n"
        + "            securityContext: null\n"
        + "            startupProbe: null\n"
        + "            stdin: null\n"
        + "            stdinOnce: null\n"
        + "            terminationMessagePath: null\n"
        + "            terminationMessagePolicy: null\n"
        + "            tty: null\n"
        + "            volumeDevices: null\n"
        + "            volumeMounts: null\n"
        + "            workingDir: null\n"
        + "        }]";


    // ConfigMap with valid Pod Template.
    final V1ConfigMap configMapValidPod = new V1ConfigMapBuilder()
        .withNewMetadata()
          .withName(CONFIGMAP_NAME)
        .endMetadata()
        .addToData(POD_TEMPLATE_NAME, POD_TEMPLATE_VALID)
        .build();

    // Test case container.
    // Input: ConfigMap to setup mock KubernetesShim, Boolean flag for executor/manager switch.
    // Output: The expected Pod template as a string.
    final List<TestTuple<Pair<V1ConfigMap, Boolean>, String>> testCases = new LinkedList<>();
    testCases.add(new TestTuple<>("Executor valid Pod Template",
        new Pair<>(configMapValidPod, true), expected));
    testCases.add(new TestTuple<>("Manager valid Pod Template",
        new Pair<>(configMapValidPod, false), expected));

    // Test loop.
    for (TestTuple<Pair<V1ConfigMap, Boolean>, String> testCase : testCases) {
      doReturn(testCase.input.first)
          .when(v1ControllerWithPodTemplate)
          .getConfigMap(anyString());

      V1PodTemplateSpec podTemplateSpec = v1ControllerWithPodTemplate.loadPodFromTemplate(true);

      Assert.assertTrue(podTemplateSpec.toString().contains(testCase.expected));
    }
  }

  @Test
  public void testLoadPodFromTemplateInvalidConfigMap() {
    // ConfigMap with an invalid Pod Template.
    final String invalidPodTemplate =
        "apiVersion: apps/v1\n"
            + "kind: InvalidTemplate\n"
            + "metadata:\n"
            + "  name: heron-tracker\n"
            + "  namespace: default\n"
            + "template:\n"
            + "  metadata:\n"
            + "    labels:\n"
            + "      app: heron-tracker\n"
            + "  spec:\n";
    final V1ConfigMap configMap = new V1ConfigMapBuilder()
        .withNewMetadata()
          .withName(CONFIGMAP_NAME)
        .endMetadata()
        .addToData(POD_TEMPLATE_NAME, invalidPodTemplate)
        .build();


    // Test case container.
    // Input: ConfigMap to setup mock KubernetesShim, Boolean flag for executor/manager switch.
    // Output: The expected Pod template as a string.
    final List<TestTuple<Pair<V1ConfigMap, Boolean>, String>> testCases = new LinkedList<>();
    testCases.add(new TestTuple<>("Executor invalid Pod Template",
        new Pair<>(configMap, true), "Error parsing"));
    testCases.add(new TestTuple<>("Manager invalid Pod Template",
        new Pair<>(configMap, false), "Error parsing"));

    // Test loop.
    for (TestTuple<Pair<V1ConfigMap, Boolean>, String> testCase : testCases) {
      doReturn(testCase.input.first)
          .when(v1ControllerWithPodTemplate)
          .getConfigMap(anyString());

      String message = "";
      try {
        v1ControllerWithPodTemplate.loadPodFromTemplate(testCase.input.second);
      } catch (TopologySubmissionException e) {
        message = e.getMessage();
      }
      Assert.assertTrue(message.contains(testCase.expected));
    }
  }

  @Test
  public void testDisablePodTemplates() {
    // ConfigMap with valid Pod Template.
    V1ConfigMap configMapValidPod = new V1ConfigMapBuilder()
        .withNewMetadata()
          .withName(CONFIGMAP_NAME)
        .endMetadata()
        .addToData(POD_TEMPLATE_NAME, POD_TEMPLATE_VALID)
        .build();
    final String expected = "Pod Templates are disabled";
    String message = "";
    doReturn(configMapValidPod)
        .when(v1ControllerPodTemplate)
        .getConfigMap(anyString());

    try {
      v1ControllerPodTemplate.loadPodFromTemplate(true);
    } catch (TopologySubmissionException e) {
      message = e.getMessage();
    }
    Assert.assertTrue(message.contains(expected));
  }

  @Test
  public void testGetPodTemplateLocationPassing() {
    final Config testConfig = Config.newBuilder()
        .put(POD_TEMPLATE_LOCATION_EXECUTOR, CONFIGMAP_POD_TEMPLATE_NAME)
        .build();
    final KubernetesShim kubernetesShim = new KubernetesShim(testConfig, RUNTIME);
    final Pair<String, String> expected = new Pair<>(CONFIGMAP_NAME, POD_TEMPLATE_NAME);

    // Correct parsing
    final Pair<String, String> actual = kubernetesShim.getPodTemplateLocation(true);
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testGetPodTemplateLocationNoConfigMap() {
    expectedException.expect(TopologySubmissionException.class);
    final Config testConfig = Config.newBuilder()
        .put(POD_TEMPLATE_LOCATION_EXECUTOR, ".POD-TEMPLATE-NAME").build();
    KubernetesShim kubernetesShim = new KubernetesShim(testConfig, RUNTIME);
    kubernetesShim.getPodTemplateLocation(true);
  }

  @Test
  public void testGetPodTemplateLocationNoPodTemplate() {
    expectedException.expect(TopologySubmissionException.class);
    final Config testConfig = Config.newBuilder()
        .put(POD_TEMPLATE_LOCATION_EXECUTOR, "CONFIGMAP-NAME.").build();
    KubernetesShim kubernetesShim = new KubernetesShim(testConfig, RUNTIME);
    kubernetesShim.getPodTemplateLocation(true);
  }

  @Test
  public void testGetPodTemplateLocationNoDelimiter() {
    expectedException.expect(TopologySubmissionException.class);
    final Config testConfig = Config.newBuilder()
        .put(POD_TEMPLATE_LOCATION_EXECUTOR, "CONFIGMAP-NAMEPOD-TEMPLATE-NAME").build();
    KubernetesShim kubernetesShim = new KubernetesShim(testConfig, RUNTIME);
    kubernetesShim.getPodTemplateLocation(true);
  }
}
