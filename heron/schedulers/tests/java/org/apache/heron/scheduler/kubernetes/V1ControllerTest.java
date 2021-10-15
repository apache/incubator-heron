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
import java.util.LinkedList;
import java.util.List;
import java.util.stream.IntStream;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Spy;
import org.mockito.runners.MockitoJUnitRunner;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.heron.common.basics.ByteAmount;
import org.apache.heron.common.basics.Pair;
import org.apache.heron.scheduler.TopologySubmissionException;
import org.apache.heron.spi.common.Config;
import org.apache.heron.spi.common.Key;
import org.apache.heron.spi.packing.Resource;

import io.kubernetes.client.custom.Quantity;
import io.kubernetes.client.openapi.models.V1ConfigMap;
import io.kubernetes.client.openapi.models.V1ConfigMapBuilder;
import io.kubernetes.client.openapi.models.V1Container;
import io.kubernetes.client.openapi.models.V1ContainerBuilder;
import io.kubernetes.client.openapi.models.V1ContainerPort;
import io.kubernetes.client.openapi.models.V1EnvVar;
import io.kubernetes.client.openapi.models.V1EnvVarSource;
import io.kubernetes.client.openapi.models.V1ObjectFieldSelector;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1PodSpec;
import io.kubernetes.client.openapi.models.V1PodSpecBuilder;
import io.kubernetes.client.openapi.models.V1PodTemplateSpec;
import io.kubernetes.client.openapi.models.V1ResourceRequirements;
import io.kubernetes.client.openapi.models.V1Volume;
import io.kubernetes.client.openapi.models.V1VolumeBuilder;
import io.kubernetes.client.openapi.models.V1VolumeMount;
import io.kubernetes.client.openapi.models.V1VolumeMountBuilder;

import static org.mockito.Mockito.doReturn;

@RunWith(MockitoJUnitRunner.class)
public class V1ControllerTest {

  private static final String TOPOLOGY_NAME = "topology-name";
  private static final String CONFIGMAP_POD_TEMPLATE_NAME = "CONFIG-MAP-NAME.POD-TEMPLATE-NAME";
  private static final String CONFIGMAP_NAME = "CONFIG-MAP-NAME";
  private static final String POD_TEMPLATE_NAME = "POD-TEMPLATE-NAME";
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

  private final Config config = Config.newBuilder().build();
  private final Config configWithPodTemplate = Config.newBuilder()
      .put(KubernetesContext.KUBERNETES_POD_TEMPLATE_CONFIGMAP_NAME, CONFIGMAP_POD_TEMPLATE_NAME)
      .build();
  private final Config runtime = Config.newBuilder()
      .put(Key.TOPOLOGY_NAME, TOPOLOGY_NAME)
      .build();
  private final Config configDisabledPodTemplate = Config.newBuilder()
      .put(KubernetesContext.KUBERNETES_POD_TEMPLATE_CONFIGMAP_NAME, CONFIGMAP_POD_TEMPLATE_NAME)
      .put(KubernetesContext.KUBERNETES_POD_TEMPLATE_CONFIGMAP_DISABLED, "true")
      .build();

  private final LinkedList<V1ConfigMap> emptyConfigMapList;
  private final LinkedList<V1ConfigMap> nonTargetConfigMapList;
  private final V1ConfigMap configMapWithNonTargetData;

  @Spy
  private final V1Controller v1ControllerWithPodTemplate =
      new V1Controller(configWithPodTemplate, runtime);

  @Spy
  private final V1Controller v1ControllerPodTemplate =
      new V1Controller(configDisabledPodTemplate, runtime);

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  public V1ControllerTest() {

    // ConfigMap List with empty and null data.
    V1ConfigMap emptyConfigMap = new V1ConfigMap();
    emptyConfigMapList = new LinkedList<>(
        Arrays.asList(emptyConfigMap, emptyConfigMap, null, emptyConfigMap, null, emptyConfigMap));
    int index = 0;
    for (V1ConfigMap configMap : emptyConfigMapList) {
      if (configMap == null) {
        continue;
      }
      configMap.setMetadata(new V1ObjectMeta().name("some-config-map-name" + (++index)));
    }

    // ConfigMap List with empty and non-target maps.
    configMapWithNonTargetData = new V1ConfigMap();
    configMapWithNonTargetData.setMetadata(new V1ObjectMeta().name(CONFIGMAP_NAME));
    configMapWithNonTargetData.putDataItem("Dummy Key", "Dummy Value");
    nonTargetConfigMapList = new LinkedList<>(emptyConfigMapList);
    nonTargetConfigMapList.add(configMapWithNonTargetData);
  }

  @Test
  public void testLoadPodFromTemplateDefault() {
    final V1Controller v1ControllerNoPodTemplate = new V1Controller(config, runtime);
    final V1PodTemplateSpec podSpec = v1ControllerNoPodTemplate.loadPodFromTemplate();

    Assert.assertEquals(podSpec, new V1PodTemplateSpec());
  }

  @Test
  public void testLoadPodFromTemplateNullConfigMaps() {
    final String expected = "No ConfigMaps";
    String message = "";

    doReturn(null).when(v1ControllerWithPodTemplate).getConfigMaps();
    try {
      v1ControllerWithPodTemplate.loadPodFromTemplate();
    } catch (TopologySubmissionException e) {
      message = e.getMessage();
    }
    Assert.assertTrue(message.contains(expected));
  }

  @Test
  public void testLoadPodFromTemplateNoConfigMaps() {
    final String expected = "Failed to locate Pod Template";
    String message = "";

    doReturn(new LinkedList<V1ConfigMap>()).when(v1ControllerWithPodTemplate).getConfigMaps();
    try {
      v1ControllerWithPodTemplate.loadPodFromTemplate();
    } catch (TopologySubmissionException e) {
      message = e.getMessage();
    }
    Assert.assertTrue(message.contains(expected));
  }

  @Test
  public void testLoadPodFromTemplateNoTargetConfigMaps() {
    final String expected = "Failed to locate Pod Template";
    String message = "";

    doReturn(emptyConfigMapList).when(v1ControllerWithPodTemplate).getConfigMaps();
    try {
      v1ControllerWithPodTemplate.loadPodFromTemplate();
    } catch (TopologySubmissionException e) {
      message = e.getMessage();
    }
    Assert.assertTrue(message.contains(expected));

    doReturn(nonTargetConfigMapList).when(v1ControllerWithPodTemplate).getConfigMaps();
    try {
      v1ControllerWithPodTemplate.loadPodFromTemplate();
    } catch (TopologySubmissionException e) {
      message = e.getMessage();
    }
    Assert.assertTrue(message.contains(expected));
  }

  @Test
  public void testLoadPodFromTemplateBadTargetConfigMaps() {
    final String expected = "Error parsing";
    String message = "";

    // ConfigMap List without target ConfigMaps and an invalid Pod Template.
    V1ConfigMap configMapInvalidPod = new V1ConfigMapBuilder()
        .withNewMetadata()
          .withName(CONFIGMAP_NAME)
        .endMetadata()
        .addToData(POD_TEMPLATE_NAME, "Dummy Value")
        .build();
    final LinkedList<V1ConfigMap> invalidPodConfigMapList = new LinkedList<>(
        Arrays.asList(configMapWithNonTargetData, configMapInvalidPod));

    doReturn(invalidPodConfigMapList).when(v1ControllerWithPodTemplate).getConfigMaps();
    try {
      v1ControllerWithPodTemplate.loadPodFromTemplate();
    } catch (TopologySubmissionException e) {
      message = e.getMessage();
    }
    Assert.assertTrue(message.contains(expected));

    // ConfigMap List without target ConfigMaps and an empty Pod Template.
    V1ConfigMap configMapEmptyPod = new V1ConfigMapBuilder()
        .withNewMetadata()
          .withName(CONFIGMAP_NAME)
        .endMetadata()
        .addToData(POD_TEMPLATE_NAME, "")
        .build();
    final LinkedList<V1ConfigMap> emptyPodConfigMapList = new LinkedList<>(
        Arrays.asList(configMapWithNonTargetData, configMapEmptyPod));

    doReturn(emptyPodConfigMapList).when(v1ControllerWithPodTemplate).getConfigMaps();
    try {
      v1ControllerWithPodTemplate.loadPodFromTemplate();
    } catch (TopologySubmissionException e) {
      message = e.getMessage();
    }
    Assert.assertTrue(message.contains(expected));
  }

  @Test
  public void testLoadPodFromTemplateValidConfigMaps() {
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


    // ConfigMap List with valid Pod Template.
    V1ConfigMap configMapValidPod = new V1ConfigMapBuilder()
        .withNewMetadata()
          .withName(CONFIGMAP_NAME)
        .endMetadata()
        .addToData(POD_TEMPLATE_NAME, POD_TEMPLATE_VALID)
        .build();
    final LinkedList<V1ConfigMap> validPodConfigMapList = new LinkedList<>(
        Arrays.asList(configMapWithNonTargetData, configMapValidPod));

    doReturn(validPodConfigMapList).when(v1ControllerWithPodTemplate).getConfigMaps();
    V1PodTemplateSpec podTemplateSpec = v1ControllerWithPodTemplate.loadPodFromTemplate();

    Assert.assertTrue(podTemplateSpec.toString().contains(expected));
  }

  @Test
  public void testLoadPodFromTemplateInvalidConfigMaps() {
    // ConfigMap List with an invalid Pod Template.
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
    V1ConfigMap configMap = new V1ConfigMapBuilder()
        .withNewMetadata()
          .withName(CONFIGMAP_NAME)
        .endMetadata()
        .addToData(POD_TEMPLATE_NAME, invalidPodTemplate)
        .build();
    final LinkedList<V1ConfigMap> configMapList =
        new LinkedList<>(Collections.singletonList(configMap));

    final String expected = "Error parsing";
    String message = "";

    doReturn(configMapList).when(v1ControllerWithPodTemplate).getConfigMaps();
    try {
      v1ControllerWithPodTemplate.loadPodFromTemplate();
    } catch (TopologySubmissionException e) {
      message = e.getMessage();
    }
    Assert.assertTrue(message.contains(expected));
  }

  @Test
  public void testDisablePodTemplates() {
    // ConfigMap List with valid Pod Template.
    V1ConfigMap configMapValidPod = new V1ConfigMapBuilder()
        .withNewMetadata()
          .withName(CONFIGMAP_NAME)
        .endMetadata()
        .addToData(POD_TEMPLATE_NAME, POD_TEMPLATE_VALID)
        .build();
    final LinkedList<V1ConfigMap> validPodConfigMapList = new LinkedList<>(
        Arrays.asList(configMapWithNonTargetData, configMapValidPod));
    final String expected = "Pod Templates are disabled";
    doReturn(validPodConfigMapList).when(v1ControllerPodTemplate).getConfigMaps();

    try {
      v1ControllerPodTemplate.loadPodFromTemplate();
    } catch (TopologySubmissionException e) {
      Assert.assertTrue(e.getMessage().contains(expected));
    }
  }

  @Test
  public void testGetPodTemplateLocationPassing() {
    final Config testConfig = Config.newBuilder()
        .put(KubernetesContext.KUBERNETES_POD_TEMPLATE_CONFIGMAP_NAME, CONFIGMAP_POD_TEMPLATE_NAME)
        .build();
    final V1Controller v1Controller = new V1Controller(testConfig, runtime);
    final Pair<String, String> expected = new Pair<>(CONFIGMAP_NAME, POD_TEMPLATE_NAME);
    Pair<String, String> actual;

    // Correct parsing
    actual = v1Controller.getPodTemplateLocation();
    Assert.assertEquals(actual, expected);
  }

  @Test
  public void testGetPodTemplateLocationNoConfigMap() {
    expectedException.expect(TopologySubmissionException.class);
    final Config testConfig = Config.newBuilder()
        .put(KubernetesContext.KUBERNETES_POD_TEMPLATE_CONFIGMAP_NAME,
        ".POD-TEMPLATE-NAME").build();
    V1Controller v1Controller = new V1Controller(testConfig, runtime);
    v1Controller.getPodTemplateLocation();
  }

  @Test
  public void testGetPodTemplateLocationNoPodTemplate() {
    expectedException.expect(TopologySubmissionException.class);
    final Config testConfig = Config.newBuilder()
        .put(KubernetesContext.KUBERNETES_POD_TEMPLATE_CONFIGMAP_NAME,
        "CONFIGMAP-NAME.").build();
    V1Controller v1Controller = new V1Controller(testConfig, runtime);
    v1Controller.getPodTemplateLocation();
  }

  @Test
  public void testGetPodTemplateLocationNoDelimiter() {
    expectedException.expect(TopologySubmissionException.class);
    final Config testConfig = Config.newBuilder()
        .put(KubernetesContext.KUBERNETES_POD_TEMPLATE_CONFIGMAP_NAME,
        "CONFIGMAP-NAMEPOD-TEMPLATE-NAME").build();
    V1Controller v1Controller = new V1Controller(testConfig, runtime);
    v1Controller.getPodTemplateLocation();
  }

  @Test
  public void testConfigureContainerPorts() {
    final List<V1ContainerPort> expectedPorts = new LinkedList<V1ContainerPort>();
    KubernetesConstants.EXECUTOR_PORTS.forEach((p, v) -> {
      expectedPorts.add(new V1ContainerPort().name(p.getName()).containerPort(v));
    });

    // Null ports. This is the default case.
    final V1Container inputContainerWithNullPorts = new V1ContainerBuilder().build();
    v1ControllerWithPodTemplate.configureContainerPorts(false, 0, inputContainerWithNullPorts);
    Assert.assertTrue("Server and/or shell ports for container null ports did not match",
        CollectionUtils.containsAll(inputContainerWithNullPorts.getPorts(), expectedPorts));

    // Empty ports.
    final V1Container inputContainerWithEmptyPorts = new V1ContainerBuilder()
        .withPorts(new LinkedList<>())
        .build();
    v1ControllerWithPodTemplate.configureContainerPorts(false, 0, inputContainerWithEmptyPorts);
    Assert.assertTrue("Server and/or shell ports for container empty ports did not match",
        CollectionUtils.containsAll(inputContainerWithEmptyPorts.getPorts(), expectedPorts));

    // Port overriding. Builds <expected> on prior <expected> results.
    final List<V1ContainerPort> inputPorts = new LinkedList<V1ContainerPort>() {
      {
        add(new V1ContainerPort()
            .name("server-port-to-replace").containerPort(KubernetesConstants.SERVER_PORT));
        add(new V1ContainerPort()
            .name("shell-port-to-replace").containerPort(KubernetesConstants.SHELL_PORT));
        add(new V1ContainerPort()
            .name("random-port-to-be-kept").containerPort(1111));
      }
    };
    final V1Container inputContainerWithPorts = new V1ContainerBuilder()
        .withPorts(inputPorts)
        .build();
    expectedPorts.add(new V1ContainerPort().name("random-port-to-be-kept").containerPort(1111));

    v1ControllerWithPodTemplate.configureContainerPorts(false, 0, inputContainerWithPorts);
    Assert.assertTrue("Server and/or shell ports for container were not overwritten.",
        CollectionUtils.containsAll(inputContainerWithPorts.getPorts(), expectedPorts));

    // Port overriding with debug ports. Builds <expected> on prior <expected> results.
    final int numInstances = 3;
    final List<V1ContainerPort> debugPorts = new LinkedList<>();
    IntStream.range(0, numInstances).forEach(i -> {
      final V1ContainerPort port = new V1ContainerPort()
          .name(KubernetesConstants.JVM_REMOTE_DEBUGGER_PORT_NAME + "-" + i)
          .containerPort(KubernetesConstants.JVM_REMOTE_DEBUGGER_PORT + i);
      debugPorts.add(port);
    });
    final List<V1ContainerPort> inputPortsWithDebug = new LinkedList<V1ContainerPort>(inputPorts) {
      {
        add(new V1ContainerPort()
            .name("server-port-to-replace").containerPort(KubernetesConstants.SERVER_PORT));
        add(new V1ContainerPort()
            .name("shell-port-to-replace").containerPort(KubernetesConstants.SHELL_PORT));
        add(new V1ContainerPort()
            .name("random-port-to-be-kept").containerPort(1111));
      }
    };
    inputPortsWithDebug.addAll(debugPorts);
    final V1Container inputContainerWithDebug = new V1ContainerBuilder()
        .withPorts(inputPortsWithDebug)
        .build();

    expectedPorts.addAll(debugPorts);

    v1ControllerWithPodTemplate.configureContainerPorts(
        true, numInstances, inputContainerWithDebug);
    Assert.assertTrue("Server and/or shell ports for container were not overwritten.",
        CollectionUtils.containsAll(inputContainerWithDebug.getPorts(), expectedPorts));
  }

  @Test
  public void testConfigureContainerEnvVars() {
    final List<V1EnvVar> heronEnvVars = new LinkedList<V1EnvVar>() {
      {
        add(new V1EnvVar()
            .name(KubernetesConstants.ENV_HOST)
              .valueFrom(new V1EnvVarSource()
                .fieldRef(new V1ObjectFieldSelector()
                  .fieldPath(KubernetesConstants.POD_IP))));
        add(new V1EnvVar()
            .name(KubernetesConstants.ENV_POD_NAME)
              .valueFrom(new V1EnvVarSource()
                .fieldRef(new V1ObjectFieldSelector()
                  .fieldPath(KubernetesConstants.POD_NAME))));
      }
    };

    // Null env vars. This is the default case.
    V1Container containerWithNullEnvVars = new V1ContainerBuilder().build();
    v1ControllerWithPodTemplate.configureContainerEnvVars(containerWithNullEnvVars);
    Assert.assertTrue("ENV_HOST & ENV_POD_NAME in container with null Env Vars did not match",
        CollectionUtils.containsAll(containerWithNullEnvVars.getEnv(), heronEnvVars));

    // Empty env vars.
    V1Container containerWithEmptyEnvVars = new V1ContainerBuilder()
        .withEnv(new LinkedList<>())
        .build();
    v1ControllerWithPodTemplate.configureContainerEnvVars(containerWithEmptyEnvVars);
    Assert.assertTrue("ENV_HOST & ENV_POD_NAME in container with empty Env Vars did not match",
        CollectionUtils.containsAll(containerWithEmptyEnvVars.getEnv(), heronEnvVars));

    // Env Var overriding.
    final V1EnvVar additionEnvVar = new V1EnvVar()
        .name("env-variable-to-be-kept")
          .valueFrom(new V1EnvVarSource()
            .fieldRef(new V1ObjectFieldSelector()
                .fieldPath("env-variable-was-kept")));
    final List<V1EnvVar> inputEnvVars = new LinkedList<V1EnvVar>() {
      {
        add(new V1EnvVar()
            .name(KubernetesConstants.ENV_HOST)
            .valueFrom(new V1EnvVarSource()
                .fieldRef(new V1ObjectFieldSelector()
                    .fieldPath("env-host-to-be-replaced"))));
        add(new V1EnvVar()
            .name(KubernetesConstants.ENV_POD_NAME)
            .valueFrom(new V1EnvVarSource()
                .fieldRef(new V1ObjectFieldSelector()
                    .fieldPath("pod-name-to-be-replaced"))));
        add(additionEnvVar);
      }
    };
    heronEnvVars.add(additionEnvVar);
    V1Container containerWithEnvVars = new V1ContainerBuilder()
        .withEnv(inputEnvVars)
        .build();
    v1ControllerWithPodTemplate.configureContainerEnvVars(containerWithEnvVars);
    Assert.assertTrue("ENV_HOST & ENV_POD_NAME in container with Env Vars did not match",
        CollectionUtils.containsAll(containerWithEnvVars.getEnv(), heronEnvVars));
  }

  @Test
  public void testConfigureContainerResources() {
    final Resource resourceDefault = new Resource(
        9, ByteAmount.fromGigabytes(19), ByteAmount.fromGigabytes(99));
    final Resource resourceCustom = new Resource(
        4, ByteAmount.fromGigabytes(34), ByteAmount.fromGigabytes(400));
    final Config configNoLimit = Config.newBuilder()
        .put(KubernetesContext.KUBERNETES_RESOURCE_REQUEST_MODE, "NOT_SET")
        .build();
    final Config configWithLimit = Config.newBuilder()
        .put(KubernetesContext.KUBERNETES_RESOURCE_REQUEST_MODE, "EQUAL_TO_LIMIT")
        .build();

    final V1ResourceRequirements expectDefaultRequirements = new V1ResourceRequirements()
        .putLimitsItem(KubernetesConstants.MEMORY,
            Quantity.fromString(KubernetesUtils.Megabytes(
                resourceDefault.getRam())))
        .putLimitsItem(KubernetesConstants.CPU,
            Quantity.fromString(Double.toString(V1Controller.roundDecimal(
                resourceDefault.getCpu(), 3))));

    final V1ResourceRequirements expectCustomRequirements = new V1ResourceRequirements()
        .putLimitsItem(KubernetesConstants.MEMORY,
            Quantity.fromString(KubernetesUtils.Megabytes(
                resourceCustom.getRam())))
        .putLimitsItem(KubernetesConstants.CPU,
            Quantity.fromString(Double.toString(V1Controller.roundDecimal(
                resourceCustom.getCpu(), 3))))
        .putLimitsItem("disk",
            Quantity.fromString(Double.toString(V1Controller.roundDecimal(
                resourceCustom.getDisk().getValue(), 3))));

    // Default. Null resources.
    V1Container containerNull = new V1ContainerBuilder().build();
    v1ControllerWithPodTemplate.configureContainerResources(
        containerNull, configNoLimit, resourceDefault);
    Assert.assertTrue("Default LIMITS not set in container with null LIMITS",
        containerNull.getResources().getLimits().entrySet()
            .containsAll(expectDefaultRequirements.getLimits().entrySet()));

    // Empty resources.
    V1Container containerEmpty = new V1ContainerBuilder().withNewResources().endResources().build();
    v1ControllerWithPodTemplate.configureContainerResources(
        containerEmpty, configNoLimit, resourceDefault);
    Assert.assertTrue("Default LIMITS not set in container with empty LIMITS",
        containerNull.getResources().getLimits().entrySet()
            .containsAll(expectDefaultRequirements.getLimits().entrySet()));

    // Custom resources.
    V1Container containerCustom = new V1ContainerBuilder()
        .withResources(expectCustomRequirements)
        .build();
    v1ControllerWithPodTemplate.configureContainerResources(
        containerCustom, configNoLimit, resourceDefault);
    Assert.assertTrue("Custom LIMITS not set in container with custom LIMITS",
        containerCustom.getResources().getLimits().entrySet()
            .containsAll(expectCustomRequirements.getLimits().entrySet()));

    // Custom resources with request.
    V1Container containerRequests = new V1ContainerBuilder()
        .withResources(expectCustomRequirements)
        .build();
    v1ControllerWithPodTemplate.configureContainerResources(
        containerRequests, configWithLimit, resourceDefault);
    Assert.assertTrue("Custom LIMITS not set in container with custom LIMITS and REQUEST",
        containerRequests.getResources().getLimits().entrySet()
            .containsAll(expectCustomRequirements.getLimits().entrySet()));
    Assert.assertTrue("Custom REQUEST not set in container with custom LIMITS and REQUEST",
        containerRequests.getResources().getRequests().entrySet()
            .containsAll(expectCustomRequirements.getLimits().entrySet()));
  }

  @Test
  public void testAddVolumesIfPresent() {
    final String pathDefault = "config-host-volume-path";
    final String pathNameDefault = "config-host-volume-name";
    final Config configWithVolumes = Config.newBuilder()
        .put(KubernetesContext.KUBERNETES_VOLUME_NAME, pathNameDefault)
        .put(KubernetesContext.KUBERNETES_VOLUME_TYPE, Volumes.HOST_PATH)
        .put(KubernetesContext.KUBERNETES_VOLUME_HOSTPATH_PATH, pathDefault)
        .build();
    final V1Controller controllerWithVol = new V1Controller(configWithVolumes, runtime);

    final V1Volume volumeDefault = new V1VolumeBuilder()
        .withName(pathNameDefault)
        .withNewHostPath()
          .withNewPath(pathDefault)
        .endHostPath()
        .build();
    final V1Volume volumeToBeKept = new V1VolumeBuilder()
        .withName("volume-to-be-kept-name")
        .withNewHostPath()
          .withNewPath("volume-to-be-kept-path")
        .endHostPath()
        .build();

    final List<V1Volume> customVolumeList = new LinkedList<V1Volume>() {
      {
        add(new V1VolumeBuilder()
            .withName(pathNameDefault)
            .withNewHostPath()
              .withNewPath("this-path-must-be-replaced")
            .endHostPath()
            .build());
        add(volumeToBeKept);
      }
    };
    final List<V1Volume> expectedDefault = Collections.singletonList(volumeDefault);
    final List<V1Volume> expectedCustom = Arrays.asList(volumeDefault, volumeToBeKept);

    // Default. Null Volumes.
    V1PodSpec podSpecNull = new V1PodSpecBuilder().build();
    controllerWithVol.addVolumesIfPresent(podSpecNull);
    Assert.assertTrue("Default VOLUMES not set in container with null VOLUMES",
        CollectionUtils.containsAll(expectedDefault, podSpecNull.getVolumes()));

    // Empty Volumes list
    V1PodSpec podSpecEmpty = new V1PodSpecBuilder()
        .withVolumes(new LinkedList<>())
        .build();
    controllerWithVol.addVolumesIfPresent(podSpecEmpty);
    Assert.assertTrue("Default VOLUMES not set in container with empty VOLUMES",
        CollectionUtils.containsAll(expectedDefault, podSpecEmpty.getVolumes()));

    // Custom Volumes list
    V1PodSpec podSpecCustom = new V1PodSpecBuilder()
        .withVolumes(customVolumeList)
        .build();
    controllerWithVol.addVolumesIfPresent(podSpecCustom);
    Assert.assertTrue("Default VOLUMES not set in container with custom VOLUMES",
        CollectionUtils.containsAll(expectedCustom, podSpecCustom.getVolumes()));
  }

  @Test
  public void testMountVolumeIfPresent() {
    final String pathDefault = "config-host-volume-path";
    final String pathNameDefault = "config-host-volume-name";
    final Config configWithVolumes = Config.newBuilder()
        .put(KubernetesContext.KUBERNETES_CONTAINER_VOLUME_MOUNT_NAME, pathNameDefault)
        .put(KubernetesContext.KUBERNETES_CONTAINER_VOLUME_MOUNT_PATH, pathDefault)
        .build();
    final V1Controller controllerWithMounts = new V1Controller(configWithVolumes, runtime);
    final V1VolumeMount volumeDefault = new V1VolumeMountBuilder()
        .withName(pathNameDefault)
        .withMountPath(pathDefault)
        .build();
    final V1VolumeMount volumeCustom = new V1VolumeMountBuilder()
        .withName("custom-volume-mount")
        .withMountPath("should-be-kept")
        .build();

    final List<V1VolumeMount> expectedMountsDefault = Collections.singletonList(volumeDefault);
    final List<V1VolumeMount> expectedMountsCustom = Arrays.asList(volumeCustom, volumeDefault);
    final List<V1VolumeMount> volumeMountsCustomList = new LinkedList<V1VolumeMount>() {
      {
        add(volumeCustom);
        add(new V1VolumeMountBuilder()
            .withName(pathNameDefault)
            .withMountPath("should-be-replaced")
            .build());
      }
    };

    // No Volume Mounts set.
    V1Controller controllerDoNotSetMounts = new V1Controller(Config.newBuilder().build(), runtime);
    V1Container containerNoSetMounts = new V1Container();
    controllerDoNotSetMounts.mountVolumeIfPresent(containerNoSetMounts);
    Assert.assertNull(containerNoSetMounts.getVolumeMounts());

    // Default. Null Volume Mounts.
    V1Container containerNull = new V1ContainerBuilder().build();
    controllerWithMounts.mountVolumeIfPresent(containerNull);
    Assert.assertTrue("Default VOLUME MOUNTS not set in container with null VOLUMES MOUNTS",
        CollectionUtils.containsAll(expectedMountsDefault, containerNull.getVolumeMounts()));

    // Empty Volume Mounts.
    V1Container containerEmpty = new V1ContainerBuilder()
        .withVolumeMounts(new LinkedList<>())
        .build();
    controllerWithMounts.mountVolumeIfPresent(containerEmpty);
    Assert.assertTrue("Default VOLUME MOUNTS not set in container with empty VOLUMES MOUNTS",
        CollectionUtils.containsAll(expectedMountsDefault, containerEmpty.getVolumeMounts()));

    // Custom Volume Mounts.
    V1Container containerCustom = new V1ContainerBuilder()
        .withVolumeMounts(volumeMountsCustomList)
        .build();
    controllerWithMounts.mountVolumeIfPresent(containerCustom);
    Assert.assertTrue("Default VOLUME MOUNTS not set in container with custom VOLUMES MOUNTS",
        CollectionUtils.containsAll(expectedMountsCustom, containerCustom.getVolumeMounts()));
  }
}
