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
import org.apache.heron.scheduler.kubernetes.KubernetesUtils.TestTuple;
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
import io.kubernetes.client.openapi.models.V1PersistentVolumeClaim;
import io.kubernetes.client.openapi.models.V1PersistentVolumeClaimBuilder;
import io.kubernetes.client.openapi.models.V1PodSpec;
import io.kubernetes.client.openapi.models.V1PodSpecBuilder;
import io.kubernetes.client.openapi.models.V1PodTemplateSpec;
import io.kubernetes.client.openapi.models.V1ResourceRequirements;
import io.kubernetes.client.openapi.models.V1Toleration;
import io.kubernetes.client.openapi.models.V1Volume;
import io.kubernetes.client.openapi.models.V1VolumeBuilder;
import io.kubernetes.client.openapi.models.V1VolumeMount;
import io.kubernetes.client.openapi.models.V1VolumeMountBuilder;

import static org.apache.heron.scheduler.kubernetes.KubernetesConstants.VolumeClaimTemplateConfigKeys;
import static org.mockito.Matchers.anyMap;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doReturn;

@RunWith(MockitoJUnitRunner.class)
public class V1ControllerTest {

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

  @Spy
  private final V1Controller v1ControllerWithPodTemplate =
      new V1Controller(configWithPodTemplate, runtime);

  @Spy
  private final V1Controller v1ControllerPodTemplate =
      new V1Controller(configDisabledPodTemplate, runtime);

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Test
  public void testLoadPodFromTemplateDefault() {
    final V1Controller v1ControllerNoPodTemplate = new V1Controller(config, runtime);
    final V1PodTemplateSpec podSpec = v1ControllerNoPodTemplate.loadPodFromTemplate();

    Assert.assertEquals(podSpec, new V1PodTemplateSpec());
  }

  @Test
  public void testLoadPodFromTemplateNullConfigMap() {
    final String expected = "unable to locate";
    String message = "";

    doReturn(null)
        .when(v1ControllerWithPodTemplate)
        .getConfigMap(anyString());
    try {
      v1ControllerWithPodTemplate.loadPodFromTemplate();
    } catch (TopologySubmissionException e) {
      message = e.getMessage();
    }
    Assert.assertTrue(message.contains(expected));
  }

  @Test
  public void testLoadPodFromTemplateNoConfigMap() {
    final String expected = "Failed to locate Pod Template";
    String message = "";

    doReturn(new V1ConfigMap())
        .when(v1ControllerWithPodTemplate)
        .getConfigMap(anyString());
    try {
      v1ControllerWithPodTemplate.loadPodFromTemplate();
    } catch (TopologySubmissionException e) {
      message = e.getMessage();
    }
    Assert.assertTrue(message.contains(expected));
  }

  @Test
  public void testLoadPodFromTemplateNoTargetConfigMap() {
    final String expected = "Failed to locate Pod Template";
    String message = "";
    V1ConfigMap configMapNoTargetData = new V1ConfigMapBuilder()
        .withNewMetadata()
          .withName(CONFIGMAP_NAME)
        .endMetadata()
        .addToData("Dummy Key", "Dummy Value")
        .build();

    doReturn(configMapNoTargetData)
        .when(v1ControllerWithPodTemplate)
        .getConfigMap(anyString());
    try {
      v1ControllerWithPodTemplate.loadPodFromTemplate();
    } catch (TopologySubmissionException e) {
      message = e.getMessage();
    }
    Assert.assertTrue(message.contains(expected));
  }

  @Test
  public void testLoadPodFromTemplateBadTargetConfigMap() {
    final String expected = "Error parsing";
    String message = "";

    // ConfigMap with target ConfigMap and an invalid Pod Template.
    V1ConfigMap configMapInvalidPod = new V1ConfigMapBuilder()
        .withNewMetadata()
          .withName(CONFIGMAP_NAME)
        .endMetadata()
        .addToData(POD_TEMPLATE_NAME, "Dummy Value")
        .build();

    doReturn(configMapInvalidPod)
        .when(v1ControllerWithPodTemplate)
        .getConfigMap(anyString());
    try {
      v1ControllerWithPodTemplate.loadPodFromTemplate();
    } catch (TopologySubmissionException e) {
      message = e.getMessage();
    }
    Assert.assertTrue("Invalid Pod Template parsing should fail", message.contains(expected));

    // ConfigMap with target ConfigMaps and an empty Pod Template.
    V1ConfigMap configMapEmptyPod = new V1ConfigMapBuilder()
        .withNewMetadata()
          .withName(CONFIGMAP_NAME)
        .endMetadata()
        .addToData(POD_TEMPLATE_NAME, "")
        .build();

    doReturn(configMapEmptyPod)
        .when(v1ControllerWithPodTemplate)
        .getConfigMap(anyString());
    try {
      v1ControllerWithPodTemplate.loadPodFromTemplate();
    } catch (TopologySubmissionException e) {
      message = e.getMessage();
    }
    Assert.assertTrue("Empty Pod Template parsing should fail", message.contains(expected));
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
    V1ConfigMap configMapValidPod = new V1ConfigMapBuilder()
        .withNewMetadata()
          .withName(CONFIGMAP_NAME)
        .endMetadata()
        .addToData(POD_TEMPLATE_NAME, POD_TEMPLATE_VALID)
        .build();
    doReturn(configMapValidPod)
        .when(v1ControllerWithPodTemplate)
        .getConfigMap(anyString());
    V1PodTemplateSpec podTemplateSpec = v1ControllerWithPodTemplate.loadPodFromTemplate();

    Assert.assertTrue(podTemplateSpec.toString().contains(expected));
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
    V1ConfigMap configMap = new V1ConfigMapBuilder()
        .withNewMetadata()
          .withName(CONFIGMAP_NAME)
        .endMetadata()
        .addToData(POD_TEMPLATE_NAME, invalidPodTemplate)
        .build();
    final String expected = "Error parsing";
    String message = "";

    doReturn(configMap)
        .when(v1ControllerWithPodTemplate)
        .getConfigMap(anyString());
    try {
      v1ControllerWithPodTemplate.loadPodFromTemplate();
    } catch (TopologySubmissionException e) {
      message = e.getMessage();
    }
    Assert.assertTrue(message.contains(expected));
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
      v1ControllerPodTemplate.loadPodFromTemplate();
    } catch (TopologySubmissionException e) {
      message = e.getMessage();
    }
    Assert.assertTrue(message.contains(expected));
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
    final String portNamekept = "random-port-to-be-kept";
    final int portNumberkept = 1111;
    final int numInstances = 3;
    final List<V1ContainerPort> expectedPortsBase =
        Collections.unmodifiableList(V1Controller.getExecutorPorts());
    final List<V1ContainerPort> debugPorts =
        Collections.unmodifiableList(V1Controller.getDebuggingPorts(numInstances));
    final List<V1ContainerPort> inputPortsBase = Collections.unmodifiableList(
        Arrays.asList(
            new V1ContainerPort()
                .name("server-port-to-replace").containerPort(KubernetesConstants.SERVER_PORT),
            new V1ContainerPort()
                .name("shell-port-to-replace").containerPort(KubernetesConstants.SHELL_PORT),
            new V1ContainerPort().name(portNamekept).containerPort(portNumberkept)
        )
    );

    // Null ports. This is the default case.
    final V1Container inputContainerWithNullPorts = new V1ContainerBuilder().build();
    v1ControllerWithPodTemplate.configureContainerPorts(false, 0, inputContainerWithNullPorts);
    Assert.assertTrue("Server and/or shell PORTS for container with null ports list",
        CollectionUtils.containsAll(inputContainerWithNullPorts.getPorts(), expectedPortsBase));

    // Empty ports.
    final V1Container inputContainerWithEmptyPorts = new V1ContainerBuilder()
        .withPorts(new LinkedList<>())
        .build();
    v1ControllerWithPodTemplate.configureContainerPorts(false, 0, inputContainerWithEmptyPorts);
    Assert.assertTrue("Server and/or shell PORTS for container with empty ports list",
        CollectionUtils.containsAll(inputContainerWithEmptyPorts.getPorts(), expectedPortsBase));

    // Port overriding.
    final List<V1ContainerPort> inputPorts = new LinkedList<>(inputPortsBase);
    final V1Container inputContainerWithPorts = new V1ContainerBuilder()
        .withPorts(inputPorts)
        .build();
    final List<V1ContainerPort> expectedPortsOverriding = new LinkedList<>(expectedPortsBase);
    expectedPortsOverriding
        .add(new V1ContainerPort().name(portNamekept).containerPort(portNumberkept));

    v1ControllerWithPodTemplate.configureContainerPorts(false, 0, inputContainerWithPorts);
    Assert.assertTrue("Server and/or shell PORTS for container should be overwritten.",
        CollectionUtils.containsAll(inputContainerWithPorts.getPorts(), expectedPortsOverriding));

    // Port overriding with debug ports.
    final List<V1ContainerPort> inputPortsWithDebug = new LinkedList<>(debugPorts);
    inputPortsWithDebug.addAll(inputPortsBase);
    final V1Container inputContainerWithDebug = new V1ContainerBuilder()
        .withPorts(inputPortsWithDebug)
        .build();
    final List<V1ContainerPort> expectedPortsDebug = new LinkedList<>(expectedPortsBase);
    expectedPortsDebug.add(new V1ContainerPort().name(portNamekept).containerPort(portNumberkept));
    expectedPortsDebug.addAll(debugPorts);

    v1ControllerWithPodTemplate.configureContainerPorts(
        true, numInstances, inputContainerWithDebug);
    Assert.assertTrue("Server and/or shell with debug PORTS for container should be overwritten.",
        CollectionUtils.containsAll(inputContainerWithDebug.getPorts(), expectedPortsDebug));
  }

  @Test
  public void testConfigureContainerEnvVars() {
    final List<V1EnvVar> heronEnvVars =
        Collections.unmodifiableList(V1Controller.getExecutorEnvVars());
    final V1EnvVar additionEnvVar = new V1EnvVar()
        .name("env-variable-to-be-kept")
        .valueFrom(new V1EnvVarSource()
            .fieldRef(new V1ObjectFieldSelector()
                .fieldPath("env-variable-was-kept")));
    final List<V1EnvVar> inputEnvVars = Arrays.asList(
        new V1EnvVar()
            .name(KubernetesConstants.ENV_HOST)
            .valueFrom(new V1EnvVarSource()
                .fieldRef(new V1ObjectFieldSelector()
                    .fieldPath("env-host-to-be-replaced"))),
        new V1EnvVar()
            .name(KubernetesConstants.ENV_POD_NAME)
            .valueFrom(new V1EnvVarSource()
                .fieldRef(new V1ObjectFieldSelector()
                    .fieldPath("pod-name-to-be-replaced"))),
        additionEnvVar
    );

    // Null env vars. This is the default case.
    V1Container containerWithNullEnvVars = new V1ContainerBuilder().build();
    v1ControllerWithPodTemplate.configureContainerEnvVars(containerWithNullEnvVars);
    Assert.assertTrue("ENV_HOST & ENV_POD_NAME in container with null Env Vars should match",
        CollectionUtils.containsAll(containerWithNullEnvVars.getEnv(), heronEnvVars));

    // Empty env vars.
    V1Container containerWithEmptyEnvVars = new V1ContainerBuilder()
        .withEnv(new LinkedList<>())
        .build();
    v1ControllerWithPodTemplate.configureContainerEnvVars(containerWithEmptyEnvVars);
    Assert.assertTrue("ENV_HOST & ENV_POD_NAME in container with empty Env Vars should match",
        CollectionUtils.containsAll(containerWithEmptyEnvVars.getEnv(), heronEnvVars));

    // Env Var overriding.
    final List<V1EnvVar> expectedOverriding = new LinkedList<>(heronEnvVars);
    expectedOverriding.add(additionEnvVar);
    V1Container containerWithEnvVars = new V1ContainerBuilder()
        .withEnv(inputEnvVars)
        .build();
    v1ControllerWithPodTemplate.configureContainerEnvVars(containerWithEnvVars);
    Assert.assertTrue("ENV_HOST & ENV_POD_NAME in container with Env Vars should be overridden",
        CollectionUtils.containsAll(containerWithEnvVars.getEnv(), expectedOverriding));
  }

  @Test
  public void testConfigureContainerResources() {
    final Resource resourceDefault = new Resource(
        9, ByteAmount.fromGigabytes(19), ByteAmount.fromGigabytes(99));
    final Resource resourceCustom = new Resource(
        4, ByteAmount.fromGigabytes(34), ByteAmount.fromGigabytes(400));

    final Quantity defaultRAM = Quantity.fromString(
        KubernetesUtils.Megabytes(resourceDefault.getRam()));
    final Quantity defaultCPU = Quantity.fromString(
        Double.toString(V1Controller.roundDecimal(resourceDefault.getCpu(), 3)));
    final Quantity customRAM = Quantity.fromString(
        KubernetesUtils.Megabytes(resourceCustom.getRam()));
    final Quantity customCPU = Quantity.fromString(
        Double.toString(V1Controller.roundDecimal(resourceCustom.getCpu(), 3)));
    final Quantity customDisk = Quantity.fromString(
        Double.toString(V1Controller.roundDecimal(resourceCustom.getDisk().getValue(), 3)));

    final Config configNoLimit = Config.newBuilder()
        .put(KubernetesContext.KUBERNETES_RESOURCE_REQUEST_MODE, "NOT_SET")
        .build();
    final Config configWithLimit = Config.newBuilder()
        .put(KubernetesContext.KUBERNETES_RESOURCE_REQUEST_MODE, "EQUAL_TO_LIMIT")
        .build();

    final V1ResourceRequirements expectDefaultRequirements = new V1ResourceRequirements()
        .putLimitsItem(KubernetesConstants.MEMORY, defaultRAM)
        .putLimitsItem(KubernetesConstants.CPU, defaultCPU);

    final V1ResourceRequirements expectCustomRequirements = new V1ResourceRequirements()
        .putLimitsItem(KubernetesConstants.MEMORY, defaultRAM)
        .putLimitsItem(KubernetesConstants.CPU, defaultCPU)
        .putLimitsItem("disk", customDisk);

    final V1ResourceRequirements customRequirements = new V1ResourceRequirements()
        .putLimitsItem(KubernetesConstants.MEMORY, customRAM)
        .putLimitsItem(KubernetesConstants.CPU, customCPU)
        .putLimitsItem("disk", customDisk);

    // Default. Null resources.
    V1Container containerNull = new V1ContainerBuilder().build();
    v1ControllerWithPodTemplate.configureContainerResources(
        containerNull, configNoLimit, resourceDefault);
    Assert.assertTrue("Default LIMITS should be set in container with null LIMITS",
        containerNull.getResources().getLimits().entrySet()
            .containsAll(expectDefaultRequirements.getLimits().entrySet()));

    // Empty resources.
    V1Container containerEmpty = new V1ContainerBuilder().withNewResources().endResources().build();
    v1ControllerWithPodTemplate.configureContainerResources(
        containerEmpty, configNoLimit, resourceDefault);
    Assert.assertTrue("Default LIMITS should be set in container with empty LIMITS",
        containerNull.getResources().getLimits().entrySet()
            .containsAll(expectDefaultRequirements.getLimits().entrySet()));

    // Custom resources.
    V1Container containerCustom = new V1ContainerBuilder()
        .withResources(customRequirements)
        .build();
    v1ControllerWithPodTemplate.configureContainerResources(
        containerCustom, configNoLimit, resourceDefault);
    Assert.assertTrue("Custom LIMITS should be set in container with custom LIMITS",
        containerCustom.getResources().getLimits().entrySet()
            .containsAll(expectCustomRequirements.getLimits().entrySet()));

    // Custom resources with request.
    V1Container containerRequests = new V1ContainerBuilder()
        .withResources(customRequirements)
        .build();
    v1ControllerWithPodTemplate.configureContainerResources(
        containerRequests, configWithLimit, resourceDefault);
    Assert.assertTrue("Custom LIMITS should be set in container with custom LIMITS and REQUEST",
        containerRequests.getResources().getLimits().entrySet()
            .containsAll(expectCustomRequirements.getLimits().entrySet()));
    Assert.assertTrue("Custom REQUEST should be set in container with custom LIMITS and REQUEST",
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

    final List<V1Volume> customVolumeList = Arrays.asList(
        new V1VolumeBuilder()
            .withName(pathNameDefault)
            .withNewHostPath()
              .withNewPath("this-path-must-be-replaced")
            .endHostPath()
            .build(),
        volumeToBeKept
    );
    final List<V1Volume> expectedDefault = Collections.singletonList(volumeDefault);
    final List<V1Volume> expectedCustom = Arrays.asList(volumeDefault, volumeToBeKept);

    // No Volumes set.
    V1Controller controllerDoNotSetVolumes = new V1Controller(Config.newBuilder().build(), runtime);
    V1PodSpec podSpecNoSetVolumes = new V1PodSpec();
    controllerDoNotSetVolumes.addVolumesIfPresent(podSpecNoSetVolumes);
    Assert.assertNull(podSpecNoSetVolumes.getVolumes());

    // Default. Null Volumes.
    V1PodSpec podSpecNull = new V1PodSpecBuilder().build();
    controllerWithVol.addVolumesIfPresent(podSpecNull);
    Assert.assertTrue("Default VOLUMES should be set in container with null VOLUMES",
        CollectionUtils.containsAll(expectedDefault, podSpecNull.getVolumes()));

    // Empty Volumes list
    V1PodSpec podSpecEmpty = new V1PodSpecBuilder()
        .withVolumes(new LinkedList<>())
        .build();
    controllerWithVol.addVolumesIfPresent(podSpecEmpty);
    Assert.assertTrue("Default VOLUMES should be set in container with empty VOLUMES",
        CollectionUtils.containsAll(expectedDefault, podSpecEmpty.getVolumes()));

    // Custom Volumes list
    V1PodSpec podSpecCustom = new V1PodSpecBuilder()
        .withVolumes(customVolumeList)
        .build();
    controllerWithVol.addVolumesIfPresent(podSpecCustom);
    Assert.assertTrue("Default VOLUMES should be set in container with custom VOLUMES",
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
    final List<V1VolumeMount> volumeMountsCustomList = Arrays.asList(
        new V1VolumeMountBuilder()
            .withName(pathNameDefault)
            .withMountPath("should-be-replaced")
            .build(),
        volumeCustom
    );

    // No Volume Mounts set.
    V1Controller controllerDoNotSetMounts = new V1Controller(Config.newBuilder().build(), runtime);
    V1Container containerNoSetMounts = new V1Container();
    controllerDoNotSetMounts.mountVolumeIfPresent(containerNoSetMounts);
    Assert.assertNull(containerNoSetMounts.getVolumeMounts());

    // Default. Null Volume Mounts.
    V1Container containerNull = new V1ContainerBuilder().build();
    controllerWithMounts.mountVolumeIfPresent(containerNull);
    Assert.assertTrue("Default VOLUME MOUNTS should be set in container with null VOLUME MOUNTS",
        CollectionUtils.containsAll(expectedMountsDefault, containerNull.getVolumeMounts()));

    // Empty Volume Mounts.
    V1Container containerEmpty = new V1ContainerBuilder()
        .withVolumeMounts(new LinkedList<>())
        .build();
    controllerWithMounts.mountVolumeIfPresent(containerEmpty);
    Assert.assertTrue("Default VOLUME MOUNTS should be set in container with empty VOLUME MOUNTS",
        CollectionUtils.containsAll(expectedMountsDefault, containerEmpty.getVolumeMounts()));

    // Custom Volume Mounts.
    V1Container containerCustom = new V1ContainerBuilder()
        .withVolumeMounts(volumeMountsCustomList)
        .build();
    controllerWithMounts.mountVolumeIfPresent(containerCustom);
    Assert.assertTrue("Default VOLUME MOUNTS should be set in container with custom VOLUME MOUNTS",
        CollectionUtils.containsAll(expectedMountsCustom, containerCustom.getVolumeMounts()));
  }

  @Test
  public void testConfigureTolerations() {
    final V1Toleration keptToleration = new V1Toleration()
        .key("kept toleration")
        .operator("Some Operator")
        .effect("Some Effect")
        .tolerationSeconds(5L);
    final List<V1Toleration> expectedTolerationBase =
        Collections.unmodifiableList(V1Controller.getTolerations());
    final List<V1Toleration> inputTolerationsBase = Collections.unmodifiableList(
        Arrays.asList(
            new V1Toleration()
                .key(KubernetesConstants.TOLERATIONS.get(0)).operator("replace").effect("replace"),
            new V1Toleration()
                .key(KubernetesConstants.TOLERATIONS.get(1)).operator("replace").effect("replace"),
            keptToleration
        )
    );

    // Null Tolerations. This is the default case.
    final V1PodSpec podSpecNullTolerations = new V1PodSpecBuilder().build();
    v1ControllerWithPodTemplate.configureTolerations(podSpecNullTolerations);
    Assert.assertTrue("Pod Spec has null TOLERATIONS and should be set to Heron's defaults",
        CollectionUtils.containsAll(podSpecNullTolerations.getTolerations(),
            expectedTolerationBase));

    // Empty Tolerations.
    final V1PodSpec podSpecWithEmptyTolerations = new V1PodSpecBuilder()
        .withTolerations(new LinkedList<>())
        .build();
    v1ControllerWithPodTemplate.configureTolerations(podSpecWithEmptyTolerations);
    Assert.assertTrue("Pod Spec has empty TOLERATIONS and should be set to Heron's defaults",
        CollectionUtils.containsAll(podSpecWithEmptyTolerations.getTolerations(),
            expectedTolerationBase));

    // Toleration overriding.
    final V1PodSpec podSpecWithTolerations = new V1PodSpecBuilder()
        .withTolerations(inputTolerationsBase)
        .build();
    final List<V1Toleration> expectedTolerationsOverriding =
        new LinkedList<>(expectedTolerationBase);
    expectedTolerationsOverriding.add(keptToleration);

    v1ControllerWithPodTemplate.configureTolerations(podSpecWithTolerations);
    Assert.assertTrue("Pod Spec has TOLERATIONS and should be overridden with Heron's defaults",
        CollectionUtils.containsAll(podSpecWithTolerations.getTolerations(),
            expectedTolerationsOverriding));
  }

  @Test
  public void testCreatePersistentVolumeClaims() {
    final String topologyName = "topology-name";
    final String volumeNameOne = "volume-name-one";
    final String volumeNameTwo = "volume-name-two";
    final String volumeNameStatic = "volume-name-static";
    final String storageClassName = "storage-class-name";
    final String sizeLimit = "555Gi";
    final String accessModesList = "ReadWriteOnce,ReadOnlyMany,ReadWriteMany";
    final String accessModes = "ReadOnlyMany";
    final String volumeMode = "VolumeMode";
    final String path = "/path/to/mount/";
    final String subPath = "/sub/path/to/mount/";
    final Map<String, Map<VolumeClaimTemplateConfigKeys, String>> mapPVCOpts =
        ImmutableMap.of(
            volumeNameOne, new HashMap<VolumeClaimTemplateConfigKeys, String>() {
              {
                put(VolumeClaimTemplateConfigKeys.storageClassName, storageClassName);
                put(VolumeClaimTemplateConfigKeys.sizeLimit, sizeLimit);
                put(VolumeClaimTemplateConfigKeys.accessModes, accessModesList);
                put(VolumeClaimTemplateConfigKeys.volumeMode, volumeMode);
                put(VolumeClaimTemplateConfigKeys.path, path);
              }
            },
            volumeNameTwo, new HashMap<VolumeClaimTemplateConfigKeys, String>() {
              {
                put(VolumeClaimTemplateConfigKeys.storageClassName, storageClassName);
                put(VolumeClaimTemplateConfigKeys.sizeLimit, sizeLimit);
                put(VolumeClaimTemplateConfigKeys.accessModes, accessModes);
                put(VolumeClaimTemplateConfigKeys.volumeMode, volumeMode);
                put(VolumeClaimTemplateConfigKeys.path, path);
                put(VolumeClaimTemplateConfigKeys.subPath, subPath);
              }
            },
            volumeNameStatic, new HashMap<VolumeClaimTemplateConfigKeys, String>() {
              {
                put(VolumeClaimTemplateConfigKeys.sizeLimit, sizeLimit);
                put(VolumeClaimTemplateConfigKeys.accessModes, accessModes);
                put(VolumeClaimTemplateConfigKeys.volumeMode, volumeMode);
                put(VolumeClaimTemplateConfigKeys.path, path);
                put(VolumeClaimTemplateConfigKeys.subPath, subPath);
              }
            }
        );

    final V1PersistentVolumeClaim claimOne = new V1PersistentVolumeClaimBuilder()
        .withNewMetadata()
          .withName(volumeNameOne)
          .withLabels(V1Controller.getPersistentVolumeClaimLabels(topologyName))
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
          .withLabels(V1Controller.getPersistentVolumeClaimLabels(topologyName))
        .endMetadata()
        .withNewSpec()
          .withStorageClassName(storageClassName)
          .withAccessModes(Collections.singletonList(accessModes))
          .withVolumeMode(volumeMode)
          .withNewResources()
            .addToRequests("storage", new Quantity(sizeLimit))
          .endResources()
        .endSpec()
        .build();

    final V1PersistentVolumeClaim claimStatic = new V1PersistentVolumeClaimBuilder()
        .withNewMetadata()
          .withName(volumeNameStatic)
          .withLabels(V1Controller.getPersistentVolumeClaimLabels(topologyName))
        .endMetadata()
        .withNewSpec()
          .withAccessModes(Collections.singletonList(accessModes))
          .withVolumeMode(volumeMode)
          .withNewResources()
            .addToRequests("storage", new Quantity(sizeLimit))
          .endResources()
        .endSpec()
        .build();

    final List<V1PersistentVolumeClaim> expectedClaims =
        new LinkedList<>(Arrays.asList(claimOne, claimTwo, claimStatic));

    final List<V1PersistentVolumeClaim> actualClaims =
        v1ControllerWithPodTemplate.createPersistentVolumeClaims(mapPVCOpts);

    Assert.assertTrue(expectedClaims.containsAll(actualClaims));
  }

  @Test
  public void testCreatePersistentVolumeClaimVolumeMounts() {
    final String volumeNameOne = "VolumeNameONE";
    final String volumeNameTwo = "VolumeNameTWO";
    final String mountPathOne = "/mount/path/ONE";
    final String mountPathTwo = "/mount/path/TWO";
    final String mountSubPathTwo = "/mount/sub/path/TWO";
    Map<String, Map<VolumeClaimTemplateConfigKeys, String>> mapOfOpts =
        ImmutableMap.of(
            volumeNameOne, ImmutableMap.of(
                VolumeClaimTemplateConfigKeys.path, mountPathOne),
            volumeNameTwo, ImmutableMap.of(
                VolumeClaimTemplateConfigKeys.path, mountPathTwo,
                VolumeClaimTemplateConfigKeys.subPath, mountSubPathTwo)
        );
    final V1VolumeMount volumeMountOne = new V1VolumeMountBuilder()
        .withName(volumeNameOne)
        .withMountPath(mountPathOne)
        .build();
    final V1VolumeMount volumeMountTwo = new V1VolumeMountBuilder()
        .withName(volumeNameTwo)
        .withMountPath(mountPathTwo)
        .withSubPath(mountSubPathTwo)
        .build();

    // Test case container.
    final List<TestTuple<List<V1VolumeMount>, List<V1VolumeMount>>> testCases =
        new LinkedList<>();

    // Default case: No PVC provided.
    final List<V1VolumeMount> actualEmpty =
        v1ControllerPodTemplate.createPersistentVolumeClaimVolumeMounts(new HashMap<>());
    testCases.add(new TestTuple<>("Generated an empty list of Volumes", actualEmpty,
        new LinkedList<>()));

    // PVC Provided.
    final List<V1VolumeMount> expectedFull = Arrays.asList(volumeMountOne, volumeMountTwo);
    final List<V1VolumeMount> actualFull =
        v1ControllerPodTemplate.createPersistentVolumeClaimVolumeMounts(mapOfOpts);
    testCases.add(new TestTuple<>("Generated a list of Volumes", actualFull, expectedFull));

    // Testing loop.
    for (TestTuple<List<V1VolumeMount>, List<V1VolumeMount>> testCase : testCases) {
      Assert.assertTrue(testCase.description + " Mounts",
          testCase.expected.containsAll(testCase.input));
    }
  }

  @Test
  public void testConfigurePodWithPersistentVolumeClaims() {
    final String volumeNameClashing = "clashing-volume";
    final String volumeMountNameClashing = "original-volume-mount";
    V1VolumeMount baseVolumeMount = new V1VolumeMountBuilder()
        .withName(volumeMountNameClashing)
        .withMountPath("/original/mount/path")
        .build();
    V1VolumeMount clashingVolumeMount = new V1VolumeMountBuilder()
        .withName(volumeMountNameClashing)
        .withMountPath("/clashing/mount/path")
        .build();
    V1VolumeMount secondaryVolumeMount = new V1VolumeMountBuilder()
        .withName("secondary-volume-mount")
        .withMountPath("/secondary/mount/path")
        .build();

    /* Test case container.
     * Input: [V1Container to configure, List<V1VolumeMounts> to add]
     * Expected: V1Container as it should be post configuration.
     */
    final List<TestTuple<Object[], V1Container>> testCases = new LinkedList<>();

    // No Persistent Volume Claim.
    final V1Container executorEmptyCase =
        new V1ContainerBuilder().withVolumeMounts(baseVolumeMount).build();
    final V1Container expectedEmptyExecutor =
        new V1ContainerBuilder().withVolumeMounts(baseVolumeMount).build();
    List<V1VolumeMount> emptyVolumeMount = new LinkedList<>();

    testCases.add(new TestTuple<>("Empty",
        new Object[]{executorEmptyCase, emptyVolumeMount}, expectedEmptyExecutor));

    // Non-clashing Persistent Volume Claim.
    final V1Container executorNoClashCase = new V1ContainerBuilder()
        .withVolumeMounts(baseVolumeMount)
        .build();
    final V1Container expectedNoClashExecutor = new V1ContainerBuilder()
        .addToVolumeMounts(baseVolumeMount)
        .addToVolumeMounts(secondaryVolumeMount)
        .build();

    List<V1VolumeMount> noClashVolumeMount =  Collections.singletonList(secondaryVolumeMount);

    testCases.add(new TestTuple<>("No Clash",
        new Object[]{executorNoClashCase, noClashVolumeMount}, expectedNoClashExecutor));

    // Clashing Persistent Volume Claim.
    final V1Container executorClashCase = new V1ContainerBuilder()
        .withVolumeMounts(baseVolumeMount)
        .build();
    final V1Container expectedClashExecutor = new V1ContainerBuilder()
        .addToVolumeMounts(clashingVolumeMount)
        .addToVolumeMounts(secondaryVolumeMount)
        .build();

    List<V1VolumeMount> clashVolumeMount =
        new LinkedList<>(Arrays.asList(clashingVolumeMount, secondaryVolumeMount));

    testCases.add(new TestTuple<>("Clashing",
        new Object[]{executorClashCase, clashVolumeMount}, expectedClashExecutor));

    // Testing loop.
    for (TestTuple<Object[], V1Container> testCase : testCases) {
      doReturn(testCase.input[1])
          .when(v1ControllerWithPodTemplate)
          .createPersistentVolumeClaimVolumeMounts(anyMap());

      v1ControllerWithPodTemplate
          .configurePodWithPersistentVolumeClaims((V1Container) testCase.input[0]);

      Assert.assertEquals("Executors match " + testCase.description,
          testCase.input[0], testCase.expected);
    }
  }
}
