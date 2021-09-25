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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Spy;
import org.mockito.runners.MockitoJUnitRunner;

import org.apache.heron.common.basics.Pair;
import org.apache.heron.scheduler.TopologySubmissionException;
import org.apache.heron.spi.common.Config;
import org.apache.heron.spi.common.Key;

import io.kubernetes.client.openapi.models.V1ConfigMap;
import io.kubernetes.client.openapi.models.V1PodTemplateSpec;

import static org.mockito.Mockito.doReturn;

@RunWith(MockitoJUnitRunner.class)
public class V1ControllerTest {

  private static final String TOPOLOGY_NAME = "topology-name";
  private static final String CONFIGMAP_POD_TEMPLATE_NAME = "CONFIG-MAP-NAME.POD-TEMPLATE-NAME";
  private static final String CONFIGMAP_NAME = "CONFIG-MAP-NAME";
  private static final String POD_TEMPLATE_NAME = "POD-TEMPLATE-NAME";
  private static final String POD_TEMPLATE_DEFAULT = new V1PodTemplateSpec().toString();

  private final Config config = Config.newBuilder().build();
  private final Config configWithPodTemplate = Config.newBuilder()
      .put(KubernetesContext.KUBERNETES_POD_TEMPLATE_CONFIGMAP_NAME, CONFIGMAP_POD_TEMPLATE_NAME)
      .build();
  private final Config runtime = Config.newBuilder()
      .put(Key.TOPOLOGY_NAME, TOPOLOGY_NAME)
      .build();

  private final LinkedList<V1ConfigMap> emptyConfigMapList;
  private final LinkedList<V1ConfigMap> dummyConfigMapList;
  private final V1ConfigMap configMapWithNonTargetData;

  private final V1Controller v1ControllerNoPodTemplate = new V1Controller(config, runtime);
  @Spy
  private final V1Controller v1ControllerWithPodTemplate =
      new V1Controller(configWithPodTemplate, runtime);

  @InjectMocks
  private final Method loadPodFromTemplate = V1Controller.class
      .getDeclaredMethod("loadPodFromTemplate");

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  public V1ControllerTest() throws NoSuchMethodException {
    loadPodFromTemplate.setAccessible(true);

    // ConfigMap List with empty and null data.
    V1ConfigMap emptyConfigMap = new V1ConfigMap();
    emptyConfigMapList = new LinkedList<>(
        Arrays.asList(emptyConfigMap, emptyConfigMap, null, emptyConfigMap, emptyConfigMap));

    // ConfigMap List with empty and non-target maps.
    configMapWithNonTargetData = new V1ConfigMap();
    configMapWithNonTargetData.putDataItem("Dummy Key", "Dummy Value");
    dummyConfigMapList = new LinkedList<>(emptyConfigMapList);
    dummyConfigMapList.add(configMapWithNonTargetData);
  }

  @Test
  public void testLoadPodFromTemplateDefault()
      throws InvocationTargetException, IllegalAccessException {
    final V1PodTemplateSpec podSpec = (V1PodTemplateSpec) loadPodFromTemplate
        .invoke(v1ControllerNoPodTemplate);

    Assert.assertEquals(podSpec.toString(), POD_TEMPLATE_DEFAULT);
  }

  @Test
  public void testLoadPodFromTemplateNullConfigMaps() throws IllegalAccessException {
    final String expected = "No ConfigMaps";
    String message = "";

    doReturn(null).when(v1ControllerWithPodTemplate).getConfigMaps();
    try {
      loadPodFromTemplate.invoke(v1ControllerWithPodTemplate);
    } catch (InvocationTargetException e) {
      message = e.getCause().getMessage();
    }
    Assert.assertTrue(message.contains(expected));
  }

  @Test
  public void testLoadPodFromTemplateNoConfigMaps() throws IllegalAccessException {
    final String expected = "Failed to locate Pod Template";
    String message = "";

    doReturn(new LinkedList<V1ConfigMap>()).when(v1ControllerWithPodTemplate).getConfigMaps();
    try {
      loadPodFromTemplate.invoke(v1ControllerWithPodTemplate);
    } catch (InvocationTargetException e) {
      message = e.getCause().getMessage();
    }
    Assert.assertTrue(message.contains(expected));
  }

  @Test
  public void testLoadPodFromTemplateNoTargetConfigMaps() throws IllegalAccessException {
    final String expected = "Failed to locate Pod Template";
    String message = "";

    doReturn(emptyConfigMapList).when(v1ControllerWithPodTemplate).getConfigMaps();
    try {
      loadPodFromTemplate.invoke(v1ControllerWithPodTemplate);
    } catch (InvocationTargetException e) {
      message = e.getCause().getMessage();
    }
    Assert.assertTrue(message.contains(expected));

    doReturn(dummyConfigMapList).when(v1ControllerWithPodTemplate).getConfigMaps();
    try {
      loadPodFromTemplate.invoke(v1ControllerWithPodTemplate);
    } catch (InvocationTargetException e) {
      message = e.getCause().getMessage();
    }
    Assert.assertTrue(message.contains(expected));
  }

  @Test
  public void testLoadPodFromTemplateBadTargetConfigMaps() throws IllegalAccessException {
    final String expected = "Error parsing";
    String message = "";

    // ConfigMap List with empty and invalid target maps.
    final LinkedList<V1ConfigMap> invalidPodConfigMapList;
    V1ConfigMap configMapInvalidPod = new V1ConfigMap();
    configMapInvalidPod.putDataItem(CONFIGMAP_POD_TEMPLATE_NAME, "Dummy Value");
    invalidPodConfigMapList = new LinkedList<>(
        Arrays.asList(configMapWithNonTargetData, configMapInvalidPod));

    doReturn(invalidPodConfigMapList).when(v1ControllerWithPodTemplate).getConfigMaps();
    try {
      loadPodFromTemplate.invoke(v1ControllerWithPodTemplate);
    } catch (InvocationTargetException e) {
      message = e.getCause().getMessage();
    }
    Assert.assertTrue(message.contains(expected));

    // ConfigMap List with empty and empty target maps.
    final LinkedList<V1ConfigMap> emptyPodConfigMapList;
    V1ConfigMap configMapEmptyPod = new V1ConfigMap();
    configMapEmptyPod.putDataItem(CONFIGMAP_POD_TEMPLATE_NAME, "");
    emptyPodConfigMapList = new LinkedList<>(
        Arrays.asList(configMapWithNonTargetData, configMapEmptyPod));

    doReturn(emptyPodConfigMapList).when(v1ControllerWithPodTemplate).getConfigMaps();
    try {
      loadPodFromTemplate.invoke(v1ControllerWithPodTemplate);
    } catch (InvocationTargetException e) {
      message = e.getCause().getMessage();
    }
    Assert.assertTrue(message.contains(expected));
  }

  @Test
  public void testLoadPodFromTemplateValidConfigMaps()
      throws IllegalAccessException, InvocationTargetException {
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
    final String validPodTemplate =
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
    final LinkedList<V1ConfigMap> validPodConfigMapList;
    V1ConfigMap configMapValidPod = new V1ConfigMap();
    configMapValidPod.putDataItem(CONFIGMAP_POD_TEMPLATE_NAME, validPodTemplate);
    validPodConfigMapList = new LinkedList<>(
        Arrays.asList(configMapWithNonTargetData, configMapValidPod));

    doReturn(validPodConfigMapList).when(v1ControllerWithPodTemplate).getConfigMaps();
    V1PodTemplateSpec podTemplateSpec =
        (V1PodTemplateSpec) loadPodFromTemplate.invoke(v1ControllerWithPodTemplate);

    Assert.assertTrue(podTemplateSpec.toString().contains(expected));
  }

  @Test
  public void testLoadPodFromTemplateInvalidConfigMaps() throws IllegalAccessException {
    // ConfigMap List with valid Pod Template.
    final String invalidPodTemplate =
        "apiVersion: apps/v1\n"
            + "kind: PottyTemplate\n"
            + "metadata:\n"
            + "  name: heron-tracker\n"
            + "  namespace: default\n"
            + "template:\n"
            + "  metadata:\n"
            + "    labels:\n"
            + "      app: heron-tracker\n"
            + "  spec:\n";
    V1ConfigMap configMap = new V1ConfigMap();
    configMap.putDataItem(CONFIGMAP_POD_TEMPLATE_NAME, invalidPodTemplate);
    LinkedList<V1ConfigMap> configMapList =
        new LinkedList<>(Collections.singletonList(configMap));

    final String expected = "Error parsing";
    String message = "";

    doReturn(configMapList).when(v1ControllerWithPodTemplate).getConfigMaps();
    try {
      loadPodFromTemplate.invoke(v1ControllerWithPodTemplate);
    } catch (InvocationTargetException e) {
      message = e.getCause().getMessage();
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
    Assert.assertTrue(actual.equals(expected));
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
}
