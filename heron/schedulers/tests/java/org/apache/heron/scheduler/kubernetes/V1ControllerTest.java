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
import java.util.LinkedList;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Spy;
import org.mockito.runners.MockitoJUnitRunner;

import org.apache.heron.spi.common.Config;
import org.apache.heron.spi.common.Key;

import io.kubernetes.client.openapi.models.V1ConfigMap;
import io.kubernetes.client.openapi.models.V1PodTemplateSpec;

import static org.mockito.Mockito.doReturn;

@RunWith(MockitoJUnitRunner.class)
public class V1ControllerTest {

  private static final String TOPOLOGY_NAME = "topology-name";
  private static final String CONFIGMAP_NAME = "configmap-name";
  private static final String POD_TEMPLATE_DEFAULT = new V1PodTemplateSpec().toString();

  private final Config config = Config.newBuilder().build();
  private final Config configWithPodTemplate = Config.newBuilder()
      .put(KubernetesContext.KUBERNETES_POD_TEMPLATE_CONFIGMAP_NAME, CONFIGMAP_NAME)
      .build();
  private final Config runtime = Config.newBuilder()
      .put(Key.TOPOLOGY_NAME, TOPOLOGY_NAME)
      .build();

  private final LinkedList<V1ConfigMap> emptyConfigMapList;
  private final LinkedList<V1ConfigMap> dummyConfigMapList;
  private final LinkedList<V1ConfigMap> invalidPodConfigMapList;
  private final LinkedList<V1ConfigMap> emptyPodConfigMapList;

  private final V1Controller v1ControllerNoPodTemplate = new V1Controller(config, runtime);
  @Spy
  private final V1Controller v1ControllerWithPodTemplate =
      new V1Controller(configWithPodTemplate, runtime);

  @InjectMocks
  private final Method loadPodFromTemplate = V1Controller.class
      .getDeclaredMethod("loadPodFromTemplate");

  public V1ControllerTest() throws NoSuchMethodException {
    loadPodFromTemplate.setAccessible(true);

    // ConfigMap List with empty and null data.
    V1ConfigMap emptyConfigMap = new V1ConfigMap();
    emptyConfigMapList = new LinkedList<>(
        Arrays.asList(emptyConfigMap, emptyConfigMap, null, emptyConfigMap, emptyConfigMap));

    // ConfigMap List with empty and non-target maps.
    V1ConfigMap configMapWithNonTargetData = new V1ConfigMap();
    configMapWithNonTargetData.putDataItem("Dummy Key", "Dummy Value");
    dummyConfigMapList = new LinkedList<V1ConfigMap>(emptyConfigMapList);
    dummyConfigMapList.add(configMapWithNonTargetData);

    // ConfigMap List with empty and invalid target maps.
    V1ConfigMap configMapInvalidPod = new V1ConfigMap();
    configMapInvalidPod.putDataItem(CONFIGMAP_NAME, "Dummy Value");
    invalidPodConfigMapList = new LinkedList<V1ConfigMap>(
        Arrays.asList(configMapWithNonTargetData, configMapInvalidPod));

    // ConfigMap List with empty and empty target maps.
    V1ConfigMap configMapEmptyPod = new V1ConfigMap();
    configMapEmptyPod.putDataItem(CONFIGMAP_NAME, "");
    emptyPodConfigMapList = new LinkedList<V1ConfigMap>(
        Arrays.asList(configMapWithNonTargetData, configMapEmptyPod));
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

    doReturn(invalidPodConfigMapList).when(v1ControllerWithPodTemplate).getConfigMaps();
    try {
      loadPodFromTemplate.invoke(v1ControllerWithPodTemplate);
    } catch (InvocationTargetException e) {
      message = e.getCause().getMessage();
    }
    Assert.assertTrue(message.contains(expected));

    doReturn(emptyPodConfigMapList).when(v1ControllerWithPodTemplate).getConfigMaps();
    try {
      loadPodFromTemplate.invoke(v1ControllerWithPodTemplate);
    } catch (InvocationTargetException e) {
      message = e.getCause().getMessage();
    }
    Assert.assertTrue(message.contains(expected));
  }
}
