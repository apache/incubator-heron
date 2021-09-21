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

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import org.apache.heron.spi.common.Config;
import org.apache.heron.spi.common.Key;

import io.kubernetes.client.openapi.models.V1ConfigMapList;
import io.kubernetes.client.openapi.models.V1PodTemplateSpec;

import static org.mockito.Mockito.when;

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

  private final V1Controller v1ControllerNoPodTemplate = new V1Controller(config, runtime);
  private final V1Controller v1ControllerWithPodTemplate =
      new V1Controller(configWithPodTemplate, runtime);

  @Mock
  private V1ConfigMapList mockConfigMapList;

  @InjectMocks
  private final Method loadPodFromTemplate = V1Controller.class
      .getDeclaredMethod("loadPodFromTemplate");

  public V1ControllerTest() throws NoSuchMethodException {
    loadPodFromTemplate.setAccessible(true);
  }

  @Rule
  public final ExpectedException exceptionRule = ExpectedException.none();

  @Before
  public void setUp() {
    when(mockConfigMapList.getItems()).thenReturn(null);
  }

  @Test
  public void testLoadPodFromTemplateDefault()
      throws InvocationTargetException, IllegalAccessException {
    final V1PodTemplateSpec podSpec = (V1PodTemplateSpec) loadPodFromTemplate
        .invoke(v1ControllerNoPodTemplate);

    Assert.assertEquals(podSpec.toString(), POD_TEMPLATE_DEFAULT);
  }

  @Test
  public void testLoadPodFromTemplateNoConfigMaps()
      throws InvocationTargetException, IllegalAccessException {
//    exceptionRule.expect(TopologySubmissionException.class);
//    exceptionRule.expectMessage("No ConfigMaps set");
//
//    final V1PodTemplateSpec podSpec = (V1PodTemplateSpec) loadPodFromTemplate
//        .invoke(v1ControllerWithPodTemplate);
  }
}
