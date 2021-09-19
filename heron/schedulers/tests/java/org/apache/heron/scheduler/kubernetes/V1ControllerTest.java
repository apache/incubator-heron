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
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import org.apache.heron.spi.common.Config;
import org.apache.heron.spi.common.Key;

import io.kubernetes.client.openapi.models.V1ConfigMap;
import io.kubernetes.client.openapi.models.V1PodTemplateSpec;

import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class V1ControllerTest {

  private static final String TOPOLOGY_NAME = "topology-name";
  private static final String CONFIGMAP_NAME = "configmap-name";

  private final Config config = Config.newBuilder().build();
  private final Config runtime = Config.newBuilder()
      .put(Key.TOPOLOGY_NAME, TOPOLOGY_NAME)
      .build();

  @InjectMocks
  private V1Controller v1Controller = new V1Controller(config, runtime);

  @Mock
  private KubernetesController mockController;
  @Mock
  private List<V1ConfigMap> mockConfigMapList;

  @Before
  public void setUp() {
    when(mockController.getPodTemplateConfigName())
        .thenReturn(CONFIGMAP_NAME, null, "", "Nothing");

    when(mockConfigMapList.get(anyInt())).thenReturn(new V1ConfigMap());
  }

  @Test
  public void testLoadPodFromTemplate() throws NoSuchMethodException,
      InvocationTargetException, IllegalAccessException {
    Method loadPodFromTemplate = V1Controller.class
        .getDeclaredMethod("loadPodFromTemplate");
    loadPodFromTemplate.setAccessible(true);

    final V1PodTemplateSpec podSpec = (V1PodTemplateSpec) loadPodFromTemplate
        .invoke(v1Controller);
  }
}
