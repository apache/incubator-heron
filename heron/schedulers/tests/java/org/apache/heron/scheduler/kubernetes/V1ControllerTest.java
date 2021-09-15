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
import org.junit.Test;

import org.apache.heron.spi.common.Config;
import org.apache.heron.spi.common.Key;

import io.kubernetes.client.openapi.models.V1ConfigMapVolumeSource;
import io.kubernetes.client.openapi.models.V1KeyToPath;
import io.kubernetes.client.openapi.models.V1PodSpec;

public class V1ControllerTest {

  private static final String TOPOLOGY_NAME = "topology-name";
  private static final String CONFIGMAP_NAME = "configmap-name";

  private final Config config = Config.newBuilder().build();
  private final Config runtime = Config.newBuilder()
      .put(Key.TOPOLOGY_NAME, TOPOLOGY_NAME)
      .build();

  private final V1Controller v1Controller = new V1Controller(config, runtime);

  @Test
  public void testPodTemplateConfigMapVolume() throws NoSuchMethodException,
      InvocationTargetException, IllegalAccessException {
    Method createPodTemplateConfigMapVolume = V1Controller.class
        .getDeclaredMethod("createPodTemplateConfigMapVolume", String.class);
    createPodTemplateConfigMapVolume.setAccessible(true);

    final V1PodSpec podSpec = (V1PodSpec) createPodTemplateConfigMapVolume
        .invoke(v1Controller, CONFIGMAP_NAME);

    checkPodTemplateConfigMap(podSpec);
  }

  private void checkPodTemplateConfigMap(V1PodSpec podSpec) {
    Assert.assertNotNull(podSpec);
    Assert.assertFalse(podSpec.getVolumes().isEmpty());

    Assert.assertEquals(podSpec.getVolumes().get(0).getName(),
        KubernetesConstants.POD_TEMPLATE_VOLUME_NAME);

    final V1ConfigMapVolumeSource configMap = podSpec.getVolumes().get(0).getConfigMap();
    Assert.assertEquals(configMap.getName(), CONFIGMAP_NAME);

    final V1KeyToPath items = configMap.getItems().get(0);

    Assert.assertEquals(items.getKey(), KubernetesConstants.POD_TEMPLATE_KEY);
    Assert.assertEquals(items.getPath(), KubernetesConstants.EXECUTOR_POD_SPEC_TEMPLATE_FILE_NAME);
  }
}
