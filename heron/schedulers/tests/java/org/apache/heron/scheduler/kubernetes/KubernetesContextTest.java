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
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import org.apache.heron.common.basics.Pair;
import org.apache.heron.spi.common.Config;

import static org.apache.heron.scheduler.kubernetes.KubernetesConstants.PersistentVolumeClaimOptions;

public class KubernetesContextTest {

  public static final String KUBERNETES_POD_TEMPLATE_CONFIGMAP_NAME =
      "heron.kubernetes.pod.template.configmap.name";
  private static final String POD_TEMPLATE_CONFIGMAP_NAME = "pod-template-configmap-name";
  private final Config config = Config.newBuilder().build();
  private final Config configWithPodTemplateConfigMap = Config.newBuilder()
      .put(KubernetesContext.KUBERNETES_POD_TEMPLATE_CONFIGMAP_NAME,
          POD_TEMPLATE_CONFIGMAP_NAME)
      .build();

  @Test
  public void testPodTemplateConfigMapName() {
    Assert.assertEquals(KubernetesContext.KUBERNETES_POD_TEMPLATE_CONFIGMAP_NAME,
        KUBERNETES_POD_TEMPLATE_CONFIGMAP_NAME);
    Assert.assertEquals(
        KubernetesContext.getPodTemplateConfigMapName(configWithPodTemplateConfigMap),
        POD_TEMPLATE_CONFIGMAP_NAME);
    Assert.assertNull(KubernetesContext.getPodTemplateConfigMapName(config));
  }

  @Test
  public void testPodTemplateConfigMapDisabled() {
    Assert.assertFalse(KubernetesContext.getPodTemplateConfigMapDisabled(config));
    Assert.assertFalse(KubernetesContext
        .getPodTemplateConfigMapDisabled(configWithPodTemplateConfigMap));

    final Config configWithPodTemplateConfigMapOff = Config.newBuilder()
        .put(KubernetesContext.KUBERNETES_POD_TEMPLATE_CONFIGMAP_NAME,
            POD_TEMPLATE_CONFIGMAP_NAME)
        .put(KubernetesContext.KUBERNETES_POD_TEMPLATE_CONFIGMAP_DISABLED, "TRUE")
        .build();
    Assert.assertTrue(KubernetesContext
        .getPodTemplateConfigMapDisabled(configWithPodTemplateConfigMapOff));
  }

  @Test
  public void testGetPersistentVolumeClaims() {
    final String volumeNameOne = "volumeNameOne";
    final String volumeNameTwo = "volumeNameTwo";
    final String keyPattern = "%s%s.%s";

    final String claimNameField =
        PersistentVolumeClaimOptions.storageClassName.claimName.toString();
    final String expectedClaimName = "expectedClaimName";
    final String claimNameKeyOne = String.format(keyPattern,
        KubernetesContext.KUBERNETES_PERSISTENT_VOLUME_CLAIM_PREFIX, volumeNameOne, claimNameField);
    final String claimNameKeyTwo = String.format(keyPattern,
        KubernetesContext.KUBERNETES_PERSISTENT_VOLUME_CLAIM_PREFIX, volumeNameTwo, claimNameField);

    final String storageClassField = PersistentVolumeClaimOptions.storageClassName.toString();
    final String expectedStorageClass = "expectedStorageClass";
    final String storageClassKeyOne = String.format(keyPattern,
        KubernetesContext.KUBERNETES_PERSISTENT_VOLUME_CLAIM_PREFIX, volumeNameOne,
        storageClassField);
    final String storageClassKeyTwo = String.format(keyPattern,
        KubernetesContext.KUBERNETES_PERSISTENT_VOLUME_CLAIM_PREFIX, volumeNameTwo,
        storageClassField);

    final Config configPVC = Config.newBuilder()
        .put(claimNameKeyOne, expectedClaimName)
        .put(claimNameKeyTwo, expectedClaimName)
        .put(storageClassKeyOne, expectedStorageClass)
        .put(storageClassKeyTwo, expectedStorageClass)
        .build();

    List<String> expectedKeys = Arrays.asList(volumeNameOne, volumeNameTwo);
    List<Pair<PersistentVolumeClaimOptions, String>> expectedOptions =
        Arrays.asList(
            new Pair<>(PersistentVolumeClaimOptions.storageClassName.claimName,
                expectedClaimName),
            new Pair<>(PersistentVolumeClaimOptions.storageClassName.storageClassName,
                expectedStorageClass)
        );

    // List of provided PVC options.
    Map<String, List<Pair<PersistentVolumeClaimOptions, String>>> listOfPVC =
        KubernetesContext.getPersistentVolumeClaims(configPVC);

    Assert.assertTrue("Contains all provided Volumes",
        listOfPVC.keySet().containsAll(expectedKeys));
    for (List<Pair<PersistentVolumeClaimOptions, String>> items : listOfPVC.values()) {
      Assert.assertTrue("Contains all provided option keys and values",
          items.containsAll(expectedOptions));
    }

    // Empty PVC.
    Map<String, List<Pair<PersistentVolumeClaimOptions, String>>> emptyPVC =
        KubernetesContext.getPersistentVolumeClaims(Config.newBuilder().build());
    Assert.assertTrue("Empty PVC is returned when no options provided", emptyPVC.isEmpty());

  }
}
