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

import java.util.Map;

import com.google.common.collect.ImmutableMap;

import org.junit.Assert;
import org.junit.Test;

import io.kubernetes.client.openapi.models.V1Volume;
import io.kubernetes.client.openapi.models.V1VolumeBuilder;

public class VolumesTests {

  @Test
  public void testEmptyDir() {
    final String volumeName = "volume-name-empty-dir";
    final String medium = "Memory";
    final String sizeLimit = "1Gi";
    final String path = "/path/to/mount";
    final String subPath = "/sub/path/to/mount";
    final Map<KubernetesConstants.VolumeConfigKeys, String> configEmptyDir =
        ImmutableMap.<KubernetesConstants.VolumeConfigKeys, String>builder()
            .put(KubernetesConstants.VolumeConfigKeys.sizeLimit, sizeLimit)
            .put(KubernetesConstants.VolumeConfigKeys.medium, medium)
            .put(KubernetesConstants.VolumeConfigKeys.path, path)
            .put(KubernetesConstants.VolumeConfigKeys.subPath, subPath)
            .build();
    final V1Volume expectedVolume = new V1VolumeBuilder()
        .withName(volumeName)
        .withNewEmptyDir()
        .withMedium(medium)
        .withNewSizeLimit(sizeLimit)
        .endEmptyDir()
        .build();

    final V1Volume actualVolume = Volumes.get()
        .create(Volumes.VolumeType.EmptyDir, volumeName, configEmptyDir);

    Assert.assertEquals("Volume Factory Empty Directory", expectedVolume, actualVolume);
  }
}
