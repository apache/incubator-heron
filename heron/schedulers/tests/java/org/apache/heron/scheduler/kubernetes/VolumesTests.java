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

import org.junit.Assert;
import org.junit.Test;

import org.apache.heron.spi.common.Config;

import io.kubernetes.client.openapi.models.V1Volume;

public class VolumesTests {

  @Test
  public void testNoVolume() {
    final Config config = Config.newBuilder().build();
    final V1Volume volume = Volumes.get().create(config);
    Assert.assertNull(volume);
  }

  @Test
  public void testHostPathVolume() {
    final String path = "/test/dir1";
    final Config config = Config.newBuilder()
        .put(KubernetesContext.HERON_KUBERNETES_VOLUME_TYPE, "hostPath")
        .put(KubernetesContext.HERON_KUBERNETES_VOLUME_HOSTPATH_PATH, path)
        .build();

    final V1Volume volume = Volumes.get().create(config);
    Assert.assertNotNull(volume);
    Assert.assertNotNull(volume.getHostPath());
    Assert.assertEquals(volume.getHostPath().getPath(), path);
  }

  @Test
  public void testNfsVolume() {
    final String path = "/test/dir1";
    final String server = "10.10.10.10";
    final Config config = Config.newBuilder()
        .put(KubernetesContext.HERON_KUBERNETES_VOLUME_TYPE, "nfs")
        .put(KubernetesContext.HERON_KUBERNETES_VOLUME_NFS_PATH, path)
        .put(KubernetesContext.HERON_KUBERNETES_VOLUME_NFS_SERVER, server)
        .build();

    final V1Volume volume = Volumes.get().create(config);
    Assert.assertNotNull(volume);
    Assert.assertNotNull(volume.getNfs());
    Assert.assertEquals(volume.getNfs().getPath(), path);
    Assert.assertEquals(volume.getNfs().getServer(), server);
  }

  @Test
  public void testAwsEbsVolume() {
    final String volumeId = "aws-ebs-1";
    final String fsType = "ext4";
    final Config config = Config.newBuilder()
        .put(KubernetesContext.HERON_KUBERNETES_VOLUME_TYPE, "awsElasticBlockStore")
        .put(KubernetesContext.HERON_KUBERNETES_VOLUME_AWS_EBS_VOLUME_ID, volumeId)
        .put(KubernetesContext.HERON_KUBERNETES_VOLUME_AWS_EBS_FS_TYPE, fsType)
        .build();

    final V1Volume volume = Volumes.get().create(config);
    Assert.assertNotNull(volume);
    Assert.assertNotNull(volume.getAwsElasticBlockStore());
    Assert.assertEquals(volume.getAwsElasticBlockStore().getVolumeID(), volumeId);
    Assert.assertEquals(volume.getAwsElasticBlockStore().getFsType(), fsType);
  }
}
