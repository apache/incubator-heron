//  Copyright 2017 Twitter. All rights reserved.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
package com.twitter.heron.scheduler.kubernetes;

import java.util.HashMap;
import java.util.Map;

import com.twitter.heron.spi.common.Config;

import io.kubernetes.client.models.V1HostPathVolumeSource;
import io.kubernetes.client.models.V1Volume;

final class Volumes {

  private final Map<String, VolumeFactory> volumes = new HashMap<>();

  private Volumes() {
    volumes.put("hostPath", new HostPathVolumeFactory());
  }

  static Volumes get() {
    return new Volumes();
  }

  V1Volume create(Config config) {
    final String volumeType = KubernetesContext.getVolumeType(config);
    if (volumes.containsKey(volumeType)) {
      return volumes.get(volumeType).create(config);
    }
    return null;
  }

  interface VolumeFactory {
    V1Volume create(Config config);
  }

  static class HostPathVolumeFactory implements VolumeFactory {
    @Override
    public V1Volume create(Config config) {
      final String volumeName = KubernetesContext.getVolumeName(config);
      final V1Volume volume = new V1Volume()
          .name(volumeName);

      final String path = KubernetesContext.getHostPathVolumePath(config);
      final V1HostPathVolumeSource hostPathVolume =
          new V1HostPathVolumeSource()
              .path(path);
      volume.hostPath(hostPathVolume);

      return volume;
    }
  }
}
