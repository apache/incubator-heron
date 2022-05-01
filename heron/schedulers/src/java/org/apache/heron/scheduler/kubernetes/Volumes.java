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

import java.util.HashMap;
import java.util.Map;

import org.apache.heron.spi.common.Config;

import io.kubernetes.client.openapi.models.V1Volume;

final class Volumes {

  private final Map<String, VolumeFactory> volumes = new HashMap<>();

  private Volumes() {
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

  private static V1Volume newVolume(Config config) {
    final String volumeName = KubernetesContext.getVolumeName(config);
    return new V1Volume().name(volumeName);
  }
}
