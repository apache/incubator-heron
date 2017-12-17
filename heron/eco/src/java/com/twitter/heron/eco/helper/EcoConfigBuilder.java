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
package com.twitter.heron.eco.helper;

import java.util.Map;

import com.twitter.heron.eco.definition.EcoTopologyDefinition;
import com.twitter.heron.streamlet.Config;

public class EcoConfigBuilder {

  public static Config buildConfig(EcoTopologyDefinition topologyDefinition) {
    Map<String, Object> configMap = topologyDefinition.getConfig();
    if (configMap == null) {
      return com.twitter.heron.streamlet.Config.defaultConfig();
    } else {
      //todo finish this implementation correctly
      Config topologyConfig = Config.newBuilder()
          .setNumContainers(5)
          .setPerContainerRamInGigabytes(10)
          .setPerContainerCpu(3.5f)
          .setDeliverySemantics(Config.DeliverySemantics.EFFECTIVELY_ONCE)
          .setSerializer(Config.Serializer.JAVA)
          .setUserConfig("some-key", "some-value")
          .build();
      return topologyConfig;

    }
  }
}
