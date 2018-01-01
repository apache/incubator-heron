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
package com.twitter.heron.eco.builder;

import java.util.Map;

import org.apache.storm.Config;

import com.twitter.heron.eco.definition.EcoTopologyDefinition;

public class ConfigBuilder {

  protected Config buildConfig(EcoTopologyDefinition topologyDefinition) {
    Map<String, Object> configMap = topologyDefinition.getConfig();
    if (configMap == null) {
      return new Config();
    } else {
      Config config = new Config();
      for (Map.Entry<String, Object> entry: topologyDefinition.getConfig().entrySet()) {
        config.put(entry.getKey(), entry.getValue());
      }
      return config;
    }
  }
}
