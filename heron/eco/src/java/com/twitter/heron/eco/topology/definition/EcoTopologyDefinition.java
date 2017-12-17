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
package com.twitter.heron.eco.topology.definition;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

public class EcoTopologyDefinition {
  private static final Logger LOG = Logger.getLogger(EcoTopologyDefinition.class.getName());

  private String name;
  private Map<String, Object> config = new HashMap<>();

  public Map<String, Object> getConfig() {
    return config;
  }

  public void setConfig(Map<String, Object> config) {
    this.config = config;
  }

  public String getName() {

    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  @Override
  public String toString() {
    return "EcoTopologyDefinition{" +
        "name='" + name + '\'' +
        ", config=" + config +
        '}';
  }
}
