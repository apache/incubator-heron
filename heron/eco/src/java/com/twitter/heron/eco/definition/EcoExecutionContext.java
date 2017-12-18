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
package com.twitter.heron.eco.definition;

import java.util.Map;

import com.twitter.heron.api.Config;


public class EcoExecutionContext {

  private EcoTopologyDefinition topologyDefinition;

  private Config config;

  private Map<String, Object> sources;

  private Map<String, Object> children;

  private Map<String, ComponentStream> streams;

  public EcoExecutionContext(EcoTopologyDefinition topologyDefinition, Config config) {
    this.topologyDefinition = topologyDefinition;
    this.config = config;
  }

  public EcoTopologyDefinition getTopologyDefinition() {
    return topologyDefinition;
  }

  public void setTopologyDefinition(EcoTopologyDefinition topologyDefinition) {
    this.topologyDefinition = topologyDefinition;
  }

  public Config getConfig() {
    return config;
  }

  public void setConfig(Config config) {
    this.config = config;
  }

  public Map<String, Object> getSources() {
    return sources;
  }

  public void setSources(Map<String, Object> sources) {
    this.sources = sources;
  }

  public Map<String, Object> getChildren() {
    return children;
  }

  public void setChildren(Map<String, Object> children) {
    this.children = children;
  }

  public Object getChild(String id) {
    return this.children.get(id);
  }

  public Map<String, ComponentStream> getStreams() {
    return streams;
  }

  public void setStreams(Map<String, ComponentStream> streams) {
    this.streams = streams;
  }
}
