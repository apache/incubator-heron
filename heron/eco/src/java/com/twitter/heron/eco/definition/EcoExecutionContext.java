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

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.Config;


public class EcoExecutionContext {

  private EcoTopologyDefinition topologyDefinition;

  private Config config;

  private Map<String, Object> spouts;

  private Map<String, Object> bolts;

  private Map<String, ComponentStream> streams;

  private Map<String, Object> components = new HashMap<>();

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

  public Map<String, Object> getSpouts() {
    return spouts;
  }

  public void setSpouts(Map<String, Object> spouts) {
    this.spouts = spouts;
  }

  public Map<String, Object> getBolts() {
    return bolts;
  }

  public Object getBolt(String id) {
    return this.bolts.get(id);
  }

  public void setBolts(Map<String, Object> bolts) {
    this.bolts = bolts;
  }

  public void addBolt(String key, Object value) {
    this.bolts.put(key, value);
  }

  public Object getChild(String id) {
    return this.bolts.get(id);
  }

  public Map<String, ComponentStream> getStreams() {
    return streams;
  }

  public void setStreams(Map<String, ComponentStream> streams) {
    this.streams = streams;
  }

  public Map<String, Object> getComponents() {
    return components;
  }

  public void addComponent(String key, Object value) {
    this.components.put(key, value);
  }

  public Object getComponent(String id) {
    return this.components.get(id);
  }

  public void setComponents(Map<String, Object> components) {
    this.components = components;
  }
}
