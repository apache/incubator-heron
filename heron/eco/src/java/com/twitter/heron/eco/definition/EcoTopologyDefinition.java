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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class EcoTopologyDefinition {

  private String name;
  private Map<String, Object> config = new HashMap<>();
  private Map<String, SourceDefinition> sources =  new LinkedHashMap<>();
  private Map<String, ChildDefinition> children = new LinkedHashMap<>();
  private List<StreamDefinition> streams = new ArrayList<>();

  public List<SourceDefinition> getSources() {
    return new ArrayList<>(this.sources.values());
  }

  public void setSources(List<SourceDefinition> sources) {
    this.sources = new LinkedHashMap<>();
    for (SourceDefinition source: sources) {
      this.sources.put(source.getName(), source);
    }
  }

  public List<ChildDefinition> getChildren() {
    return new ArrayList<>(this.children.values());
  }

  public void setChildren(List<ChildDefinition> children) {
    this.children = new LinkedHashMap<>();
    for (ChildDefinition child: children) {
      this.children.put(child.getName(), child);
    }
  }

  public List<StreamDefinition> getStreams() {
    return streams;
  }

  public void setStreams(List<StreamDefinition> streams) {
    this.streams = streams;
  }

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
    return "EcoTopologyDefinition{"
        + "name='" + name + '\''
        + ", config=" + config
        + ", sources=" + sources
        + ", children=" + children
        + ", streams=" + streams
        + '}';
  }
}
