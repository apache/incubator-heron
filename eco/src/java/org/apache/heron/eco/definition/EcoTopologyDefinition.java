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

package org.apache.heron.eco.definition;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class EcoTopologyDefinition {

  private String name;
  private String type;
  private Map<String, Object> config = new HashMap<>();
  private Map<String, SpoutDefinition> spouts =  new LinkedHashMap<>();
  private Map<String, BoltDefinition> bolts = new LinkedHashMap<>();
  private List<StreamDefinition> streams = new ArrayList<>();
  private Map<String, BeanDefinition> components = new LinkedHashMap<>();

  public List<SpoutDefinition> getSpouts() {
    return new ArrayList<>(this.spouts.values());
  }

  public SpoutDefinition getSpout(String id) {
    return this.spouts.get(id);
  }

  public void setSpouts(List<SpoutDefinition> sources) {
    this.spouts = new LinkedHashMap<>();
    for (SpoutDefinition source: sources) {
      this.spouts.put(source.getId(), source);
    }
  }

  public List<BoltDefinition> getBolts() {
    return new ArrayList<>(this.bolts.values());
  }

  public BoltDefinition getBolt(String id) {
    return this.bolts.get(id);
  }

  public void setBolts(List<BoltDefinition> children) {
    this.bolts = new LinkedHashMap<>();
    for (BoltDefinition child: children) {
      this.bolts.put(child.getId(), child);
    }
  }

  public List<BeanDefinition> getComponents() {
    return new ArrayList<>(this.components.values());
  }

  public Object getComponent(String id) {
    return this.components.get(id);
  }

  public void setComponents(List<BeanDefinition> components) {
    for (BeanDefinition bean: components) {
      this.components.put(bean.getId(), bean);
    }
  }

  public void addComponent(String key, BeanDefinition value) {
    this.components.put(key, value);
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

  public String getType() {
    if (type == null || "storm".equals(type)) {
      return "storm";
    }

    if ("heron".equals(type)) {
      return "heron";
    }

    return null;
  }

  public void setType(String type) {
    this.type = type;
  }

  public void setName(String name) {
    this.name = name;
  }

  public Number parallelismForBolt(String to) {
    return this.bolts.get(to).getParallelism();
  }
}
