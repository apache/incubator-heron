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

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

import com.twitter.heron.api.Config;
import com.twitter.heron.api.topology.TopologyBuilder;
import com.twitter.heron.eco.definition.ComponentDefinition;
import com.twitter.heron.eco.definition.ComponentStream;
import com.twitter.heron.eco.definition.EcoExecutionContext;
import com.twitter.heron.eco.definition.EcoTopologyDefinition;
import com.twitter.heron.eco.definition.StreamDefinition;

public final class EcoBuilder {
  private static final Logger LOG = Logger.getLogger(EcoBuilder.class.getName());

  private EcoBuilder() { }

  public static TopologyBuilder buildTopologyBuilder(EcoExecutionContext executionContext)
      throws InstantiationException, IllegalAccessException, ClassNotFoundException {

    buildSpouts(executionContext);
    buildBolts(executionContext);
    buildStreams(executionContext);

    return build(executionContext);
  }

  private static TopologyBuilder build(EcoExecutionContext executionContext) {
    TopologyBuilder builder = new TopologyBuilder();
    Map<String, Object> sources = executionContext.getSources();


    return builder;
  }

  private static void buildStreams(EcoExecutionContext executionContext) {
    EcoTopologyDefinition topologyDefinition = executionContext.getTopologyDefinition();
    Map<String, ComponentStream> componentStreams = new HashMap<>();

    for (StreamDefinition def: topologyDefinition.getStreams()) {
      ComponentStream componentStream = new ComponentStream();
      componentStream.setFromComponent(def.getFrom());
      componentStream.setToComponent(def.getTo());
      componentStream.setStreamName(def.getName());
      componentStreams.put(def.getName(), componentStream);
      LOG.info("component stream: " + componentStream.toString());
    }

    executionContext.setStreams(componentStreams);

  }

  private static void buildBolts(EcoExecutionContext executionContext)
      throws IllegalAccessException, InstantiationException, ClassNotFoundException {
    EcoTopologyDefinition topologyDefinition = executionContext.getTopologyDefinition();
    Map<String, Object> bolts = new HashMap<>();

    for (ComponentDefinition def: topologyDefinition.getBolts()) {
      Object obj = buildObject(def);
      bolts.put(def.getName(), obj);
    }

    executionContext.setChildren(bolts);
  }

  private static void buildSpouts(EcoExecutionContext executionContext)
      throws ClassNotFoundException, InstantiationException, IllegalAccessException {
    EcoTopologyDefinition topologyDefinition = executionContext.getTopologyDefinition();
    Map<String, Object> spouts = new HashMap<>();

    for (ComponentDefinition def: topologyDefinition.getSpouts()) {
      Object obj = buildObject(def);
      spouts.put(def.getName(), obj);
    }

    executionContext.setSources(spouts);

  }

  @SuppressWarnings("rawtypes")
  private static Object buildObject(ComponentDefinition def)
      throws ClassNotFoundException, IllegalAccessException, InstantiationException {
    Class clazz = Class.forName(def.getClassName());
    return clazz.newInstance();

  }

  public static Config buildConfig(EcoTopologyDefinition topologyDefinition) {
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
