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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import com.twitter.heron.eco.definition.ComponentDefinition;
import com.twitter.heron.eco.definition.ComponentStream;
import com.twitter.heron.eco.definition.EcoExecutionContext;
import com.twitter.heron.eco.definition.EcoTopologyDefinition;
import com.twitter.heron.eco.definition.StreamDefinition;
import com.twitter.heron.streamlet.Builder;
import com.twitter.heron.streamlet.Config;
import com.twitter.heron.streamlet.SerializableSupplier;
import com.twitter.heron.streamlet.Source;
import com.twitter.heron.streamlet.Streamlet;
import com.twitter.heron.streamlet.impl.StreamletImpl;

public class EcoBuilder {

  private static final Logger LOG = Logger.getLogger(EcoBuilder.class.getName());
  public static Builder buildBuilder(EcoExecutionContext executionContext) throws InstantiationException, IllegalAccessException, ClassNotFoundException {

    buildSources(executionContext);
    buildChildren(executionContext);
    buildStreams(executionContext);

    return build(executionContext);
  }

  private static Builder build(EcoExecutionContext executionContext) {
    Builder builder = Builder.newBuilder();
    StreamletImpl
    Map<String, Object> sources = executionContext.getSources();
    for (Map.Entry<String, Object> entry: sources.entrySet()) {
      Object value = entry.getValue();

      if (value instanceof SerializableSupplier) {
        LOG.info("eco sink is instance of serialzableSupplier");
        SerializableSupplier supplier = (SerializableSupplier) value;


      } else if (value instanceof Source) {
        LOG.info("eco sink is instance of Source");
        builder.newSource((Source) value);

      } else {
        LOG.info("eco sink is instance of who knows");
        LOG.info("KEY: " + value.toString());
      }
    }

    Map<String, Object>  children = executionContext.getChildren();
    for (Map.Entry<String, Object> entry: children.entrySet()) {
      Object value = entry.getValue();
    }

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

  private static void buildChildren(EcoExecutionContext executionContext) throws IllegalAccessException, InstantiationException, ClassNotFoundException {
    EcoTopologyDefinition topologyDefinition = executionContext.getTopologyDefinition();
    Map<String, Object> children = new HashMap<>();

    for (ComponentDefinition def: topologyDefinition.getChildren()) {
      Object obj = buildObject(def);
      children.put(def.getName(), obj);
    }

    executionContext.setChildren(children);
  }

  private static void buildSources(EcoExecutionContext executionContext) throws ClassNotFoundException, InstantiationException, IllegalAccessException {
    EcoTopologyDefinition topologyDefinition = executionContext.getTopologyDefinition();
    Map<String, Object> sources = new HashMap<>();

    for (ComponentDefinition def: topologyDefinition.getSources()) {
      Object obj = buildObject(def);
      sources.put(def.getName(), obj);
    }

    executionContext.setSources(sources);

  }

  @SuppressWarnings("rawtypes")
  private static Object buildObject(ComponentDefinition def) throws ClassNotFoundException, IllegalAccessException, InstantiationException {
    Class clazz = Class.forName(def.getClassName());
    return clazz.newInstance();

  }

}
