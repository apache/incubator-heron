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


import java.lang.reflect.InvocationTargetException;

import org.apache.storm.Config;
import org.apache.storm.topology.TopologyBuilder;

import com.twitter.heron.eco.definition.EcoExecutionContext;
import com.twitter.heron.eco.definition.EcoTopologyDefinition;


public class EcoBuilder {

  private SpoutBuilder spoutBuilder;

  private BoltBuilder boltBuilder;

  private StreamBuilder streamBuilder;

  private ComponentBuilder componentBuilder;

  private ConfigBuilder configBuilder;

  public EcoBuilder(SpoutBuilder spoutBuilder, BoltBuilder boltBuilder,
                    StreamBuilder streamBuilder, ComponentBuilder componentBuilder,
                    ConfigBuilder configBuilder) {
    this.spoutBuilder = spoutBuilder;
    this.boltBuilder = boltBuilder;
    this.streamBuilder = streamBuilder;
    this.componentBuilder = componentBuilder;
    this.configBuilder = configBuilder;
  }

  public TopologyBuilder buildTopologyBuilder(EcoExecutionContext executionContext,
                                              ObjectBuilder objectBuilder)
      throws InstantiationException, IllegalAccessException,
      ClassNotFoundException,
      NoSuchFieldException, InvocationTargetException {

    TopologyBuilder builder = new TopologyBuilder();
    componentBuilder.buildComponents(executionContext, objectBuilder);
    spoutBuilder.buildSpouts(executionContext, builder, objectBuilder);
    boltBuilder.buildBolts(executionContext, objectBuilder);
    streamBuilder.buildStreams(executionContext, builder, objectBuilder);

    return builder;
  }

  public Config buildConfig(EcoTopologyDefinition topologyDefinition) {
    return this.configBuilder.buildConfig(topologyDefinition);
  }
}
