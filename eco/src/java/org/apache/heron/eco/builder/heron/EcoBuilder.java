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

package org.apache.heron.eco.builder.heron;


import java.lang.reflect.InvocationTargetException;
import java.util.logging.Logger;

import org.apache.heron.api.Config;
import org.apache.heron.api.topology.TopologyBuilder;
import org.apache.heron.eco.builder.BoltBuilder;
import org.apache.heron.eco.builder.ComponentBuilder;
import org.apache.heron.eco.builder.ConfigBuilder;
import org.apache.heron.eco.builder.ObjectBuilder;
import org.apache.heron.eco.definition.EcoExecutionContext;
import org.apache.heron.eco.definition.EcoTopologyDefinition;


public class EcoBuilder {

  private SpoutBuilder spoutBuilder;

  private BoltBuilder boltBuilder;

  private StreamBuilder streamBuilder;

  private ComponentBuilder componentBuilder;

  private ConfigBuilder configBuilder;

  private static final Logger LOG = Logger.getLogger(EcoBuilder.class.getName());

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
    LOG.info("Building components");
    componentBuilder.buildComponents(executionContext, objectBuilder);
    LOG.info("Building spouts");
    spoutBuilder.buildSpouts(executionContext, builder, objectBuilder);
    LOG.info("Building bolts");
    boltBuilder.buildBolts(executionContext, objectBuilder);
    LOG.info("Building streams");
    streamBuilder.buildStreams(executionContext, builder, objectBuilder);

    return builder;
  }

  public Config buildConfig(EcoTopologyDefinition topologyDefinition) throws Exception {
    LOG.info("Building topology config");
    return this.configBuilder.buildConfig(topologyDefinition);
  }
}
