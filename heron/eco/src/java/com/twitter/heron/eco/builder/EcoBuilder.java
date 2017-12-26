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


import com.twitter.heron.api.Config;
import com.twitter.heron.api.topology.TopologyBuilder;

import com.twitter.heron.eco.definition.EcoExecutionContext;
import com.twitter.heron.eco.definition.EcoTopologyDefinition;


public final class EcoBuilder extends BaseBuilder {

  private SpoutBuilder spoutBuilder;

  public EcoBuilder(SpoutBuilder spoutBuilder) {
    this.spoutBuilder = spoutBuilder;
  }

  public TopologyBuilder buildTopologyBuilder(EcoExecutionContext executionContext)
      throws InstantiationException, IllegalAccessException, ClassNotFoundException {

    TopologyBuilder builder = new TopologyBuilder();
    spoutBuilder.addSpoutsToExecutionContext(executionContext, builder);
    BoltBuilder.buildBolts(executionContext);
    StreamBuilder.buildStreams(executionContext, builder);

    return builder;
  }

  public Config buildConfig(EcoTopologyDefinition topologyDefinition) {
    return ConfigBuilder.buildConfig(topologyDefinition);
  }
}
