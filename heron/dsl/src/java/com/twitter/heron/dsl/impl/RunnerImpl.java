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

package com.twitter.heron.dsl.impl;

import com.twitter.heron.api.Config;
import com.twitter.heron.api.HeronSubmitter;
import com.twitter.heron.api.exception.AlreadyAliveException;
import com.twitter.heron.api.exception.InvalidTopologyException;
import com.twitter.heron.api.topology.TopologyBuilder;
import com.twitter.heron.dsl.Builder;
import com.twitter.heron.dsl.Runner;

/**
 * RunnerImpl implements the Runner interface. Currently its a
 * straightforward implementation that builds the Topology using
 * TopologyBuilder and submits it using HeronTopology.submitTopology
 */
public final class RunnerImpl implements Runner {
  @Override
  public void run(String name, Config config, Builder builder) {
    BuilderImpl bldr = (BuilderImpl) builder;
    TopologyBuilder topologyBuilder = bldr.build();
    try {
      HeronSubmitter.submitTopology(name, config, topologyBuilder.createTopology());
    } catch (AlreadyAliveException | InvalidTopologyException e) {
      e.printStackTrace();
    }
  }

  public RunnerImpl() { }
}
