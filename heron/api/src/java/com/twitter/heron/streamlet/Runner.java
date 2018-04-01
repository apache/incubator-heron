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

package com.twitter.heron.streamlet;

import com.twitter.heron.api.HeronSubmitter;
import com.twitter.heron.api.exception.AlreadyAliveException;
import com.twitter.heron.api.exception.InvalidTopologyException;
import com.twitter.heron.api.topology.TopologyBuilder;
import com.twitter.heron.streamlet.impl.BuilderImpl;

/**
 * Runner is used to run a topology that is built by the builder.
 * It exports a sole function called run that takes care of constructing the topology
 */
public final class Runner {
  public Runner() { }

  /**
   * Runs the computation
   * @param name The name of the topology
   * @param config Any config that is passed to the topology
   * @param builder The builder used to keep track of the sources.
   */
  public void run(String name, Config config, Builder builder) {
    BuilderImpl bldr = (BuilderImpl) builder;
    TopologyBuilder topologyBuilder = bldr.build();
    try {
      HeronSubmitter.submitTopology(name, config.getHeronConfig(),
                                    topologyBuilder.createTopology());
    } catch (AlreadyAliveException | InvalidTopologyException e) {
      e.printStackTrace();
    }
  }
}
