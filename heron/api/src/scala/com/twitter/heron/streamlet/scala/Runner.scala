//  Copyright 2018 Twitter. All rights reserved.
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
package com.twitter.heron.streamlet.scala

import java.util.logging.{Level, Logger}

import com.twitter.heron.api.HeronSubmitter
import com.twitter.heron.api.exception.AlreadyAliveException
import com.twitter.heron.api.exception.InvalidTopologyException
import com.twitter.heron.streamlet.Config
import com.twitter.heron.streamlet.scala.impl.BuilderImpl

/**
  * Runner is used to run a topology that is built by the builder.
  * It exports a sole function called run that takes care of constructing the topology
  */
class Runner {

  private val LOG = Logger.getLogger(classOf[Runner].getName)

  /**
    * Runs the computation
    *
    * @param name    The name of the topology
    * @param config  Any config that is passed to the topology
    * @param builder The builder used to keep track of the sources.
    */
  def run(name: String, config: Config, builder: Builder): Unit = {
    val builderImpl = builder.asInstanceOf[BuilderImpl]
    val topologyBuilder = builderImpl.build
    try {
      HeronSubmitter.submitTopology(name,
                                    config.getHeronConfig,
                                    topologyBuilder.createTopology)
    } catch {
      case e @ (_: AlreadyAliveException | _: InvalidTopologyException) =>
        LOG.log(Level.SEVERE, "Topology could not be submitted", e)
    }
  }
}
