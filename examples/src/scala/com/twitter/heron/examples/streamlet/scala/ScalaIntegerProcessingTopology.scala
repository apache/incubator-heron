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

//  http://www.apache.org/licenses/LICENSE-2.0

//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
package com.twitter.heron.examples.streamlet.scala

import java.util.concurrent.ThreadLocalRandom

import com.twitter.heron.streamlet.Config

import com.twitter.heron.streamlet.scala.{Builder, Runner}

//TODO - Add javadoc

object ScalaIntegerProcessingTopology {

  private val CPU = 1.5f
  private val GIGABYTES_OF_RAM = 1
  private val NUM_CONTAINERS = 2

  /**
    * All Heron topologies require a main function that defines the topology's behavior
    * at runtime
    */
  @throws[Exception]
  def main(args: Array[String]): Unit = {
    val builder = Builder.newBuilder

    val zeroes = builder.newSource(() => 0).setName("zeroes")

    builder
      .newSource(() => ThreadLocalRandom.current.nextInt(1, 11))
      .setName("random-numbers")
      .map[Int]((i: Int) => (i * 10))
      .setNumPartitions(3)
      .filter((i: Int) => i >= 50)
      .setName("numbers-higher-than-50")
      .union(zeroes)
      .setName("unioned-numbers")
      .setNumPartitions(3)
      .log()

    val config = Config.newBuilder
      .setNumContainers(NUM_CONTAINERS)
      .setPerContainerRamInGigabytes(GIGABYTES_OF_RAM)
      .setPerContainerCpu(CPU)
      .build

    // Fetches the topology name from the first command-line argument
    val topologyName = getTopologyName(args)

    // Finally, the processing graph and configuration are passed to the Runner, which converts
    // the graph into a Heron topology that can be run in a Heron cluster.
    new Runner().run(topologyName, config, builder)
  }

  /**
    * Fetches the topology's name from the first command-line argument or
    * throws an exception if not present.
    */
  @throws[Exception]
  private def getTopologyName(args: Array[String]): String = {
    require(args.length > 0, "A Topology name should be supplied")
    args(0)
  }

}

final class ScalaIntegerProcessingTopology private () {}
