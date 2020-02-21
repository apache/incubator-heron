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
package org.apache.heron.examples.streamlet.scala

import java.util.concurrent.ThreadLocalRandom

import org.apache.heron.examples.streamlet.scala.common.ScalaTopologyExampleUtils
import org.apache.heron.streamlet.Config
import org.apache.heron.streamlet.scala.{Builder, Runner}

/**
  * This is a very simple topology that shows a series of Scala Streamlet operations
  * on a source streamlet of random integers (between 1 and 10). First, 10 is multiplied
  * to each integer and then, the numbers that are lower than 50, are excluded form streamlet.
  * That streamlet is then united with a streamlet that consists of an indefinite stream of zeroes.
  * The final output of the processing graph is then logged.
  */
object ScalaIntegerProcessingTopology {

  private val CPU = 1.5f
  private val GIGABYTES_OF_RAM = 1
  private val NUM_CONTAINERS = 2

  /**
    * All Heron topologies require a main function that defines the topology's behavior
    * at runtime
    */
  def main(args: Array[String]): Unit = {
    val builder = Builder.newBuilder

    val zeroes = builder.newSource(() => 0).setName("zeroes")

    val numbers = builder
      .newSource(() => ThreadLocalRandom.current.nextInt(1, 11))
      .setName("random-numbers")

    numbers
      .map[Int]((i: Int) => i * 10)
      .setNumPartitions(2)
      .filter((i: Int) => i >= 50)
      .setName("numbers-higher-than-50")
      .union(zeroes)
      .setName("union-of-numbers")
      .log()
      .setName("log")
      .setNumPartitions(1)

    val config = Config.newBuilder
      .setNumContainers(NUM_CONTAINERS)
      .setPerContainerRamInGigabytes(GIGABYTES_OF_RAM)
      .setPerContainerCpu(CPU)
      .build

    // Fetches the topology name from the first command-line argument
    val topologyName = ScalaTopologyExampleUtils.getTopologyName(args)

    // Finally, the processing graph and configuration are passed to the Runner, which converts
    // the graph into a Heron topology that can be run in a Heron cluster.
    new Runner().run(topologyName, config, builder)
  }

}
