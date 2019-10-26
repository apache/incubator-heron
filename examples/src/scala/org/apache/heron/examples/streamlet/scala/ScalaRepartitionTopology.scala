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

import java.util.logging.Logger

import scala.util.Random

import org.apache.heron.examples.streamlet.scala.common.ScalaTopologyExampleUtils
import org.apache.heron.streamlet.Config
import org.apache.heron.streamlet.scala.{Builder, Runner}

/**
  * This topology demonstrates the usage of a simple repartitioning algorithm
  * using the Heron Streamlet API for Scala. Normally, streamlet elements are
  * distributed randomly across downstream instances when processed.
  * Repartitioning enables you to select which instances (partitions) to send
  * elements to on the basis of a user-defined logic. Here, a source streamlet
  * emits an indefinite series of random integers between 0 and 99. The value
  * of that number then determines to which topology instance (partition) the
  * element is routed.
  */
object ScalaRepartitionTopology {

  private val log =
    Logger.getLogger(ScalaRepartitionTopology.getClass.getName)

  def main(args: Array[String]): Unit = {
    val builder = Builder.newBuilder

    val numbers = builder
      .newSource(() => Random.nextInt(100))
      .setNumPartitions(2)
      .setName("numbers-lower-than-100")

    numbers
      .repartition(5, repartitionFn)
      .setName("repartitioned-incoming-values")
      .repartition(2)
      .setName("reduce-partitions-for-logging-operation")
      .log()

    val config = Config.defaultConfig()

    // Fetches the topology name from the first command-line argument
    val topologyName = ScalaTopologyExampleUtils.getTopologyName(args)

    // Finally, the processing graph and configuration are passed to the Runner, which converts
    // the graph into a Heron topology that can be run in a Heron cluster.
    new Runner().run(topologyName, config, builder)
  }

  /**
    * The repartition function that determines to which partition each incoming
    * streamlet element is routed (across 5 possible partitions). Integers between 0
    * and 19 are routed to partition 1, integers between 20 and 39 to partition 2,
    * and so on.
    */
  private def repartitionFn(incomingInteger: Int, numPartitions: Int) = {
    val partitionIndex = incomingInteger match {
      case x if (x >= 0 && x <= 99) => x / 20 + 1
      case x =>
        throw new IllegalArgumentException(
          s"Incoming number($x) must not be higher than 100")
    }

    log.info(s"Sending value: $incomingInteger to partitions: $partitionIndex")

    List(partitionIndex)
  }
}
