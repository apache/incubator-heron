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

import org.apache.heron.streamlet.{Config, Context}
import org.apache.heron.streamlet.scala.{
  Builder,
  Runner,
  SerializableTransformer,
  Sink
}

/**
  * In this topology, a supplier generates an indefinite series of random integers between 0
  * and 99. From there, a series of transform operations are applied that ultimately leave
  * the original value unchanged.
  */
object ScalaTransformsAndCloneTopology {

  private val log =
    Logger.getLogger(ScalaTransformsAndCloneTopology.getClass.getName)

  def main(args: Array[String]): Unit = {
    val builder = Builder.newBuilder

    val numbers = builder
      .newSource(() => Random.nextInt(100))
      .setName("numbers-lower-than-100")

    val clonedStreamlets = numbers
      .transform(new IncrementTransformer(2000))
      .transform(new IncrementTransformer(-1300))
      .transform(new DoNothingTransformer())
      .transform(new IncrementTransformer(-500))
      .transform(new IncrementTransformer(-200))
      .clone(2)

    clonedStreamlets(0).log()

    clonedStreamlets(1).toSink(new FormattedLogSink)

    val config = Config.defaultConfig()

    // Fetches the topology name from the first command-line argument
    val topologyName = ScalaTopologyExampleUtils.getTopologyName(args)

    // Finally, the processing graph and configuration are passed to the Runner, which converts
    // the graph into a Heron topology that can be run in a Heron cluster.
    new Runner().run(topologyName, config, builder)
  }

  /**
    * This transformer leaves incoming values unmodified. The Consumer simply accepts incoming
    * values as-is during the transform phase.
    */
  private class DoNothingTransformer extends SerializableTransformer[Int, Int] {
    override def setup(context: Context): Unit = {}

    override def transform(i: Int, f: Int => Unit): Unit = f(i)

    override def cleanup(): Unit = {}
  }

  /**
    * This transformer increments incoming values by a user-supplied increment (which can also,
    * of course, be negative).
    */
  private class IncrementTransformer(factor: Int)
      extends SerializableTransformer[Int, Int] {
    override def setup(context: Context): Unit = {}

    override def transform(i: Int, f: Int => Unit): Unit = f(i + factor)

    override def cleanup(): Unit = {}
  }

  /**
    * This is a formatted log sink, which writes incoming tuples in defined log format.
    */
  private class FormattedLogSink extends Sink[Int] {
    override def setup(context: Context): Unit = {}

    override def put(tuple: Int): Unit =
      log.info(s"The current number is $tuple")

    override def cleanup(): Unit = {}
  }
}
