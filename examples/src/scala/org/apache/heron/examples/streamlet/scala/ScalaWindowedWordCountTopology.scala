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
import org.apache.heron.streamlet.{Config, KeyValue, KeyedWindow, WindowConfig}
import org.apache.heron.streamlet.scala.{Builder, Runner, StreamletReducers}

/**
  * This topology is an implementation of the classic word count example
  * for the Heron Streamlet API for Scala. A source streamlet emits an
  * indefinite series of sentences chosen at random from a pre-defined list.
  * Each sentence is then mapped to lower-case and "flattened" into a list of individual words. A
  * reduce function keeps a running tally of the number of times each word
  * is encountered within each time window (in this case a tumbling count
  * window of 50 operations). The result is then logged.
  */
object ScalaWindowedWordCountTopology {

  private val log =
    Logger.getLogger(ScalaWindowedWordCountTopology.getClass.getName)

  private val sentences = List(
    "I HAVE NOTHING TO DECLARE BUT MY GENIUS",
    "YOU CAN EVEN",
    "COMPASSION IS AN ACTION WORD WITH NO BOUNDARIES",
    "TO THINE OWN SELF BE TRUE"
  )

  def main(args: Array[String]): Unit = {
    val builder = Builder.newBuilder()

    val sentenceSource = builder
      .newSource(() => getRandomSentence())
      .setName("random-sentences-source")

    sentenceSource
      .map[String](_.toLowerCase)
      .setName("sentences-with-lower-case")
      .flatMap[String](_.split(" "))
      .setName("flatten-into-individual-words")
      .reduceByKeyAndWindow[String, Int]((word: String) => word,
                                         (x: String) => 1,
                                         WindowConfig.TumblingCountWindow(50),
                                         StreamletReducers.sum(_: Int, _: Int))
      .setName("reduce-operation")
      .consume((kv: KeyValue[KeyedWindow[String], Int]) =>
        log.info(s"word: ${kv.getKey.getKey} - count: ${kv.getValue}"))

    val config = Config.defaultConfig()

    // Fetches the topology name from the first command-line argument
    val topologyName = ScalaTopologyExampleUtils.getTopologyName(args)

    // Finally, the processing graph and configuration are passed to the Runner, which converts
    // the graph into a Heron topology that can be run in a Heron cluster.
    new Runner().run(topologyName, config, builder)
  }

  /**
    * This function provides a random selected sentence.
    */
  private def getRandomSentence() =
    sentences(Random.nextInt(sentences.size))
}
