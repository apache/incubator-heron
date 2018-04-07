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
package com.twitter.heron.integration_test.topology.scala_streamlet_with_filter_and_transform

import java.util.concurrent.atomic.AtomicInteger

import com.twitter.heron.api.Config
import com.twitter.heron.integration_test.common.{
  AbstractTestTopology,
  ScalaIntegrationTestBase
}
import com.twitter.heron.integration_test.core.TestTopologyBuilder
import com.twitter.heron.streamlet.Context
import com.twitter.heron.streamlet.scala.{Builder, SerializableTransformer}

object ScalaStreamletWithFilterAndTransform {
  def main(args: Array[String]): Unit = {
    val conf = new Config
    val topology = new ScalaStreamletWithFilterAndTransform(args)
    topology.submit(conf)
  }
}

/**
  * Scala Streamlet Integration Test
  */
@SerialVersionUID(-7280407024398984674L)
class ScalaStreamletWithFilterAndTransform(args: Array[String])
    extends AbstractTestTopology(args)
    with ScalaIntegrationTestBase {

  override protected def buildTopology(
      testTopologyBuilder: TestTopologyBuilder): TestTopologyBuilder = {
    val atomicInteger = new AtomicInteger

    val streamletBuilder = Builder.newBuilder

    streamletBuilder
      .newSource(() => atomicInteger.getAndIncrement())
      .setName("incremented-numbers")
      .filter((i: Int) => i <= 7)
      .setName("positive-numbers-lower-than-8")
      .transform[String](new TextTransformer())
      .setName("numbers-transformed-to-text")

    build(testTopologyBuilder, streamletBuilder)
  }

}

private class TextTransformer extends SerializableTransformer[Int, String] {
  private val alphabet = List("a", "b", "c", "d", "e", "f", "g", "h")

  override def setup(context: Context): Unit = {}

  override def transform(i: Int, fun: String => Unit): Unit =
    fun(s"${alphabet(i)}-$i".toUpperCase)

  override def cleanup(): Unit = {}
}
