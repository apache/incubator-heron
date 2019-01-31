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
package org.apache.heron.integration_test.topology.scala_streamlet_with_split_and_with_stream

import java.util.concurrent.atomic.AtomicInteger

import org.apache.heron.api.Config
import org.apache.heron.integration_test.core.TestTopologyBuilder
import org.apache.heron.streamlet.Context

import org.apache.heron.integration_test.common.{
  AbstractTestTopology,
  ScalaIntegrationTestBase
}
import org.apache.heron.streamlet.scala.{
  Builder, SerializableTransformer
}

object ScalaStreamletWithSplitAndWithStream {
  def main(args: Array[String]): Unit = {
    val conf = new Config
    val topology = new ScalaStreamletWithSplitAndWithStream(args)
    topology.submit(conf)
  }
}

/**
  * Scala Streamlet Integration Test
  */
class ScalaStreamletWithSplitAndWithStream(args: Array[String])
    extends AbstractTestTopology(args)
    with ScalaIntegrationTestBase {

  override protected def buildTopology(
      testTopologyBuilder: TestTopologyBuilder): TestTopologyBuilder = {
    val atomicInteger = new AtomicInteger(-3)

    val streamletBuilder = Builder.newBuilder

    val multi = streamletBuilder
        .newSource(() => atomicInteger.getAndIncrement())
        .setName("incremented-numbers-from--3")
        .filter(i => i <= 4)
        .setName("numbers-lower-than-4")
        .split(Map(
            "all" -> { num => true },
            "positive" -> { num => num > 0 },
            "negative" -> { num => num < 0 }
        ))
        .setName("split")

    multi.withStream("all").map { i => s"all_$i" }
    multi.withStream("positive").map { i => s"pos_$i" }
    multi.withStream("negative").map{ i => s"neg_$i" }

    build(testTopologyBuilder, streamletBuilder)
  }
}
