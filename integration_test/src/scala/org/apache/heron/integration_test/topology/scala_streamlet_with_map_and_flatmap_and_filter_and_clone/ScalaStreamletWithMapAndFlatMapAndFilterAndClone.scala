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
package org.apache.heron.integration_test.topology.scala_streamlet_with_map_and_flatmap_and_filter_and_clone

import scala.collection.mutable.Set

import org.apache.heron.api.Config
import org.apache.heron.integration_test.common.{
  AbstractTestTopology,
  ScalaIntegrationTestBase
}
import org.apache.heron.integration_test.core.TestTopologyBuilder
import org.apache.heron.streamlet.scala.Builder

object ScalaStreamletWithMapAndFlatMapAndFilterAndClone {
  val months = "january - february - march - april - may - june" +
    " - july - august - september - october - november - december"

  val summerMonths =
    List("june", "july", "august")

  val incomingMonths = Set[String]()

  def main(args: Array[String]): Unit = {
    val conf = new Config
    val topology = new ScalaStreamletWithMapAndFlatMapAndFilterAndClone(args)
    topology.submit(conf)
  }
}

/**
  * Scala Streamlet Integration Test by covering source, map, flatMap, filter and clone operations.
  */
class ScalaStreamletWithMapAndFlatMapAndFilterAndClone(args: Array[String])
    extends AbstractTestTopology(args)
    with ScalaIntegrationTestBase {

  import ScalaStreamletWithMapAndFlatMapAndFilterAndClone._

  override protected def buildTopology(
      testTopologyBuilder: TestTopologyBuilder): TestTopologyBuilder = {

    val streamletBuilder = Builder.newBuilder

    val clonedStreamlet = streamletBuilder
      .newSource(() => months)
      .setName("months-text")
      .flatMap[String]((m: String) => m.split(" - "))
      .setName("months")
      .filter((month: String) =>
        (summerMonths.contains(month.toLowerCase)
          && incomingMonths.add(month.toLowerCase)))
      .setName("summer-months")
      .map[String]((word: String) => word.substring(0, 3))
      .setName("summer-months-with-short-name")
      .clone(numClones = 2)

    //Returns Summer Months with Uppercase
    clonedStreamlet(0).map[String](month => month + "_2018")

    //Returns Summer Months with Uppercase
    clonedStreamlet(1).map[String](_.toUpperCase)

    build(testTopologyBuilder, streamletBuilder)
  }

}
