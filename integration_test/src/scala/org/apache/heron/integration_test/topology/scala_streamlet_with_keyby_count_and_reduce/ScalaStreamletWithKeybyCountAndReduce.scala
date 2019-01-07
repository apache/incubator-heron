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
package org.apache.heron.integration_test.topology.scala_streamlet_with_keyby_count_and_reduce

import scala.collection.mutable.Set

import org.apache.heron.api.Config
import org.apache.heron.integration_test.common.{
  AbstractTestTopology,
  ScalaIntegrationTestBase
}
import org.apache.heron.integration_test.core.TestTopologyBuilder
import org.apache.heron.streamlet.scala.Builder

object ScalaStreamletWithKeybyCountAndReduce {
  final val months = "january - february - march - april - may - june" +
    " - july - august - september - october - november - december"

  final val SPRING_MONTHS = List("march", "april", "may")
  final val SUMMER_MONTHS = List("june", "july", "august")
  final val FALL_MONTHS = List("september", "october", "november")
  final val WINTER_MONTHS = List("december", "january", "february")

  val incomingMonths = Set[String]()

  def main(args: Array[String]): Unit = {
    val conf = new Config
    val topology = new ScalaStreamletWithKeybyCountAndReduce(args)
    topology.submit(conf)
  }

  def getSeason(month: String): String = {
    month match {
      case x if SPRING_MONTHS.contains(x) => "spring"
      case x if SUMMER_MONTHS.contains(x) => "summer"
      case x if FALL_MONTHS.contains(x) => "fall"
      case x if WINTER_MONTHS.contains(x) => "winter"
      case _ => "really?"
    }
  }

  def getNumberOfDays(month: String): Int = {
    month match {
      case "january" => 31
        case "february" => 28       // Dont use this code in real projects
        case "march" => 31
        case "april" => 30
        case "may" => 31
        case "june"  => 30
        case "july" => 31
        case "august" => 31
        case "september" => 30
        case "october" => 31
        case "november" => 30
        case "december" => 31
        case _ => -1                // Shouldn't be here
    }
  }
}

/**
  * Scala Streamlet Integration Test by covering keyBy, CountByKey and ReduceByKey operations.
  */
class ScalaStreamletWithKeybyCountAndReduce(args: Array[String])
    extends AbstractTestTopology(args)
    with ScalaIntegrationTestBase {

  import ScalaStreamletWithKeybyCountAndReduce._

  override protected def buildTopology(
      testTopologyBuilder: TestTopologyBuilder): TestTopologyBuilder = {

    val streamletBuilder = Builder.newBuilder

    val monthStreamlet = streamletBuilder
      .newSource(() => months)
      .setName("months-text")
      .flatMap[String]((m: String) => m.split(" - "))
      .setName("months")
      .filter((month: String) => incomingMonths.add(month.toLowerCase))
      .setName("unique-months")

    // Count months per season
    monthStreamlet
        .keyBy(getSeason(_), getNumberOfDays(_))
        .setName("key-by-season")
        .countByKey(_.getKey)
        .setName("key-by-and-count")
        .map(x => s"${x.getKey}: ${x.getValue} months")
        .setName("to-string")

    // Sum days per season
    monthStreamlet
        .reduceByKey[String, Int](
            getSeason(_), getNumberOfDays(_), (x: Int, y: Int) => x + y)
        .setName("sum-by-season")
        .map(x => s"${x.getKey}: ${x.getValue} days")
        .setName("to-string-2")

    build(testTopologyBuilder, streamletBuilder)
  }
}
