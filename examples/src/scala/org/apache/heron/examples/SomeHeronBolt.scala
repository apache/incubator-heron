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
package org.apache.heron.examples
import java.util.{Map => JMap}
import java.lang.{Long => JLong, Double => JDouble}

import org.apache.heron.api.bolt.{BaseBasicBolt, BasicOutputCollector}
import org.apache.heron.api.metric.{IMetric, MeanReducer, MultiCountMetric, MultiReducedMetric, MeanReducerState}
import org.apache.heron.api.topology.{TopologyContext, OutputFieldsDeclarer}
import org.apache.heron.api.tuple.Tuple

/**
 * This bolt is the same as SomeStormBolt, but modified to use the Heron APIs that uses generics
 */
class SomeHeronBolt extends BaseBasicBolt {

  val countMetrics:MultiCountMetric = new MultiCountMetric
  val reducedMetrics:MultiReducedMetric[MeanReducerState, Number, JDouble] =
    new MultiReducedMetric[MeanReducerState, Number, JDouble](new MeanReducer)

  // stormConf is now a typed Map
  override def prepare(stormConf: JMap[String, Object], context: TopologyContext): Unit = {
    super.prepare(stormConf, context)
    // Initialize metric counters at object initialization time now
    context.registerMetric[IMetric[JMap[String, JLong]], JMap[String, JLong]]("countmetrics", countMetrics, 60)
    context.registerMetric[IMetric[JMap[String, JDouble]], JMap[String, JDouble]]("avgmetrics", reducedMetrics, 60)
  }

  override def execute(tuple:Tuple, collector: BasicOutputCollector): Unit = {
    tuple getValue 1 match {
      case json: String if json.trim.nonEmpty =>
        AnotherThingThatDoesStuff.doStuff(json, reducedMetrics)
      case _ =>
        countMetrics scope "unexpected_input" incr()
    }
  }

  override def declareOutputFields(declarer: OutputFieldsDeclarer): Unit = {}
}

object AnotherThingThatDoesStuff {

  def doStuff(str: String, avgStat:MultiReducedMetric[_,Number,_]): Unit = {
    // Do something with String

    // Update metrics
    avgStat scope "some_avg" update 10
  }
}