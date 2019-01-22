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
import java.util

import backtype.storm.metric.api.{MeanReducer, MultiCountMetric, MultiReducedMetric}
import backtype.storm.task.TopologyContext
import backtype.storm.topology.{BasicOutputCollector, OutputFieldsDeclarer}
import backtype.storm.topology.base.BaseBasicBolt
import backtype.storm.tuple.Tuple

/**
 * This is an example of a bolt that has some patterns that might be in use in scala that is
 * compatible with the Storm API. It will run on both Storm and Heron.
 */
class SomeStormBolt extends BaseBasicBolt {

  var countMetrics: MultiCountMetric = _
  var reducedMetrics: MultiReducedMetric = _

  override def prepare(stormConf: util.Map[_,_], context: TopologyContext): Unit = {
    super.prepare(stormConf, context)
    countMetrics = new MultiCountMetric
    reducedMetrics = new MultiReducedMetric(new MeanReducer)
    context.registerMetric("countmetrics", countMetrics, 60)
    context.registerMetric("avgmetrics", reducedMetrics, 60)
  }

  override def execute(tuple:Tuple, collector: BasicOutputCollector): Unit = {
    tuple getValue 1 match {
      case str: String if str.trim.nonEmpty =>
        SomethingThatDoesStuff.doStuff(str, reducedMetrics)
      case _ =>
        countMetrics scope "unexpected_input" incr()
    }
  }

  override def declareOutputFields(declarer: OutputFieldsDeclarer): Unit = {}
}

object SomethingThatDoesStuff {

  def doStuff(str:String, metric:MultiReducedMetric): Unit ={
    // Do something with String

    // Update metrics
    metric scope "some_avg" update 10
  }
}
