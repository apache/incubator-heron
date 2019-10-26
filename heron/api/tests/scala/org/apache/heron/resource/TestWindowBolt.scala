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

package org.apache.heron.resource

import java.lang.{Double => JDouble}
import java.util.{Map => JMap}

import org.apache.heron.api.bolt.BaseWindowedBolt
import org.apache.heron.api.bolt.OutputCollector
import org.apache.heron.api.topology.OutputFieldsDeclarer
import org.apache.heron.api.topology.TopologyContext
import org.apache.heron.api.tuple.Fields
import org.apache.heron.api.tuple.Values
import org.apache.heron.api.windowing.TupleWindow

class TestWindowBolt extends BaseWindowedBolt {
  var outputCollector: OutputCollector = _
  var tupleExecuted: Int = 0
  
  override def prepare(topoConf: JMap[String, Object], context: TopologyContext,
                       collector: OutputCollector): Unit = {
    this.outputCollector = collector
  }

  override def execute(inputWindow: TupleWindow): Unit = {
    tupleExecuted = tupleExecuted + 1
    val size: JDouble = inputWindow.get().size()
    outputCollector.emit(new Values(size))
  }

  override def declareOutputFields(declarer: OutputFieldsDeclarer): Unit = {
    declarer.declare(new Fields("count"))
  }
}

