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

import java.util.{Map => JMap}

import org.apache.heron.api.bolt.BaseBasicBolt
import org.apache.heron.api.bolt.BasicOutputCollector
import org.apache.heron.api.topology.OutputFieldsDeclarer
import org.apache.heron.api.topology.TopologyContext
import org.apache.heron.api.tuple.Fields
import org.apache.heron.api.tuple.Tuple
import org.apache.heron.api.tuple.Values

class TestBasicBolt extends BaseBasicBolt {
  var tupleExecuted: Int = 0
  
  override def prepare(heronConf: JMap[String, Object], context: TopologyContext): Unit = {
  }

  override def execute(input: Tuple, collector: BasicOutputCollector): Unit = {
    tupleExecuted = tupleExecuted + 1
    val v = input.getDouble(0)
    collector.emit(new Values(v))
  }

  
  override def declareOutputFields(outputFieldsDeclarer: OutputFieldsDeclarer): Unit = {
    outputFieldsDeclarer.declare(new Fields("output"))
  }
}
