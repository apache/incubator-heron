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


import java.util.{List => JList}
import java.util.{Map => JMap}

import org.junit.Ignore

import org.apache.heron.api.spout.BaseRichSpout
import org.apache.heron.api.spout.SpoutOutputCollector
import org.apache.heron.api.topology.OutputFieldsDeclarer
import org.apache.heron.api.topology.TopologyContext
import org.apache.heron.api.tuple.Fields
import org.apache.heron.api.tuple.Values

/**
 * A Spout used for unit test, it will:
 * 1. It will emit EMIT_COUNT of tuples with MESSAGE_ID.
 * 2. The tuples are declared by outputFieldsDeclarer in fields "word"
 */

@Ignore
class TestSpout extends BaseRichSpout {
  private val EMIT_COUNT = 10
  private val MESSAGE_ID = "MESSAGE_ID"

  private val toSend = Array("A", "B")
  private var outputCollector: SpoutOutputCollector = _
  private var emitted = 0

  override def declareOutputFields(outputFieldsDeclarer: OutputFieldsDeclarer ): Unit = {
    outputFieldsDeclarer.declare(new Fields("word"))
  }

  override def open(
      conf: JMap[String, Object],
      topologyContext: TopologyContext,
      spoutOutputCollector: SpoutOutputCollector): Unit = {
    this.outputCollector = spoutOutputCollector
  }

  override def close(): Unit = {
  }

  override def nextTuple(): Unit = {
    // It will emit A, B, A, B, A, B, A, B, A, B
    if (emitted < EMIT_COUNT) {
      val word = toSend(emitted % toSend.length)
      emit(outputCollector, new Values(word), MESSAGE_ID, emitted)
      emitted = emitted + 1
    }
  }

  def emit(collector: SpoutOutputCollector,
           tuple: JList[Object],
           messageId: Object,
           emittedCount: Int): Unit = {
    collector.emit(tuple, messageId)
  }
}
