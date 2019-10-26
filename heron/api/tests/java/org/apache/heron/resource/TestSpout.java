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

package org.apache.heron.resource;


import java.util.List;
import java.util.Map;

import org.junit.Ignore;

import org.apache.heron.api.spout.BaseRichSpout;
import org.apache.heron.api.spout.SpoutOutputCollector;
import org.apache.heron.api.topology.OutputFieldsDeclarer;
import org.apache.heron.api.topology.TopologyContext;
import org.apache.heron.api.tuple.Fields;
import org.apache.heron.api.tuple.Values;

/**
 * A Spout used for unit test, it will:
 * 1. It will emit EMIT_COUNT of tuples with MESSAGE_ID.
 * 2. The tuples are declared by outputFieldsDeclarer in fields "word"
 */

@Ignore
public class TestSpout extends BaseRichSpout {
  private static final int EMIT_COUNT = 10;
  private static final String MESSAGE_ID = "MESSAGE_ID";

  private final String[] toSend = new String[]{"A", "B"};
  private SpoutOutputCollector outputCollector;
  private int emitted = 0;

  @Override
  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    outputFieldsDeclarer.declare(new Fields("word"));
  }

  @Override
  public void open(
      Map<String, Object> map,
      TopologyContext topologyContext,
      SpoutOutputCollector spoutOutputCollector) {
    this.outputCollector = spoutOutputCollector;
  }

  @Override
  public void close() {
  }

  @Override
  public void nextTuple() {
    // It will emit A, B, A, B, A, B, A, B, A, B
    if (emitted < EMIT_COUNT) {
      String word = toSend[emitted % toSend.length];
      emit(outputCollector, new Values(word), MESSAGE_ID, emitted++);
    }
  }

  protected void emit(SpoutOutputCollector collector,
                      List<Object> tuple, Object messageId, int emittedCount) {
    collector.emit(tuple, messageId);
  }
}

