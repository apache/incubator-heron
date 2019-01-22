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

package org.apache.heron.integration_test.common.spout;

import java.util.Map;

import org.apache.heron.api.spout.BaseRichSpout;
import org.apache.heron.api.spout.SpoutOutputCollector;
import org.apache.heron.api.topology.OutputFieldsDeclarer;
import org.apache.heron.api.topology.TopologyContext;
import org.apache.heron.api.tuple.Fields;
import org.apache.heron.api.tuple.Values;


/**
 * A spout that emit "A" and "B" continuously in order, one word every "nextTuple()" called. If
 * {@code appendSequenceId} is true, appends and underscore followed by the sequence id.
 */
public class ABSpout extends BaseRichSpout {
  private static final long serialVersionUID = 3233011943332591934L;
  private static final String[] TO_SEND = new String[]{"A", "B"};

  private SpoutOutputCollector collector;
  private int emitted = 0;
  private boolean appendSequenceId;

  public ABSpout() {
    this(false);
  }

  public ABSpout(boolean appendSequenceId) {
    this.appendSequenceId = appendSequenceId;
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("word"));
  }

  @Override
  public void open(Map<String, Object> conf,
                   TopologyContext context,
                   SpoutOutputCollector newCollector) {
    this.collector = newCollector;
  }

  @Override
  public void nextTuple() {
    String word = TO_SEND[emitted % TO_SEND.length];
    if (appendSequenceId) {
      word = word + "_" + emitted;
    }
    collector.emit(new Values(word));
    emitted++;
  }
}
