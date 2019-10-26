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

package org.apache.heron.integration_topology_test.common.spout;

import java.util.Map;

import org.apache.heron.api.spout.SpoutOutputCollector;
import org.apache.heron.api.topology.OutputFieldsDeclarer;
import org.apache.heron.api.topology.TopologyContext;
import org.apache.heron.api.tuple.Fields;
import org.apache.heron.api.tuple.Values;
import org.apache.heron.integration_topology_test.core.StatefulSpout;

public class StatefulABSpout extends StatefulSpout {

  private static final long serialVersionUID = 7431612805823106868L;
  private static final String[] TO_SEND = new String[]{"A", "B"};

  private int emitted = 0;
  private boolean appendSequenceId;

  public StatefulABSpout() {
    this(false);
  }

  public StatefulABSpout(boolean appendSequenceId) {
    this.appendSequenceId = appendSequenceId;
  }

  @Override
  public void open(Map<String, Object> conf,
                   TopologyContext newContext,
                   SpoutOutputCollector newCollector) {
    super.open(conf, newContext, newCollector);
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("word"));
  }

  @Override
  public void nextTuple() {
    String word = TO_SEND[emitted % TO_SEND.length];
    if (appendSequenceId) {
      word = word + "_" + emitted;
    }
    collector.emit(new Values(word));
    if (!state.containsKey(word)) {
      state.put(word, 1);
    } else {
      state.put(word, state.get(word) + 1);
    }
    emitted++;
  }
}
