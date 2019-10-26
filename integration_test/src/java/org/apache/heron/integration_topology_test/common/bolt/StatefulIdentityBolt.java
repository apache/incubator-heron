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

package org.apache.heron.integration_topology_test.common.bolt;

import java.util.Map;
import java.util.logging.Logger;

import org.apache.heron.api.bolt.OutputCollector;
import org.apache.heron.api.state.State;
import org.apache.heron.api.topology.OutputFieldsDeclarer;
import org.apache.heron.api.topology.TopologyContext;
import org.apache.heron.api.tuple.Fields;
import org.apache.heron.api.tuple.Tuple;
import org.apache.heron.integration_topology_test.core.StatefulBolt;

public class StatefulIdentityBolt extends StatefulBolt {
  private static final long serialVersionUID = 1174496806359441609L;
  private static final Logger LOG = Logger.getLogger(StatefulIdentityBolt.class.getName());
  private Fields fields;

  public StatefulIdentityBolt() { }

  public StatefulIdentityBolt(Fields fields) {
    this.fields = fields;
  }

  @Override
  public void prepare(Map<String, Object> map,
                      TopologyContext context,
                      OutputCollector outputCollector) {
    super.prepare(map, context, outputCollector);
  }

  @Override
  public void execute(Tuple input) {
    StringBuilder sb = new StringBuilder();
    for (Object o : input.getValues()) {
      if (sb.length() > 0) {
        sb.append(",");
      }
      sb.append(o.toString());
    }
    String word = sb.toString();
    LOG.info("Receiving and emitting tuple values: " + word);
    if (!state.containsKey(word)) {
      state.put(word, 1);
    } else {
      state.put(word, state.get(word) + 1);
    }
    collector.emit(input.getValues());
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(fields);
  }

  @Override
  public void initState(State<String, Integer> state) {
    super.initState(state);
  }
}
