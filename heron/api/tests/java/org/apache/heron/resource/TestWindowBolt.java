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

import java.util.Map;

import org.apache.heron.api.bolt.BaseWindowedBolt;
import org.apache.heron.api.bolt.OutputCollector;
import org.apache.heron.api.topology.OutputFieldsDeclarer;
import org.apache.heron.api.topology.TopologyContext;
import org.apache.heron.api.tuple.Fields;
import org.apache.heron.api.tuple.Values;
import org.apache.heron.api.windowing.TupleWindow;

public class TestWindowBolt extends BaseWindowedBolt {
  private OutputCollector outputCollector;
  private int tupleExecuted;

  @Override
  public void prepare(Map<String, Object> topoConf, TopologyContext context,
                      OutputCollector collector) {
    this.outputCollector = collector;
    this.tupleExecuted = 0;
  }

  @Override
  public void execute(TupleWindow inputWindow) {
    tupleExecuted++;
    outputCollector.emit(new Values(inputWindow.get().size()));
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("count"));
  }

  public int getExecuted() {
    return tupleExecuted;
  }
}

