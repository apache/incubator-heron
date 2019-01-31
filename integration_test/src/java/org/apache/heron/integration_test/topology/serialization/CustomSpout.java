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

package org.apache.heron.integration_test.topology.serialization;

import java.util.Map;

import org.apache.heron.api.spout.BaseRichSpout;
import org.apache.heron.api.spout.SpoutOutputCollector;
import org.apache.heron.api.topology.OutputFieldsDeclarer;
import org.apache.heron.api.topology.TopologyContext;
import org.apache.heron.api.tuple.Fields;
import org.apache.heron.api.tuple.Values;

/**
 * A spout that emits two different custom objects continuously in order,
 * one object every "nextTuple()" called
 * Note that CustomObject uses Java serialization
 */
public class CustomSpout extends BaseRichSpout {
  private static final long serialVersionUID = -5906196959214294058L;
  private SpoutOutputCollector collector;
  private int emitted = 0;
  private CustomObject[] inputObjects;

  public CustomSpout(CustomObject[] inputObjects) {
    this.inputObjects = inputObjects;
  }

  @Override
  public void open(Map<String, Object> conf,
                   TopologyContext context,
                   SpoutOutputCollector outputCollector) {
    this.collector = outputCollector;
  }

  @Override
  public void nextTuple() {
    CustomObject obj = inputObjects[(emitted++) % inputObjects.length];
    collector.emit(new Values(obj));
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("custom"));
  }
}
