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

package org.apache.heron.examples.eco;

import java.util.Map;
import java.util.Random;
import java.util.logging.Logger;

import org.apache.heron.api.spout.BaseRichSpout;
import org.apache.heron.api.spout.SpoutOutputCollector;
import org.apache.heron.api.state.State;
import org.apache.heron.api.topology.IStatefulComponent;
import org.apache.heron.api.topology.OutputFieldsDeclarer;
import org.apache.heron.api.topology.TopologyContext;
import org.apache.heron.api.tuple.Fields;
import org.apache.heron.api.tuple.Values;
import org.apache.heron.api.utils.Utils;

@SuppressWarnings("HiddenField")
public class StatefulNumberSpout extends BaseRichSpout
    implements IStatefulComponent<String, Long> {
  private static final Logger LOG = Logger.getLogger(StatefulNumberSpout.class.getName());
  private static final long serialVersionUID = 5454291010750852782L;
  private SpoutOutputCollector collector;
  private Random rand;
  private long msgId;
  private State<String, Long> state;

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("value", "ts", "msgid"));
  }

  @Override
  public void open(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector
      collector) {
    this.collector = collector;
    this.rand = new Random();
  }

  @Override
  public void nextTuple() {
    Utils.sleep(1000);
    long val = msgId;
    long randomNumber = System.currentTimeMillis() - (24 * 60 * 60 * 1000);
    System.out.println("Emitting: " + val);
    collector.emit(new Values(val,
        randomNumber, msgId), msgId);
    msgId++;
  }

  @Override
  public void ack(Object msgId) {
    LOG.fine("Got ACK for msgId : " + msgId);
  }

  @Override
  public void fail(Object msgId) {
    LOG.fine("Got FAIL for msgId : " + msgId);
  }

  @Override
  public void initState(State<String, Long> state) {
    this.state = state;
    this.msgId = this.state.getOrDefault("msgId", 0L);
  }

  @Override
  public void preSave(String checkpointId) {
    this.state.put("msgId", msgId);
  }
}
