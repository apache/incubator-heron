// Copyright 2016 Twitter. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.twitter.heron.resource;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Ignore;

import com.twitter.heron.api.bolt.BaseRichBolt;
import com.twitter.heron.api.bolt.OutputCollector;
import com.twitter.heron.api.topology.OutputFieldsDeclarer;
import com.twitter.heron.api.topology.TopologyContext;
import com.twitter.heron.api.tuple.Fields;
import com.twitter.heron.api.tuple.Tuple;
import com.twitter.heron.api.tuple.Values;
import com.twitter.heron.common.basics.SingletonRegistry;

/**
 * A Bolt used for unit test, it will execute and modify some singletons' value:
 * 1. It will tuple.getString(0) and append to the singleton "received-string-list"
 * 2. It will increment singleton "execute-count" when it executes once
 * 3. It will ack the tuple and increment singleton Constants.ACK_COUNT when # of tuples executed is odd
 * 4. It will fail the tuple and increment singleton Constants.FAIL_COUNT when # of tuples executed is even
 */
@Ignore
public class TestBolt extends BaseRichBolt {
  private static final long serialVersionUID = -5160420613503624743L;
  private OutputCollector outputCollector;
  private int tupleExecuted = 0;

  @Override
  public void prepare(
      Map<String, Object> map,
      TopologyContext topologyContext,
      OutputCollector collector) {
    this.outputCollector = collector;
  }

  @Override
  public void execute(Tuple tuple) {
    AtomicInteger ackCount =
        (AtomicInteger) SingletonRegistry.INSTANCE.getSingleton(Constants.ACK_COUNT);
    AtomicInteger failCount =
        (AtomicInteger) SingletonRegistry.INSTANCE.getSingleton(Constants.FAIL_COUNT);
    AtomicInteger tupleExecutedCount =
        (AtomicInteger) SingletonRegistry.INSTANCE.getSingleton("execute-count");
    StringBuilder receivedStrings =
        (StringBuilder) SingletonRegistry.INSTANCE.getSingleton("received-string-list");

    if (receivedStrings != null) {
      receivedStrings.append(tuple.getString(0));
    }

    if (tupleExecutedCount != null) {
      tupleExecutedCount.getAndIncrement();
    }
    if ((tupleExecuted & 1) == 0) {
      outputCollector.ack(tuple);
      if (ackCount != null) {
        ackCount.getAndIncrement();
      }
    } else {
      outputCollector.fail(tuple);
      if (failCount != null) {
        failCount.getAndIncrement();
      }
    }
    tupleExecuted++;
    outputCollector.emit(new Values(tuple.getString(0)));
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    outputFieldsDeclarer.declare(new Fields("word"));
  }
}
