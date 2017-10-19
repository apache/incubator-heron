//  Copyright 2017 Twitter. All rights reserved.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

package com.twitter.heron.streamlet.impl.sinks;

import java.util.Map;
import java.util.logging.Logger;

import com.twitter.heron.api.bolt.OutputCollector;
import com.twitter.heron.api.topology.TopologyContext;
import com.twitter.heron.api.tuple.Tuple;
import com.twitter.heron.streamlet.SerializableConsumer;
import com.twitter.heron.streamlet.impl.operators.StreamletOperator;

/**
 * ConsumerSink is a very simple Sink that basically invokes a user supplied
 * consume function for every tuple.
 */
public class ConsumerSink<R> extends StreamletOperator {
  private static final Logger LOG = Logger.getLogger(ConsumerSink.class.getName());
  private static final long serialVersionUID = 8716140142187667638L;
  private SerializableConsumer<R> consumer;
  private OutputCollector collector;

  public ConsumerSink(SerializableConsumer<R> consumer) {
    this.consumer = consumer;
  }

  @SuppressWarnings("rawtypes")
  @Override
  public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
    this.collector = outputCollector;
  }

  @SuppressWarnings("unchecked")
  @Override
  public void execute(Tuple tuple) {
    R obj = (R) tuple.getValue(0);
    consumer.accept(obj);
    collector.ack(tuple);
  }
}
