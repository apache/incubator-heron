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

package com.twitter.heron.dsl.impl.sinks;

import java.util.Map;
import java.util.logging.Logger;

import com.twitter.heron.api.bolt.OutputCollector;
import com.twitter.heron.api.topology.TopologyContext;
import com.twitter.heron.api.tuple.Tuple;
import com.twitter.heron.dsl.impl.operators.DslOperator;

/**
 * LogSink is a very simple Bolt that implements the log functionality.
 * It basically logs every tuple.
 */
public class LogSink<R> extends DslOperator {
  private static final long serialVersionUID = -6392422646613189818L;
  private static final Logger LOG = Logger.getLogger(LogSink.class.getName());
  private OutputCollector collector;

  public LogSink() {
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
    LOG.info(String.valueOf(obj));
    collector.ack(tuple);
  }
}
