/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.storm.topology;

import java.util.Map;

import org.apache.storm.task.OutputCollectorImpl;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.TupleImpl;
import org.apache.storm.windowing.TupleWindowImpl;

public class IWindowedBoltDelegate implements com.twitter.heron.api.bolt.IWindowedBolt {

  private static final long serialVersionUID = 8753943395082633132L;
  private final IWindowedBolt delegate;
  private TopologyContext topologyContextImpl;
  private OutputCollectorImpl outputCollectorImpl;

  public IWindowedBoltDelegate(IWindowedBolt iWindowedBolt) {
    this.delegate = iWindowedBolt;
  }

  @Override
  public void declareOutputFields(com.twitter.heron.api.topology.OutputFieldsDeclarer declarer) {
    OutputFieldsGetter getter = new OutputFieldsGetter(declarer);
    delegate.declareOutputFields(getter);
  }

  @Override
  public Map<String, Object> getComponentConfiguration() {
    return this.delegate.getComponentConfiguration();
  }

  @Override
  public void prepare(
      Map<String, Object> conf,
      com.twitter.heron.api.topology.TopologyContext context,
      com.twitter.heron.api.bolt.OutputCollector collector) {
    topologyContextImpl = new TopologyContext(context);
    outputCollectorImpl = new OutputCollectorImpl(collector);
    delegate.prepare(conf, topologyContextImpl, outputCollectorImpl);
  }

  @Override
  public void execute(com.twitter.heron.api.windowing.TupleWindow inputWindow) {
    this.delegate.execute(new TupleWindowImpl(inputWindow));
  }

  @Override
  public void cleanup() {
    this.delegate.cleanup();
  }

  @Override
  public com.twitter.heron.api.windowing.TimestampExtractor getTimestampExtractor() {

    return (this.delegate.getTimestampExtractor() == null) ? null
        : new com.twitter.heron.api.windowing.TimestampExtractor() {
          @Override
          public long extractTimestamp(com.twitter.heron.api.tuple.Tuple tuple) {
            return delegate.getTimestampExtractor().extractTimestamp(new TupleImpl(tuple));
          }
        };
  }
}
