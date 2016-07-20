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

import org.apache.storm.spout.SpoutOutputCollectorImpl;
import org.apache.storm.task.TopologyContext;

import com.twitter.heron.api.spout.SpoutOutputCollector;

/**
 * When writing topologies using Java, {@link IRichBolt} and {@link IRichSpout} are the main interfaces
 * to use to implement components of the topology.
 */
public class IRichSpoutDelegate implements com.twitter.heron.api.spout.IRichSpout {
  private static final long serialVersionUID = -1543996045558101339L;
  private IRichSpout delegate;
  private TopologyContext topologyContextImpl;
  private SpoutOutputCollectorImpl spoutOutputCollectorImpl;

  public IRichSpoutDelegate(IRichSpout delegate) {
    this.delegate = delegate;
  }

  @Override
  public void open(Map<String, Object> conf, com.twitter.heron.api.topology.TopologyContext context,
                   SpoutOutputCollector collector) {
    topologyContextImpl = new TopologyContext(context);
    spoutOutputCollectorImpl = new SpoutOutputCollectorImpl(collector);
    delegate.open(conf, topologyContextImpl, spoutOutputCollectorImpl);
  }

  @Override
  public void close() {
    delegate.close();
  }

  @Override
  public void activate() {
    delegate.activate();
  }

  @Override
  public void deactivate() {
    delegate.deactivate();
  }

  @Override
  public void nextTuple() {
    delegate.nextTuple();
  }

  @Override
  public void ack(Object msgId) {
    delegate.ack(msgId);
  }

  @Override
  public void fail(Object msgId) {
    delegate.fail(msgId);
  }

  @Override
  public void declareOutputFields(com.twitter.heron.api.topology.OutputFieldsDeclarer declarer) {
    OutputFieldsGetter getter = new OutputFieldsGetter(declarer);
    delegate.declareOutputFields(getter);
  }

  @Override
  public Map<String, Object> getComponentConfiguration() {
    return delegate.getComponentConfiguration();
  }
}
