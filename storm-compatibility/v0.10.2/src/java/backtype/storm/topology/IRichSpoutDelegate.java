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

package backtype.storm.topology;

import java.util.Map;
import java.util.logging.Logger;

import org.apache.heron.api.spout.SpoutOutputCollector;
import org.apache.heron.api.topology.IUpdatable;

import backtype.storm.spout.SpoutOutputCollectorImpl;
import backtype.storm.task.TopologyContext;
import backtype.storm.utils.ConfigUtils;

/**
 * When writing topologies using Java, {@link IRichBolt} and {@link IRichSpout} are the main interfaces
 * to use to implement components of the topology.
 */
public class IRichSpoutDelegate implements org.apache.heron.api.spout.IRichSpout, IUpdatable {
  private static final Logger LOG = Logger.getLogger(IRichSpoutDelegate.class.getName());

  private static final long serialVersionUID = -4310232227720592316L;
  private IRichSpout delegate;
  private TopologyContext topologyContextImpl;
  private SpoutOutputCollectorImpl spoutOutputCollectorImpl;

  public IRichSpoutDelegate(IRichSpout delegate) {
    this.delegate = delegate;
  }

  @Override
  @SuppressWarnings("rawtypes")
  public void open(Map conf, org.apache.heron.api.topology.TopologyContext context,
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
  public void declareOutputFields(org.apache.heron.api.topology.OutputFieldsDeclarer declarer) {
    OutputFieldsGetter getter = new OutputFieldsGetter(declarer);
    delegate.declareOutputFields(getter);
  }

  @Override
  public Map<String, Object> getComponentConfiguration() {
    Map<String, Object> config = delegate.getComponentConfiguration();
    return ConfigUtils.translateComponentConfig(config);
  }

  @Override
  public void update(org.apache.heron.api.topology.TopologyContext topologyContext) {
    if (delegate instanceof IUpdatable) {
      ((IUpdatable) delegate).update(topologyContext);
    } else {
      LOG.warning(String.format("Update() event received but can not call update() on delegate "
          + "because it does not implement %s: %s", IUpdatable.class.getName(), delegate));
    }
  }
}
