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

import org.apache.heron.api.topology.IUpdatable;

import backtype.storm.task.OutputCollectorImpl;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.TupleImpl;
import backtype.storm.utils.ConfigUtils;

/**
 * When writing topologies using Java, {@link IRichBolt} and {@link IRichSpout} are the main interfaces
 * to use to implement components of the topology.
 */
public class IRichBoltDelegate implements org.apache.heron.api.bolt.IRichBolt, IUpdatable {
  private static final Logger LOG = Logger.getLogger(IRichBoltDelegate.class.getName());

  private static final long serialVersionUID = -3717575342431064148L;
  private IRichBolt delegate;
  private TopologyContext topologyContextImpl;
  private OutputCollectorImpl outputCollectorImpl;

  public IRichBoltDelegate(IRichBolt delegate) {
    this.delegate = delegate;
  }

  @Override
  @SuppressWarnings("rawtypes")
  public void prepare(
      Map conf,
      org.apache.heron.api.topology.TopologyContext context,
      org.apache.heron.api.bolt.OutputCollector collector) {
    topologyContextImpl = new TopologyContext(context);
    outputCollectorImpl = new OutputCollectorImpl(collector);
    delegate.prepare(conf, topologyContextImpl, outputCollectorImpl);
  }

  @Override
  public void cleanup() {
    delegate.cleanup();
  }

  @Override
  public void execute(org.apache.heron.api.tuple.Tuple tuple) {
    TupleImpl impl = new TupleImpl(tuple);
    delegate.execute(impl);
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
