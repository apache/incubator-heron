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

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;

public class BasicBoltExecutor implements IRichBolt, IUpdatable {
  private static final Logger LOG = Logger.getLogger(BasicBoltExecutor.class.getName());

  private static final long serialVersionUID = 4359767045622072660L;
  private IBasicBolt delegate;
  private transient BasicOutputCollector collector;

  public BasicBoltExecutor(IBasicBolt bolt) {
    this.delegate = bolt;
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    delegate.declareOutputFields(declarer);
  }

  @Override
  @SuppressWarnings("rawtypes")
  public void prepare(Map stormConf,
                      TopologyContext context,
                      OutputCollector newCollector) {
    delegate.prepare(stormConf, context);
    this.collector = new BasicOutputCollector(newCollector);
  }

  @Override
  public void execute(Tuple input) {
    this.collector.setContext(input);
    try {
      delegate.execute(input, collector);
      this.collector.getOutputter().ack(input);
    } catch (FailedException e) {
      if (e instanceof ReportedFailedException) {
        this.collector.reportError(e);
      }
      this.collector.getOutputter().fail(input);
    }
  }

  @Override
  public void cleanup() {
    delegate.cleanup();
  }

  @Override
  public Map<String, Object> getComponentConfiguration() {
    return delegate.getComponentConfiguration();
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
