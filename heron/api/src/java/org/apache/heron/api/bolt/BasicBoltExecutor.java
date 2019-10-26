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

package org.apache.heron.api.bolt;

import java.util.Map;
import java.util.logging.Logger;

import org.apache.heron.api.exception.FailedException;
import org.apache.heron.api.exception.ReportedFailedException;
import org.apache.heron.api.topology.IUpdatable;
import org.apache.heron.api.topology.OutputFieldsDeclarer;
import org.apache.heron.api.topology.TopologyContext;
import org.apache.heron.api.tuple.Tuple;

public class BasicBoltExecutor implements IRichBolt, IUpdatable {
  private static final Logger LOG = Logger.getLogger(BasicBoltExecutor.class.getName());

  private static final long serialVersionUID = 7021447981762957626L;

  private IBasicBolt bolt;
  private transient BasicOutputCollector collector;

  public BasicBoltExecutor(IBasicBolt aBolt) {
    bolt = aBolt;
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    bolt.declareOutputFields(declarer);
  }


  @Override
  public void prepare(
      Map<String, Object> heronConf,
      TopologyContext context,
      OutputCollector aCollector) {
    bolt.prepare(heronConf, context);
    collector = new BasicOutputCollector(aCollector);
  }

  @Override
  public void execute(Tuple input) {
    collector.setContext(input);
    try {
      bolt.execute(input, collector);
      collector.getOutputter().ack(input);
    } catch (FailedException e) {
      if (e instanceof ReportedFailedException) {
        collector.reportError(e);
      }
      collector.getOutputter().fail(input);
    }
  }

  @Override
  public void cleanup() {
    bolt.cleanup();
  }

  @Override
  public Map<String, Object> getComponentConfiguration() {
    return bolt.getComponentConfiguration();
  }

  @Override
  public void update(org.apache.heron.api.topology.TopologyContext topologyContext) {
    if (bolt instanceof IUpdatable) {
      ((IUpdatable) bolt).update(topologyContext);
    } else {
      LOG.warning(String.format("Update() event received but can not call update() on delegate "
          + "because it does not implement %s: %s", IUpdatable.class.getName(), bolt));
    }
  }
}
