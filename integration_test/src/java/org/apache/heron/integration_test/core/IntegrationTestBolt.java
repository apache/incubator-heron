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

package org.apache.heron.integration_test.core;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import org.apache.heron.api.bolt.IOutputCollector;
import org.apache.heron.api.bolt.IRichBolt;
import org.apache.heron.api.bolt.OutputCollector;
import org.apache.heron.api.generated.TopologyAPI;
import org.apache.heron.api.topology.IUpdatable;
import org.apache.heron.api.topology.OutputFieldsDeclarer;
import org.apache.heron.api.topology.TopologyContext;
import org.apache.heron.api.tuple.Fields;
import org.apache.heron.api.tuple.Tuple;
import org.apache.heron.api.tuple.Values;

public class IntegrationTestBolt implements IRichBolt, IUpdatable {
  private static final long serialVersionUID = 6304554167838679097L;
  private static final Logger LOG = Logger.getLogger(IntegrationTestBolt.class.getName());
  private final IRichBolt delegateBolt;
  private int terminalsToReceive = 0;
  private long tuplesReceived = 0;
  private long tuplesProcessed = 0;
  // For ack/fail
  private Tuple currentTupleProcessing = null;
  private OutputCollector collector;

  //whether automatically acking should be done
  private boolean ackAuto;

  public IntegrationTestBolt(IRichBolt delegate, boolean ackAuto) {
    this.delegateBolt = delegate;
    this.ackAuto = ackAuto;
  }

  @Override
  public void update(TopologyContext topologyContext) {
    LOG.info("update called with TopologyContext: " + topologyContext);
    // if we get a new topology context we reset the terminalsToReceive regardless of if we've
    // already received any. The expectation is that after a change in physical plan, upstream
    // spouts will re-emit and send new terminals.
    this.terminalsToReceive = calculateTerminalsToReceive(topologyContext);
  }

  @Override
  public void prepare(Map<String, Object> map,
                      TopologyContext context,
                      OutputCollector outputCollector) {
    update(context);
    this.collector = new OutputCollector(new IntegrationTestBoltCollector(outputCollector));
    this.delegateBolt.prepare(map, new TestTopologyContext(context), collector);
  }

  private int calculateTerminalsToReceive(TopologyContext context) {
    int total = 0;
    // Set the # of terminal Signal to receive, = the # number all instance of upstream components
    HashSet<String> upstreamComponents = new HashSet<String>();
    for (TopologyAPI.StreamId streamId : context.getThisSources().keySet()) {
      upstreamComponents.add(streamId.getComponentName());
    }
    for (String name : upstreamComponents) {
      total += context.getComponentTasks(name).size();
    }

    LOG.info("TerminalsToReceive: " + total);
    return total;
  }

  @Override
  public void execute(Tuple tuple) {
    String streamID = tuple.getSourceStreamId();

    LOG.info("Received a tuple: " + tuple + " ; from: " + streamID);

    if (streamID.equals(Constants.INTEGRATION_TEST_CONTROL_STREAM_ID)) {
      terminalsToReceive--;
      if (terminalsToReceive == 0) {

        // invoke the finishBatch() callback if necessary
        if (IBatchBolt.class.isInstance(delegateBolt)) {
          LOG.fine("Invoke bolt to do finishBatch!");
          ((IBatchBolt) delegateBolt).finishBatch();
        }

        LOG.info("Received the last terminal, populating the terminals to downstream");
        collector.emit(Constants.INTEGRATION_TEST_CONTROL_STREAM_ID,
            tuple,
            new Values(Constants.INTEGRATION_TEST_TERMINAL));
      } else {
        LOG.info(String.format(
            "Received a terminal, need to receive %s more", terminalsToReceive));
      }
    } else {
      tuplesReceived++;
      currentTupleProcessing = tuple;
      delegateBolt.execute(tuple);
      // We ack only the tuples in user's logic
      if (this.ackAuto) {
        collector.ack(tuple);
      }
    }
  }

  @Override
  public void cleanup() {
    delegateBolt.cleanup();
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    outputFieldsDeclarer.declareStream(Constants.INTEGRATION_TEST_CONTROL_STREAM_ID,
        new Fields(Constants.INTEGRATION_TEST_TERMINAL));
    delegateBolt.declareOutputFields(outputFieldsDeclarer);
  }

  @Override
  public Map<String, Object> getComponentConfiguration() {
    return delegateBolt.getComponentConfiguration();
  }

  private class IntegrationTestBoltCollector implements IOutputCollector {
    private final IOutputCollector delegate;

    IntegrationTestBoltCollector(IOutputCollector delegate) {
      this.delegate = delegate;
    }

    @Override
    public List<Integer> emit(String s, Collection<Tuple> tuples, List<Object> objects) {
      if (tuples == null) {
        return delegate.emit(s, Arrays.asList(currentTupleProcessing), objects);
      }
      return delegate.emit(s, tuples, objects);
    }

    @Override
    public void emitDirect(int i, String s, Collection<Tuple> tuples, List<Object> objects) {
      if (tuples == null) {
        delegate.emitDirect(i, s, Arrays.asList(currentTupleProcessing), objects);
      }
      delegate.emitDirect(i, s, tuples, objects);
    }

    @Override
    public void ack(Tuple tuple) {
      LOG.fine("Try to do a ack. tuplesProcessed: "
          + tuplesProcessed + " ; tuplesReceived: " + tuplesReceived);
      if (tuplesProcessed < tuplesReceived) {
        delegate.ack(tuple);
        tuplesProcessed++;
      }
    }

    @Override
    public void fail(Tuple tuple) {
      LOG.fine("Try to do a fail. tuplesProcessed: "
          + tuplesProcessed + " ; tuplesReceived: " + tuplesReceived);
      if (tuplesProcessed < tuplesReceived) {
        delegate.fail(tuple);
        tuplesProcessed++;
      }
    }

    @Override
    public void reportError(Throwable throwable) {
      delegate.reportError(throwable);
    }
  }
}
