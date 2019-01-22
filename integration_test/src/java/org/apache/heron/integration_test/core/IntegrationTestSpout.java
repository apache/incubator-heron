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

import java.io.IOException;
import java.text.ParseException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.heron.api.spout.IRichSpout;
import org.apache.heron.api.spout.ISpoutOutputCollector;
import org.apache.heron.api.spout.SpoutOutputCollector;
import org.apache.heron.api.topology.OutputFieldsDeclarer;
import org.apache.heron.api.topology.TopologyContext;
import org.apache.heron.api.tuple.Fields;
import org.apache.heron.api.tuple.Values;

public class IntegrationTestSpout implements IRichSpout {
  private static final long serialVersionUID = 6068686695658877942L;
  private static final Logger LOG = Logger.getLogger(IntegrationTestSpout.class.getName());
  static final Values TERMINAL_TUPLE = new Values(Constants.INTEGRATION_TEST_TERMINAL);

  private final IRichSpout delegateSpout;
  private final String topologyStartedStateUrl;
  private long tuplesToAck = 0;
  private SpoutOutputCollector spoutOutputCollector;
  private boolean hasSetStarted = false;
  private int maxExecutions;
  private Map<Object, List<Object>> pendingMessages;

  public IntegrationTestSpout(IRichSpout delegateSpout,
                              int maxExecutions,
                              String topologyStartedStateUrl) {
    assert maxExecutions > 0;
    this.delegateSpout = delegateSpout;
    this.maxExecutions = maxExecutions;
    this.topologyStartedStateUrl = topologyStartedStateUrl;
    this.pendingMessages = new HashMap<>();
  }

  protected void resetMaxExecutions(int resetExecutions) {
    if (!doneEmitting() || !doneAcking()) {
      throw new RuntimeException(
          "Can not reset resetMaxExecutions while tuples are still bing emitted or acked");
    }
    this.maxExecutions = resetExecutions;
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    outputFieldsDeclarer.declareStream(Constants.INTEGRATION_TEST_CONTROL_STREAM_ID,
        new Fields(Constants.INTEGRATION_TEST_TERMINAL));
    delegateSpout.declareOutputFields(outputFieldsDeclarer);
  }

  @Override
  public Map<String, Object> getComponentConfiguration() {
    return delegateSpout.getComponentConfiguration();
  }

  @Override
  public void close() {
    delegateSpout.close();
  }

  @Override
  public void activate() {
    delegateSpout.activate();
  }

  @Override
  public void deactivate() {
    delegateSpout.deactivate();
  }

  @Override
  public void open(Map<String, Object> map,
                   TopologyContext topologyContext,
                   SpoutOutputCollector outputCollector) {
    // Here the spoutOutputCollector should be a default one
    // to emit tuples without adding MessageId
    this.spoutOutputCollector = outputCollector;
    delegateSpout.open(map, new TestTopologyContext(topologyContext),
        new SpoutOutputCollector(new IntegrationTestSpoutCollector(outputCollector)));
  }

  @Override
  public void nextTuple() {
    if (doneEmitting()) {
      return;
    } else if (!this.hasSetStarted) {
      setStateToStarted();
      this.hasSetStarted = true;
    }
    maxExecutions--;

    LOG.fine("maxExecutions = " + maxExecutions);

    delegateSpout.nextTuple();
    // We need a double check here rather than set the isDone == true in nextTuple()
    // Since it is possible before nextTuple we get all the acks and the topology is done
    // However, since the isDone is not set to true, we may not emit terminals; it will cause bug.
    if (doneEmitting()) {
      // This is needed if all the tuples have been emitted and acked
      // before maxExecutions becomes 0
      emitTerminalIfNeeded();
      LOG.fine("The topology is done.");
    } else {
      if (getPostEmitSleepTime() > 0) {
        try {
          getPostEmitSleepTimeUnit().sleep(getPostEmitSleepTime());
        } catch (InterruptedException e) {
          LOG.log(Level.SEVERE, "Thread interrupted while trying to sleep post-emit", e);
        }
      }
    }
  }

  @Override
  public void ack(Object messageId) {
    tuplesToAck--;
    LOG.info("Received an ack with MessageId: " + messageId + " tuplesToAck=" + tuplesToAck);

    if (!isTestMessageId(messageId)) {
      delegateSpout.ack(messageId);
    } else {
      handleAckedMessage(messageId, pendingMessages.get(messageId));
    }
    emitTerminalIfNeeded();
  }

  protected void handleAckedMessage(Object messageId, List<Object> tuple) {
    pendingMessages.remove(messageId);
  }

  @Override
  public void fail(Object messageId) {
    LOG.info("Received a fail with MessageId: " + messageId);

    tuplesToAck--;
    if (!isTestMessageId(messageId)) {
      delegateSpout.fail(messageId);
    } else {
      if (pendingMessages.containsKey(messageId)) {
        LOG.info("Re-emitting failed tuple with messageId " + messageId
            + ", tuple: " + pendingMessages.get(messageId));
        spoutOutputCollector.emit(pendingMessages.get(messageId), messageId);
      }
    }
    emitTerminalIfNeeded();
  }

  private static boolean isTestMessageId(Object messageId) {
    return  (messageId instanceof String) && ((String) messageId)
        .startsWith(Constants.INTEGRATION_TEST_MOCK_MESSAGE_ID);
  }

  protected boolean doneEmitting() {
    return maxExecutions <= 0;
  }

  protected boolean doneAcking() {
    return tuplesToAck == 0;
  }

  protected TimeUnit getPostEmitSleepTimeUnit() {
    return TimeUnit.MILLISECONDS;
  }

  protected long getPostEmitSleepTime() {
    return 0;
  }

  protected void emitTerminalIfNeeded() {
    LOG.fine(String.format("doneEmitting = %s, tuplesToAck = %s", doneEmitting(), tuplesToAck));

    if (doneEmitting() && doneAcking()) {
      LOG.info("Emitting terminals to downstream.");
      spoutOutputCollector.emit(Constants.INTEGRATION_TEST_CONTROL_STREAM_ID, TERMINAL_TUPLE);
    }
    // Else, do nothing
  }

  protected class IntegrationTestSpoutCollector implements ISpoutOutputCollector {
    private final ISpoutOutputCollector delegate;

    IntegrationTestSpoutCollector(ISpoutOutputCollector delegate) {
      this.delegate = delegate;
    }

    @Override
    public List<Integer> emit(String streamId, List<Object> tuple, Object messageId) {
      tuplesToAck++;
      LOG.info("Emitting tuple: " + tuple + ", tuplesToAck=" + tuplesToAck);
      return delegate.emit(streamId, tuple, getMessageId(tuple, messageId));
    }

    @Override
    public void emitDirect(int taskId, String streamId, List<Object> tuple, Object messageId) {
      tuplesToAck++;
      LOG.info("Emitting tuple: " + tuple + ", tuplesToAck=" + tuplesToAck);
      delegate.emitDirect(taskId, streamId, tuple, getMessageId(tuple, messageId));
    }

    @Override
    public void reportError(Throwable throwable) {
      delegate.reportError(throwable);
    }

    private Object getMessageId(List<Object> tuple, Object messageId) {
      Object id = messageId;
      if (id == null) {
        LOG.fine("Add MessageId for tuple: " + tuple);
        id = Constants.INTEGRATION_TEST_MOCK_MESSAGE_ID + "_" + UUID.randomUUID();
      }
      pendingMessages.put(id, tuple);
      return id;
    }
  }

  private void setStateToStarted() {
    if (topologyStartedStateUrl == null) {
      return;
    }
    try {
      HttpUtils.httpJsonPost(topologyStartedStateUrl, "\"true\"");
    } catch (IOException | ParseException e) {
      throw new RuntimeException(
          "Failure posting topology_started state to " + topologyStartedStateUrl, e);
    }
  }
}
