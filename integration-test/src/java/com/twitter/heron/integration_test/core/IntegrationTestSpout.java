package com.twitter.heron.integration_test.core;

import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import com.twitter.heron.api.spout.IRichSpout;
import com.twitter.heron.api.spout.ISpoutOutputCollector;
import com.twitter.heron.api.spout.SpoutOutputCollector;
import com.twitter.heron.api.topology.OutputFieldsDeclarer;
import com.twitter.heron.api.topology.TopologyContext;
import com.twitter.heron.api.tuple.Fields;
import com.twitter.heron.api.tuple.Values;

public class IntegrationTestSpout implements IRichSpout {
  private boolean isDone = false;
  private long tuplesToComplete = 0;


  // The times of execution
  private static final int DEFAULT_EXECUTION_COUNT = 10;

  private static final Logger LOG = Logger.getLogger(IntegrationTestSpout.class.getName());

  private final IRichSpout delegateSpout;
  private SpoutOutputCollector spoutOutputCollector;

  private int maxExecutions;

  public IntegrationTestSpout(IRichSpout delegateSpout) {
    this(delegateSpout, DEFAULT_EXECUTION_COUNT);
  }

  public IntegrationTestSpout(IRichSpout delegateSpout, int maxExecutions) {
    assert maxExecutions > 0;
    this.delegateSpout = delegateSpout;
    this.maxExecutions = maxExecutions;
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
  public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector outputCollector) {
    // Here the spoutOutputCollector should be a default one
    // to emit tuples without adding MessageId
    this.spoutOutputCollector = outputCollector;
    delegateSpout.open(map, topologyContext,
        new SpoutOutputCollector(new IntegrationTestSpoutCollector(outputCollector)));

  }

  @Override
  public void nextTuple() {
    if (maxExecutions <= 0) {
      return;
    }
    maxExecutions--;

    LOG.fine("maxExecutions = " + maxExecutions);

    delegateSpout.nextTuple();
    // We need a double check here rather than set the isDone == true in nextTuple()
    // Since it is possible before nextTuple we get all the acks and the topology is done
    // However, since the isDone is not set to true, we may not emit terminals; it will cause bug.
    if (maxExecutions <= 0) {
      isDone = true;
      // This is needed if all the tuples have been emited and acked
      // before maxExecutions becomes 0
      emitTerminalIfNeeded();
      LOG.fine("The topology is done.");
    }
  }

  @Override
  public void ack(Object o) {
    LOG.fine("Received a ack with MessageId: " + o);

    tuplesToComplete--;
    if (!o.equals(Constants.INTEGRATION_TEST_MOCK_MESSAGE_ID)) {
      delegateSpout.ack(o);
    }
    emitTerminalIfNeeded();
  }

  @Override
  public void fail(Object o) {
    LOG.fine("Received a fail with MessageId: " + o);

    tuplesToComplete--;
    if (!o.equals(Constants.INTEGRATION_TEST_MOCK_MESSAGE_ID)) {
      delegateSpout.fail(o);
    }
    emitTerminalIfNeeded();
  }

  private void emitTerminalIfNeeded() {
    LOG.fine(String.format("isDone = %s, tuplesToComplete = %s", isDone, tuplesToComplete));

    if (isDone && tuplesToComplete == 0) {
      LOG.info("Emitting terminals to downstream.");
      spoutOutputCollector.emit(Constants.INTEGRATION_TEST_CONTROL_STREAM_ID,
          new Values(Constants.INTEGRATION_TEST_TERMINAL));
    }
    // Else, do nothing
  }

  private class IntegrationTestSpoutCollector implements ISpoutOutputCollector {
    private final ISpoutOutputCollector delegate;

    public IntegrationTestSpoutCollector(ISpoutOutputCollector delegate) {
      this.delegate = delegate;
    }

    @Override
    public List<Integer> emit(String s, List<Object> objects, Object o) {
      tuplesToComplete++;
      Object messageId = o;
      if (o == null) {
        LOG.fine("Add MessageId for tuple: " + objects);
        messageId = Constants.INTEGRATION_TEST_MOCK_MESSAGE_ID;
      }

      return delegate.emit(s, objects, messageId);
    }

    @Override
    public void emitDirect(int i, String s, List<Object> objects, Object o) {
      tuplesToComplete++;
      Object messageId = o;
      if (o == null) {
        messageId = Constants.INTEGRATION_TEST_MOCK_MESSAGE_ID;
      }

      delegate.emitDirect(i, s, objects, messageId);
    }

    @Override
    public void reportError(Throwable throwable) {
      delegate.reportError(throwable);
    }
  }
}
