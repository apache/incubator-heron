package org.apache.storm.task;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.TupleImpl;

/**
 * This output collector exposes the API for emitting tuples from an IRichBolt.
 * This is the core API for emitting tuples. For a simpler API, and a more restricted
 * form of stream processing, see IBasicBolt and BasicOutputCollector.
 */
public class OutputCollectorImpl extends OutputCollector {
  private com.twitter.heron.api.bolt.OutputCollector delegate;
    
  public OutputCollectorImpl(com.twitter.heron.api.bolt.OutputCollector delegate) {
    super(null);
    this.delegate = delegate;
  }
    
  @Override
  public List<Integer> emit(String streamId, Collection<Tuple> anchors, List<Object> tuple) {
    if (anchors != null) {
      ArrayList<com.twitter.heron.api.tuple.Tuple> l = new ArrayList<com.twitter.heron.api.tuple.Tuple>();
      for (Tuple t : anchors) {
        TupleImpl i = (TupleImpl)t;
        l.add(i.getDelegate());
      }
      return delegate.emit(streamId, l, tuple);
    } else {
      return delegate.emit(streamId, (Collection<com.twitter.heron.api.tuple.Tuple>) null, tuple);
    }
  }

  @Override
  public void emitDirect(int taskId, String streamId, Collection<Tuple> anchors, List<Object> tuple) {
    if (anchors != null) {
      ArrayList<com.twitter.heron.api.tuple.Tuple> l = new ArrayList<com.twitter.heron.api.tuple.Tuple>();
      for (Tuple t : anchors) {
        TupleImpl i = (TupleImpl)t;
        l.add(i.getDelegate());
      }
      delegate.emitDirect(taskId, streamId, l, tuple);
    } else {
      delegate.emitDirect(taskId, streamId, (Collection<com.twitter.heron.api.tuple.Tuple>) null, tuple);
    }
  }

  @Override
  public void ack(Tuple input) {
    TupleImpl i = (TupleImpl)input;
    delegate.ack(i.getDelegate());
  }

  @Override
  public void fail(Tuple input) {
    TupleImpl i = (TupleImpl)input;
    delegate.fail(i.getDelegate());
  }

  @Override
  public void reportError(Throwable error) {
    delegate.reportError(error);
  }
}
