package org.apache.storm.topology;

import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

public class OutputFieldsGetter implements OutputFieldsDeclarer {
  private com.twitter.heron.api.topology.OutputFieldsDeclarer delegate;

  public OutputFieldsGetter(com.twitter.heron.api.topology.OutputFieldsDeclarer delegate) {
    this.delegate = delegate;
  }

  public void declare(Fields fields) {
    declare(false, fields);
  }

  public void declare(boolean direct, Fields fields) {
    declareStream(Utils.DEFAULT_STREAM_ID, direct, fields);
  }

  public void declareStream(String streamId, Fields fields) {
    declareStream(streamId, false, fields);
  }

  public void declareStream(String streamId, boolean direct, Fields fields) {
    delegate.declareStream(streamId, direct, fields.getDelegate());
  }
}
