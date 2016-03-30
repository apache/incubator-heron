package org.apache.storm.hooks.info;

import org.apache.storm.tuple.Tuple;

public class BoltAckInfo {
  public Tuple tuple;
  public int ackingTaskId;
  public Long processLatencyMs; // null if it wasn't sampled
  
  public BoltAckInfo(Tuple tuple, int ackingTaskId, Long processLatencyMs) {
    this.tuple = tuple;
    this.ackingTaskId = ackingTaskId;
    this.processLatencyMs = processLatencyMs;
  }

  public BoltAckInfo(com.twitter.heron.api.hooks.info.BoltAckInfo info) {
    this.tuple = new org.apache.storm.tuple.TupleImpl(info.getTuple());
    this.ackingTaskId = info.getAckingTaskId();
    this.processLatencyMs = info.getProcessLatencyMs();
  }
}
