package org.apache.storm.hooks.info;

import org.apache.storm.tuple.Tuple;

public class BoltExecuteInfo {
  public Tuple tuple;
  public int executingTaskId;
  public Long executeLatencyMs; // null if it wasn't sampled
  
  public BoltExecuteInfo(Tuple tuple, int executingTaskId, Long executeLatencyMs) {
    this.tuple = tuple;
    this.executingTaskId = executingTaskId;
    this.executeLatencyMs = executeLatencyMs;
  }

  public BoltExecuteInfo(com.twitter.heron.api.hooks.info.BoltExecuteInfo info) {
    this.tuple = new org.apache.storm.tuple.TupleImpl(info.getTuple());
    this.executingTaskId = info.getExecutingTaskId();
    this.executeLatencyMs = info.getExecuteLatencyMs();
  }
}
