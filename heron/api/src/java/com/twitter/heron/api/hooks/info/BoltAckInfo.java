package com.twitter.heron.api.hooks.info;

import com.twitter.heron.api.tuple.Tuple;

public class BoltAckInfo {
  private Tuple tuple;
  private int ackingTaskId;
  private Long processLatencyMs; // null if it wasn't sampled
    
  public BoltAckInfo(Tuple tuple, int ackingTaskId, Long processLatencyMs) {
    this.tuple = tuple;
    this.ackingTaskId = ackingTaskId;
    this.processLatencyMs = processLatencyMs;
  }

  public Tuple getTuple() {
    return tuple;
  }

  public int getAckingTaskId() {
    return ackingTaskId;
  }

  public Long getProcessLatencyMs() {
    return processLatencyMs;
  }
}
