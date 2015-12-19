package com.twitter.heron.api.hooks.info;

import com.twitter.heron.api.tuple.Tuple;

public class BoltExecuteInfo {
  private Tuple tuple;
  private int executingTaskId;
  private Long executeLatencyMs; // null if it wasn't sampled
    
  public BoltExecuteInfo(Tuple tuple, int executingTaskId, Long executeLatencyMs) {
    this.tuple = tuple;
    this.executingTaskId = executingTaskId;
    this.executeLatencyMs = executeLatencyMs;
  }

  public Tuple getTuple() {
    return tuple;
  }

  public int getExecutingTaskId() {
    return executingTaskId;
  }

  public Long getExecuteLatencyMs() {
    return executeLatencyMs;
  }
}
