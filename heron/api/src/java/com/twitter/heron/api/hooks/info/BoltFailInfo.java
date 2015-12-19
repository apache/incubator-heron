package com.twitter.heron.api.hooks.info;

import com.twitter.heron.api.tuple.Tuple;

public class BoltFailInfo {
  private Tuple tuple;
  private int failingTaskId;
  private Long failLatencyMs; // null if it wasn't sampled
    
  public BoltFailInfo(Tuple tuple, int failingTaskId, Long failLatencyMs) {
    this.tuple = tuple;
    this.failingTaskId = failingTaskId;
    this.failLatencyMs = failLatencyMs;
  }

  public Tuple getTuple() {
    return tuple;
  }

  public int getFailingTaskId() {
    return failingTaskId;
  }

  public Long getFailLatencyMs() {
    return failLatencyMs;
  }
}
