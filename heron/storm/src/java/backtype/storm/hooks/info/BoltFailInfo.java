package backtype.storm.hooks.info;

import backtype.storm.tuple.Tuple;

public class BoltFailInfo {
  public Tuple tuple;
  public int failingTaskId;
  public Long failLatencyMs; // null if it wasn't sampled

  public BoltFailInfo(Tuple tuple, int failingTaskId, Long failLatencyMs) {
    this.tuple = tuple;
    this.failingTaskId = failingTaskId;
    this.failLatencyMs = failLatencyMs;
  }

  public BoltFailInfo(com.twitter.heron.api.hooks.info.BoltFailInfo info) {
    this.tuple = new backtype.storm.tuple.TupleImpl(info.getTuple());
    this.failingTaskId = info.getFailingTaskId();
    this.failLatencyMs = info.getFailLatencyMs();
  }
}
