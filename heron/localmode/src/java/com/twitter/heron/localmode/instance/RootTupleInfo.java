package com.twitter.heron.localmode.instance;

public class RootTupleInfo implements Comparable<RootTupleInfo> {
  private final String streamId;
  private final Object messageId;
  private final long insertionTime;

  public RootTupleInfo(String streamId, Object messageId) {
    this.streamId = streamId;
    this.messageId = messageId;
    this.insertionTime = System.nanoTime();
  }

  public boolean isExpired(long curTime, long timeoutInNs) {
    return insertionTime + timeoutInNs - curTime <= 0;
  }

  public long getInsertionTime() {
    return insertionTime;
  }

  public Object getMessageId() {
    return messageId;
  }

  public String getStreamId() {
    return streamId;
  }

  @Override
  public int compareTo(RootTupleInfo that) {
    if (insertionTime - that.getInsertionTime() < 0) {
      return -1;
    } else if (insertionTime - that.getInsertionTime() > 0) {
      return 1;
    } else {
      return 0;
    }
  }
}