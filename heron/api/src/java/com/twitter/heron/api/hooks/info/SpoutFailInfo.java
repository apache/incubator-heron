package com.twitter.heron.api.hooks.info;

public class SpoutFailInfo {
  private Object messageId;
  private int spoutTaskId;
  private Long failLatencyMs; // null if it wasn't sampled
    
  public SpoutFailInfo(Object messageId, int spoutTaskId, Long failLatencyMs) {
    this.messageId = messageId;
    this.spoutTaskId = spoutTaskId;
    this.failLatencyMs = failLatencyMs;
  }

  public Object getMessageId() {
    return messageId;
  }

  public int getSpoutTaskId() {
    return spoutTaskId;
  }

  public Long getFailLatencyMs() {
    return failLatencyMs;
  }
}
