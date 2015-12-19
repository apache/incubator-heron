package com.twitter.heron.api.hooks.info;

public class SpoutAckInfo {
  private Object messageId;
  private int spoutTaskId;
  private Long completeLatencyMs; // null if it wasn't sampled
    
  public SpoutAckInfo(Object messageId, int spoutTaskId, Long completeLatencyMs) {
    this.messageId = messageId;
    this.spoutTaskId = spoutTaskId;
    this.completeLatencyMs = completeLatencyMs;
  }

  public Object getMessageId() {
    return messageId;
  }

  public int getSpoutTaskId() {
    return spoutTaskId;
  }

  public Long getCompleteLatencyMs() {
    return completeLatencyMs;
  }
}
