package backtype.storm.hooks.info;

public class SpoutFailInfo {
  public Object messageId;
  public int spoutTaskId;
  public Long failLatencyMs; // null if it wasn't sampled

  public SpoutFailInfo(Object messageId, int spoutTaskId, Long failLatencyMs) {
    this.messageId = messageId;
    this.spoutTaskId = spoutTaskId;
    this.failLatencyMs = failLatencyMs;
  }

  public SpoutFailInfo(com.twitter.heron.api.hooks.info.SpoutFailInfo info) {
    this.messageId = info.getMessageId();
    this.spoutTaskId = info.getSpoutTaskId();
    this.failLatencyMs = info.getFailLatencyMs();
  }
}
