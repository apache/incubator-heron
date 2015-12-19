package backtype.storm.generated;

public class GlobalStreamId {
  private String componentId; // required
  private String streamId; // required

  public GlobalStreamId() {
  }

  public GlobalStreamId(
    String componentId,
    String streamId)
  {
    this.componentId = componentId;
    this.streamId = streamId;
  }

  public void clear() {
    this.componentId = null;
    this.streamId = null;
  }

  public String get_componentId() {
    return this.componentId;
  }

  public void set_componentId(String componentId) {
    this.componentId = componentId;
  }

  public void unset_componentId() {
    this.componentId = null;
  }

  public String get_streamId() {
    return this.streamId;
  }

  public void set_streamId(String streamId) {
    this.streamId = streamId;
  }

  public void unset_streamId() {
    this.streamId = null;
  }
}
