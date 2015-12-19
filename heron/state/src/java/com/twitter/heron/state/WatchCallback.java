package com.twitter.heron.state;

public interface WatchCallback {
  public enum WatchEventType {
    None,
    NodeCreated,
    NodeDeleted,
    NodeDataChanged,
    NodeChildrenChanged;
  }

  /**
   * @param path the node path
   * @param eventType the WatchEventType
   */
  public void processWatch(String path, WatchEventType eventType);
}
