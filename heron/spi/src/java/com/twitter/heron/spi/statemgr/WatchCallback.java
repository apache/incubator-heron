package com.twitter.heron.spi.statemgr;

/**
 * A callback interface used to set a watch on any of the nodes
 * in certain implemenations of IStateManager (Zookeeper for example)
 * Any event on that node will trigger the callback (processWatch).
 */
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
