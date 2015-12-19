package com.twitter.heron.state.zookeeper;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

import com.twitter.heron.state.WatchCallback;

public class ZkWatcherCallback {
  static public Watcher makeZkWatcher(final WatchCallback watcher) {
    return watcher == null ?
        null :
        new Watcher() {
          @Override
          public void process(WatchedEvent watchedEvent) {
            WatchCallback.WatchEventType watchEventType;
            switch (watchedEvent.getType()) {
              case None:
                watchEventType = WatchCallback.WatchEventType.None;
                break;
              case NodeCreated:
                watchEventType = WatchCallback.WatchEventType.NodeCreated;
                break;
              case NodeDeleted:
                watchEventType = WatchCallback.WatchEventType.NodeDeleted;
                break;
              case NodeDataChanged:
                watchEventType = WatchCallback.WatchEventType.NodeDataChanged;
                break;
              case NodeChildrenChanged:
                watchEventType = WatchCallback.WatchEventType.NodeChildrenChanged;
                break;

              default:
                throw new RuntimeException("Invalid integer value for conversion to EventType");
            }
            watcher.processWatch(watchedEvent.getPath(), watchEventType);
          }
        };
  }
}
