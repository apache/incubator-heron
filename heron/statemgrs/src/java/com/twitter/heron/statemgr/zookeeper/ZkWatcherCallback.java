// Copyright 2016 Twitter. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.twitter.heron.statemgr.zookeeper;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

import com.twitter.heron.spi.statemgr.WatchCallback;

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
