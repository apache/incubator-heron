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

package org.apache.heron.simulator.instance;

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

