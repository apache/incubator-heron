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

package com.twitter.heron.common.network;

import com.twitter.heron.common.basics.ByteAmount;

/**
 * Options that Heron Server/Client passes to config
 * 1. JAVA SocketChannel
 * 2. SocketChannelHelper
 */
public class HeronSocketOptions {
  private ByteAmount networkWriteBatchSize;
  private long networkWriteBatchTimeInMs;
  private ByteAmount networkReadBatchSize;
  private long networkReadBatchTimeInMs;
  private ByteAmount socketSendBufferSize;
  private ByteAmount socketReceivedBufferSize;

  public HeronSocketOptions(ByteAmount networkWriteBatchSize,
                            long networkWriteBatchTimeInMs,
                            ByteAmount networkReadBatchSize,
                            long networkReadBatchTimeInMs,
                            ByteAmount socketSendBufferSize,
                            ByteAmount socketReceivedBufferSize) {
    this.networkWriteBatchSize = networkWriteBatchSize;
    this.networkWriteBatchTimeInMs = networkWriteBatchTimeInMs;
    this.networkReadBatchSize = networkReadBatchSize;
    this.networkReadBatchTimeInMs = networkReadBatchTimeInMs;
    this.socketSendBufferSize = socketSendBufferSize;
    this.socketReceivedBufferSize = socketReceivedBufferSize;
  }

  public ByteAmount getNetworkWriteBatchSize() {
    return networkWriteBatchSize;
  }

  public long getNetworkWriteBatchTimeInMs() {
    return networkWriteBatchTimeInMs;
  }

  public ByteAmount getNetworkReadBatchSize() {
    return networkReadBatchSize;
  }

  public long getNetworkReadBatchTimeInMs() {
    return networkReadBatchTimeInMs;
  }

  public ByteAmount getSocketSendBufferSize() {
    return socketSendBufferSize;
  }

  public ByteAmount getSocketReceivedBufferSize() {
    return socketReceivedBufferSize;
  }
}
