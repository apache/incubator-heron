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

/**
 * Options that Heron Server/Client passes to config
 * 1. JAVA SocketChannel
 * 2. SocketChannelHelper
 */
public class HeronSocketOptions {
  private long networkWriteBatchSizeInBytes;
  private long networkWriteBatchTimeInMs;
  private long networkReadBatchSizeInBytes;
  private long networkReadBatchTimeInMs;
  private int socketSendBufferSizeInBytes;
  private int socketReceivedBufferSizeInBytes;

  public HeronSocketOptions(long networkWriteBatchSizeInBytes,
                            long networkWriteBatchTimeInMs,
                            long networkReadBatchSizeInBytes,
                            long networkReadBatchTimeInMs,
                            int socketSendBufferSizeInBytes,
                            int socketReceivedBufferSizeInBytes) {
    this.networkWriteBatchSizeInBytes = networkWriteBatchSizeInBytes;
    this.networkWriteBatchTimeInMs = networkWriteBatchTimeInMs;
    this.networkReadBatchSizeInBytes = networkReadBatchSizeInBytes;
    this.networkReadBatchTimeInMs = networkReadBatchTimeInMs;
    this.socketSendBufferSizeInBytes = socketSendBufferSizeInBytes;
    this.socketReceivedBufferSizeInBytes = socketReceivedBufferSizeInBytes;
  }

  public long getNetworkWriteBatchSizeInBytes() {
    return networkWriteBatchSizeInBytes;
  }

  public void setNetworkWriteBatchSizeInBytes(long networkWriteBatchSizeInBytes) {
    this.networkWriteBatchSizeInBytes = networkWriteBatchSizeInBytes;
  }

  public long getNetworkWriteBatchTimeInMs() {
    return networkWriteBatchTimeInMs;
  }

  public void setNetworkWriteBatchTimeInMs(long networkWriteBatchTimeInMs) {
    this.networkWriteBatchTimeInMs = networkWriteBatchTimeInMs;
  }

  public long getNetworkReadBatchSizeInBytes() {
    return networkReadBatchSizeInBytes;
  }

  public void setNetworkReadBatchSizeInBytes(long networkReadBatchSizeInBytes) {
    this.networkReadBatchSizeInBytes = networkReadBatchSizeInBytes;
  }

  public long getNetworkReadBatchTimeInMs() {
    return networkReadBatchTimeInMs;
  }

  public void setNetworkReadBatchTimeInMs(long networkReadBatchTimeInMs) {
    this.networkReadBatchTimeInMs = networkReadBatchTimeInMs;
  }

  public int getSocketSendBufferSizeInBytes() {
    return socketSendBufferSizeInBytes;
  }

  public void setSocketSendBufferSizeInBytes(int socketSendBufferSizeInBytes) {
    this.socketSendBufferSizeInBytes = socketSendBufferSizeInBytes;
  }

  public int getSocketReceivedBufferSizeInBytes() {
    return socketReceivedBufferSizeInBytes;
  }

  public void setSocketReceivedBufferSizeInBytes(int socketReceivedBufferSizeInBytes) {
    this.socketReceivedBufferSizeInBytes = socketReceivedBufferSizeInBytes;
  }
}
