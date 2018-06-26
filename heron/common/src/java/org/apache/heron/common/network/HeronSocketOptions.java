/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.heron.common.network;

import java.time.Duration;

import org.apache.heron.common.basics.ByteAmount;

/**
 * Options that Heron Server/Client passes to config
 * 1. JAVA SocketChannel
 * 2. SocketChannelHelper
 */
public class HeronSocketOptions {
  private ByteAmount networkWriteBatchSize;
  private Duration networkWriteBatchTime;
  private ByteAmount networkReadBatchSize;
  private Duration networkReadBatchTime;
  private ByteAmount socketSendBufferSize;
  private ByteAmount socketReceivedBufferSize;
  private ByteAmount maximumPacketSize;

  public HeronSocketOptions(ByteAmount networkWriteBatchSize,
                            Duration networkWriteBatchTime,
                            ByteAmount networkReadBatchSize,
                            Duration networkReadBatchTime,
                            ByteAmount socketSendBufferSize,
                            ByteAmount socketReceivedBufferSize,
                            ByteAmount maximumPacketSize) {
    this.networkWriteBatchSize = networkWriteBatchSize;
    this.networkWriteBatchTime = networkWriteBatchTime;
    this.networkReadBatchSize = networkReadBatchSize;
    this.networkReadBatchTime = networkReadBatchTime;
    this.socketSendBufferSize = socketSendBufferSize;
    this.socketReceivedBufferSize = socketReceivedBufferSize;
    this.maximumPacketSize = maximumPacketSize;
  }

  public ByteAmount getNetworkWriteBatchSize() {
    return networkWriteBatchSize;
  }

  public Duration getNetworkWriteBatchTime() {
    return networkWriteBatchTime;
  }

  public ByteAmount getNetworkReadBatchSize() {
    return networkReadBatchSize;
  }

  public Duration getNetworkReadBatchTime() {
    return networkReadBatchTime;
  }

  public ByteAmount getSocketSendBufferSize() {
    return socketSendBufferSize;
  }

  public ByteAmount getSocketReceivedBufferSize() {
    return socketReceivedBufferSize;
  }

  public ByteAmount getMaximumPacketSize() {
    return maximumPacketSize;
  }
}
