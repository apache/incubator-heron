package com.twitter.heron.common.core.network;

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
