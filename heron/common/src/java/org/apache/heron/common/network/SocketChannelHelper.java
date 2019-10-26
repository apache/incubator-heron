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

import java.nio.channels.ClosedChannelException;
import java.nio.channels.SocketChannel;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.logging.Logger;

import org.apache.heron.common.basics.ByteAmount;
import org.apache.heron.common.basics.ISelectHandler;
import org.apache.heron.common.basics.NIOLooper;

/**
 * This file defines the ChannelHelper class.
 * Heron Client and Server use the ChannelHelper class for doing all the reads/writes. A ChannelHelper
 * is a a placeholder for all network and io related information about a socket descriptor. It takes
 * care of async read/write of packets. Typical ways of instantiating ChannelHelper are described
 * below in a client and server settings.
 * <p>
 * SERVER
 * After an accept event, the server creates a ChannelHelper object giving it a proper option
 * ISelectHandler selectHandler = new Server();
 * NIOLoop looper = new NIOLooper();
 * SocketChannel socketChannel = ServerSocketChannel.accept();
 * ChannelHelper channelHelper = new ChannelHelper(looper, selectHandler, socketChannel, options);
 * After this point, you could use read()/write() to access socket
 * <p>
 * CLIENT
 * After a successful connect, the client creates a ChannelHelper object giving it a proper option
 * ISelectHandler selectHandler = new Client();
 * NIOLoop looper = new NIOLooper();
 * SocketChannel socketChannel = SocketChannel.open();
 * socketChannel.connect(endpoint);
 * ChannelHelper channelHelper = new ChannelHelper(looper, selectHandler, socketChannel, options);
 * After this point, you could use read()/write() to access socket.
 * <p>
 * Notice: The SocketChannelHelper would work only when the socketChannel is connected and opened.
 * Higher level logic needs to guarantee SocketChannelHelper's methods are invoked within proper
 * SocketChannel state.
 */

public class SocketChannelHelper {
  private static final Logger LOG = Logger.getLogger(SocketChannelHelper.class.getName());
  private final NIOLooper looper;
  private final ISelectHandler selectHandler;
  private final SocketChannel socketChannel;
  // The unbounded queue of outstanding packets that need to be sent
  // Carefully check the size of queue before offering packets into it
  // to avoid the unbounded-growth of queue
  private final Queue<OutgoingPacket> outgoingPacketsToWrite;

  // System Config related
  private final ByteAmount writeBatchSize;
  private final Duration writeBatchTime;
  private final ByteAmount readBatchSize;
  private final Duration readReadBatchTime;

  // Incompletely read next packet
  private IncomingPacket incomingPacket;
  private long totalPacketsRead;
  private long totalPacketsWritten;
  private long totalBytesRead;
  private long totalBytesWritten;
  private ByteAmount maximumPacketSize;

  public SocketChannelHelper(NIOLooper looper,
                             ISelectHandler selectHandler,
                             SocketChannel socketChannel,
                             HeronSocketOptions options) {
    this.looper = looper;
    this.selectHandler = selectHandler;
    this.socketChannel = socketChannel;
    this.outgoingPacketsToWrite = new LinkedList<OutgoingPacket>();
    this.incomingPacket = new IncomingPacket();

    this.writeBatchSize = options.getNetworkWriteBatchSize();
    this.writeBatchTime = options.getNetworkWriteBatchTime();

    this.readBatchSize = options.getNetworkReadBatchSize();
    this.readReadBatchTime = options.getNetworkReadBatchTime();

    this.maximumPacketSize = options.getMaximumPacketSize();

    // We will register Read by default when the connection is established
    // However, we will register Write only when we have something to write since
    // in most cases the socket will be writable but we have nothing to write
    this.enableReading();
  }

  public void clear() {
    outgoingPacketsToWrite.clear();
  }

  // Add this packet to the list of packets to be sent. The packet in itself can be sent
  // later. Packet should not be touched after sendPack on it is called
  // return value:
  // true indicates that the packet has been successfully queued to be
  // sent. It does not indicate that the packet was sent successfully.
  // false indicates an error. The most likely error is improperly
  // formatted packet.
  public boolean sendPacket(OutgoingPacket outgoingPacket) {
    // TODO -- add format check

    outgoingPacketsToWrite.add(outgoingPacket);
    enableWriting();
    return true;
  }

  // Read bytes stream from socket and convert them into a list of IncomingPacket
  // It would return an empty list if something bad happens
  public List<IncomingPacket> read() {
    // We record the start time to avoid spending too much time on readings
    long startOfCycle = System.nanoTime();
    long bytesRead = 0;

    long nPacketsRead = 0;

    List<IncomingPacket> ret = new ArrayList<IncomingPacket>();

    // We would stop reading when:
    // 1. We spent too much time
    // 2. We have read large enough data
    while ((System.nanoTime() - startOfCycle - readReadBatchTime.toNanos()) < 0
        && (bytesRead < readBatchSize.asBytes())) {
      int readState = incomingPacket.readFromChannel(socketChannel, maximumPacketSize.asBytes());

      if (readState > 0) {
        // Partial Read, just break, and read next time when the socket is readable
        break;
      } else if (readState < 0) {
        LOG.severe("Something bad happened while reading from channel: "
            + socketChannel.socket().getRemoteSocketAddress());
        selectHandler.handleError(socketChannel);

        // Clear the list of Incoming Packet to avoid bad state is used externally
        ret.clear();
        break;
      } else {
        // readState == 0, we fully read a incomingPacket
        nPacketsRead++;
        bytesRead += incomingPacket.size();
        ret.add(incomingPacket);
        incomingPacket = new IncomingPacket();
      }
    }

    totalPacketsRead += nPacketsRead;
    totalBytesRead += bytesRead;

    return ret;
  }

  // Write the outgoingPackets in buffer to socket
  public void write() {
    // We record the start time to avoid spending too much time on writings
    long startOfCycle = System.nanoTime();
    long bytesWritten = 0;

    long nPacketsWritten = 0;

    while ((System.nanoTime() - startOfCycle - writeBatchTime.toNanos()) < 0
        && (bytesWritten < writeBatchSize.asBytes())) {
      OutgoingPacket outgoingPacket = outgoingPacketsToWrite.peek();
      if (outgoingPacket == null) {
        break;
      }

      int writeState = outgoingPacket.writeToChannel(socketChannel);
      if (writeState > 0) {
        // Partial writing, we would break since we could not write more data on socket.
        // But we have set the next start point of OutgoingPacket
        // Next time when the socket is writable, it will start from that point.
        break;
      } else if (writeState < 0) {
        LOG.severe("Something bad happened while writing to channel");
        selectHandler.handleError(socketChannel);
        return;
      } else {
        // writeState == 0, we fully write a outgoingPacket
        bytesWritten += outgoingPacket.size();
        nPacketsWritten++;

        outgoingPacketsToWrite.remove();
      }
    }

    totalPacketsWritten += nPacketsWritten;
    totalBytesWritten += bytesWritten;

    // Disable writing if there are nothing to send in buffer
    if (getOutstandingPackets() == 0) {
      disableWriting();
    }
  }

  // Force to flush all data in underneath buffer queue to socket with best effort
  // It is most likely happen when we are handling some unexpected cases, such as exiting
  public void forceFlushWithBestEffort() {
    LOG.info("Forcing to flush data to socket with best effort.");
    while (!outgoingPacketsToWrite.isEmpty()) {
      int writeState = outgoingPacketsToWrite.poll().writeToChannel(socketChannel);
      if (writeState != 0) {
        LOG.info("Failed to write more to Socket. Clear and finish the flush.");
        clear();
        return;
      }
    }
  }

  public void enableReading() {
    if (!looper.isReadRegistered(socketChannel)) {
      try {
        looper.registerRead(socketChannel, selectHandler);
      } catch (ClosedChannelException e) {
        selectHandler.handleError(socketChannel);
      }
    }
  }

  public void disableReading() {
    if (looper.isReadRegistered(socketChannel)) {
      looper.unregisterRead(socketChannel);
    }
  }

  public void enableWriting() {
    if (!looper.isWriteRegistered(socketChannel)) {
      try {
        looper.registerWrite(socketChannel, selectHandler);
      } catch (ClosedChannelException e) {
        selectHandler.handleError(socketChannel);
      }
    }
  }

  public void disableWriting() {
    if (looper.isWriteRegistered(socketChannel)) {
      looper.unregisterWrite(socketChannel);
    }
  }

  public int getOutstandingPackets() {
    return outgoingPacketsToWrite.size();
  }

  public boolean hasPacketsToSend() {
    return outgoingPacketsToWrite.size() > 0;
  }

  public long getTotalPacketsWritten() {
    return totalPacketsWritten;
  }

  public long getTotalPacketsRead() {
    return totalPacketsRead;
  }

  public long getTotalBytesRead() {
    return totalBytesRead;
  }

  public long getTotalBytesWritten() {
    return totalBytesWritten;
  }
}
