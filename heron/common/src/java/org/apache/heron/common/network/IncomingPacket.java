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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;

/**
 * Defines IncomingPacket
 * <p>
 * TODO -- Sanjeev will add a detailed description of this application level protocol later
 * <p>
 * When allocating the ByteBuffer, we have two options:
 * 1. Normal java heap buffer by invoking ByteBuffer.allocate(...),
 * 2. Native heap buffer by invoking ByteBuffer.allocateDirect(...),
 * Though it would require extra memory copies in java heap buffer, after experiments trying to use
 * both of them, we choose to use normal java heap buffer, since:
 * 1. It is unsafe to use direct buffer:
 * -- Direct buffer would not trigger gc;
 * -- We could not control when to release the resources of direct buffer explicitly;
 * -- It is hard to guarantee direct buffer would not break limitation of native heap,
 * i.e. not throw OutOfMemoryError.
 * <p>
 * 2. Experiments are done by using direct buffer and the resources saving is negligible:
 * -- Direct buffer would save, in our scenarios, less than 1% of RAM;
 * -- Direct buffer could save 30%~50% CPU of Gateway thread.
 * However, the CPU used by Gateway thread is negligible,
 * less than 2% out of the whole usage in worst case.
 * -- The extra copy is within JVM boundary; it is pretty fast.
 */

public class IncomingPacket {
  private static final Logger LOG = Logger.getLogger(IncomingPacket.class.getName());
  private ByteBuffer header;
  private ByteBuffer data;
  private boolean headerRead;

  public IncomingPacket() {
    header = ByteBuffer.allocate(4);
    headerRead = false;
  }

  public int readFromChannel(SocketChannel channel) {
    return readFromChannel(channel, Integer.MAX_VALUE);
  }

  public int readFromChannel(SocketChannel channel, long limit) {
    if (!headerRead) {
      int retval = readFromChannel(channel, header);
      if (retval != 0) {
        // either we didnt read fully or we had an error
        return retval;
      }
      // We read the header fully
      headerRead = true;
      BufferHelper.flip(header);
      int size = header.getInt();
      if (size > limit) {
        LOG.log(Level.SEVERE, "packet size " + size + " exceeds limit " + limit);
        return -1;
      }
      data = ByteBuffer.allocate(size);
    }
    int retval = readFromChannel(channel, data);
    if (retval == 0) {
      BufferHelper.flip(data);
    }
    return retval;
  }

  private int readFromChannel(SocketChannel channel, ByteBuffer buffer) {
    int remaining = buffer.remaining();
    int wrote = 0;
    try {
      wrote = channel.read(buffer);
    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Error in channel.read ", e);
      return -1;
    }
    if (wrote < 0) {
      // We encountered an end of stream. report error
      LOG.severe("channel.read returned negative " + wrote);
      return wrote;
    } else {
      // We wrote something
      return remaining - wrote;
    }
  }

  // TODO:- Multiple copies going on here.
  public String unpackString() {
    int size = data.getInt();
    byte[] bytes = new byte[size];
    data.get(bytes);
    return new String(bytes);
  }

  public REQID unpackREQID() {
    return new REQID(data);
  }

  // TODO:- Multiple copies going on here.
  public void unpackMessage(Message.Builder builder) {
    int size = data.getInt();
    byte[] bytes = new byte[size];
    data.get(bytes);
    try {
      builder.mergeFrom(bytes);
    } catch (InvalidProtocolBufferException e) {
      LOG.log(Level.SEVERE, "InvalidProtocolBufferException: ", e);
    }
  }

  // TODO -- the calculation is not accurate but work
  public int size() {
    return data == null ? 0 : data.capacity();
  }
}
