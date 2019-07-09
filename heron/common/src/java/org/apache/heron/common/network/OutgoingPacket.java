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

import com.google.protobuf.Message;

/**
 * Defines OutgoingPacket
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

public class OutgoingPacket {
  private static final Logger LOG = Logger.getLogger(OutgoingPacket.class.getName());
  private ByteBuffer buffer;

  public OutgoingPacket(REQID reqid, Message message) {
    assert message.isInitialized();
    // First calculate the total size of the packet
    // including the header
    int headerSize = 4;
    String typename = message.getDescriptorForType().getFullName();
    int dataSize = sizeRequiredToPackString(typename)
        + REQID.REQID_SIZE
        + sizeRequiredToPackMessage(message);
    buffer = ByteBuffer.allocate(headerSize + dataSize);

    // First write out how much data is there as the header
    buffer.putInt(dataSize);

    // Next write the type string
    buffer.putInt(typename.length());
    buffer.put(typename.getBytes());

    // now the reqid
    reqid.pack(buffer);

    // finally the proto
    // Double copy but it is designed, see the comments on top
    buffer.putInt(message.getSerializedSize());
    buffer.put(message.toByteArray());

    // Make the buffer ready for writing out
    BufferHelper.flip(buffer);
  }

  public static int sizeRequiredToPackString(String str) {
    return 4 + str.length();
  }

  public static int sizeRequiredToPackMessage(Message msg) {
    return 4 + msg.getSerializedSize();
  }

  public int writeToChannel(SocketChannel channel) {
    int remaining = buffer.remaining();
    assert remaining > 0;
    int wrote = 0;
    try {
      wrote = channel.write(buffer);
    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Error writing to channel ", e);
      return -1;
    }
    return remaining - wrote;
  }

  public int size() {
    return buffer.capacity();
  }
}
