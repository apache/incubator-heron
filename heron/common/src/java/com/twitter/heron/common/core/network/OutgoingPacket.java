package com.twitter.heron.common.core.network;

import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.protobuf.Message;

/**
 * Defines OutgoingPacket
 * <p/>
 * TODO -- Sanjeev will add a detailed description of this application level protocol later
 * <p/>
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
 * <p/>
 * 2. Experiments are done by using direct buffer and the resources saving is negligible:
 * -- Direct buffer would save, in our scenarios, less than 1% of RAM;
 * -- Direct buffer could save 30%~50% cpu of Gateway thread.
 * However, the cpu used by Gateway thread is negligible,
 * less than 2% out of the whole usage in worst case.
 * -- The extra copy is within JVM boundary; it is pretty fast.
 */

public class OutgoingPacket {
  private static final Logger LOG = Logger.getLogger(OutgoingPacket.class.getName());
  private ByteBuffer buffer;

  public OutgoingPacket(REQID _reqid, Message _message) {
    assert _message.isInitialized();
    // First calculate the total size of the packet
    // including the header
    int headerSize = 4;
    String typename = _message.getDescriptorForType().getFullName();
    int dataSize = sizeRequiredToPackString(typename) +
        REQID.REQIDSize +
        sizeRequiredToPackMessage(_message);
    buffer = ByteBuffer.allocate(headerSize + dataSize);

    // First write out how much data is there as the header
    buffer.putInt(dataSize);

    // Next write the type string
    buffer.putInt(typename.length());
    buffer.put(typename.getBytes());

    // now the reqid
    _reqid.pack(buffer);

    // finally the proto
    // Double copy but it is designed, see the comments on top
    buffer.putInt(_message.getSerializedSize());
    buffer.put(_message.toByteArray());

    // Make the buffer ready for writing out
    buffer.flip();
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
    } catch (Exception e) {
      LOG.log(Level.SEVERE, "Error writing to channel ", e);
      return -1;
    }
    return remaining - wrote;
  }

  public int size() {
    return buffer.capacity();
  }
}
