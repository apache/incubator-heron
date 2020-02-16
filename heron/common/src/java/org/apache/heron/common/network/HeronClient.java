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
import java.net.InetSocketAddress;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SocketChannel;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.protobuf.Message;

import org.apache.heron.common.basics.ISelectHandler;
import org.apache.heron.common.basics.NIOLooper;

/**
 * Implements this class could handle some following socket related behaviors:
 * 1. handleRead(SelectableChannel), which read data from a socket and convert into incomingPacket.
 * It could handle the conditions of closedConnection, normal Reading and partial Reading. When an
 * incomingPacket is read, it will be pass to handlePacket(), which will convert incomingPackets to
 * messages and call onIncomingMessage(message), which should be implemented by its child class.
 * <p>
 * 2. handleWrite(SelectableChannel), which will try to get outgoing message by calling getOutgoingMessage(),
 * pack the outgoing message into OutgoingPacket and write to the sockets.
 * <p>
 * 3. handleConnect(SelectableChannel), which handles some basic setup when this client connect to
 * remote endpoint.
 * <p>
 * 4. handleAccept(SelectableChannel).
 * <p>
 * 5. handleError(SelectableChannel).
 * Remember, the socket client will register Read when the socket is connectible. However, it will
 * register Write when having something to write since the socket in most cases is writable.
 * To implement this, we will add the check whether write is needed into persistent tasks.
 */
public abstract class HeronClient implements ISelectHandler {
  private static final Logger LOG = Logger.getLogger(HeronClient.class.getName());
  private static final Object DUMMY = new Object();

  // When we send a request, we need to:
  // record the the context for this particular RID, and prepare the response for that RID
  // Then when the response come back, we could handle it
  protected Map<REQID, Object> contextMap;
  protected Map<REQID, Message.Builder> responseMessageMap;

  // Map from protobuf message's name to protobuf message
  protected Map<String, Message.Builder> messageMap;
  private SocketChannel socketChannel;

  // Define the endpoint this socket client will communicate with
  private InetSocketAddress endpoint;
  private NIOLooper nioLooper;
  private SocketChannelHelper socketChannelHelper;
  private HeronSocketOptions socketOptions;

  // A flag to determine whether the socket is connected or not
  // We could not simply use socketChanel.isConnected() to tell whether the socketChannel
  // is connected or not, since:
  // SocketChannel.socket().isConnected() and SocketChannel.isConnected()
  // return false before the socket is connected.
  // Once the socket is connected they will return true,
  // they will not revert to false for any reason.
  // It violates what is documented in
  // http://docs.oracle.com/javase/7/docs/api/java/nio/channels/SocketChannel.html#isConnected()
  // Consider it is a JAVA bug
  private boolean isConnected;

  /**
   * Constructor
   *
   * @param s the NIOLooper bind with this socket client
   * @param host the host of remote endpoint to communicate with
   * @param port the port of remote endpoint to communicate with
   */
  public HeronClient(NIOLooper s, String host, int port, HeronSocketOptions options) {
    nioLooper = s;
    endpoint = new InetSocketAddress(host, port);
    socketOptions = options;

    isConnected = false;
    contextMap = new ConcurrentHashMap<REQID, Object>();
    responseMessageMap = new ConcurrentHashMap<REQID, Message.Builder>();
    messageMap = new ConcurrentHashMap<String, Message.Builder>();
  }

  // Register the protobuf Message's name with protobuf Message
  public void registerOnMessage(Message.Builder builder) {
    messageMap.put(builder.getDescriptorForType().getFullName(), builder);
  }

  public void start() {
    try {
      socketChannel = SocketChannel.open();
      socketChannel.configureBlocking(false);

      // Set the maximum possible send and receive buffers
      socketChannel.socket().setSendBufferSize(
          (int) socketOptions.getSocketSendBufferSize().asBytes());
      socketChannel.socket().setReceiveBufferSize(
          (int) socketOptions.getSocketReceivedBufferSize().asBytes());
      socketChannel.socket().setTcpNoDelay(true);

      // If the socketChannel has already connect to endpoint, call handleConnect()
      // Otherwise, registerConnect(), which will call handleConnect() when it is connectible
      LOG.info("Connecting to endpoint: " + endpoint);
      if (socketChannel.connect(endpoint)) {
        handleConnect(socketChannel);
      } else {
        nioLooper.registerConnect(socketChannel, this);
      }
    } catch (IOException e) {
      // Call onConnect() with CONNECT_ERROR
      LOG.log(Level.SEVERE, "Error connecting to remote endpoint: " + endpoint, e);
      Runnable r = new Runnable() {
        public void run() {
          onConnect(StatusCode.CONNECT_ERROR);
        }
      };
      nioLooper.registerTimerEvent(Duration.ZERO, r);
    }
  }

  public void stop() {
    if (!isConnected()) {
      return;
    }

    // Flush the data to socket with best effort
    forceFlushWithBestEffort();

    LOG.info("To stop the HeronClient.");
    contextMap.clear();
    responseMessageMap.clear();
    messageMap.clear();
    socketChannelHelper.clear();
    nioLooper.removeAllInterest(socketChannel);

    try {
      socketChannel.close();
      onClose();
    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Failed to stop Client", e);
    }
  }

  @Override
  public void handleRead(SelectableChannel channel) {
    List<IncomingPacket> packets = socketChannelHelper.read();
    for (IncomingPacket ipt : packets) {
      handlePacket(ipt);
    }
  }

  @Override
  public void handleWrite(SelectableChannel channel) {
    socketChannelHelper.write();
  }

  // Send a request to the server with a certain timeout in seconds
  // This function doesnt return anything. After this function returns,
  // does not mean that the request actually sent out, merely that the request
  // was successfully queued to be sent out.
  // Actual send occurs when the socket becomes readable and all prev
  // requests are sent. If the packet cannot be sent
  // out or the request is not retired by the client within the timeout
  // period, the HandleResponse is called with the appropriate status.
  // The request is now owned by the Client class.
  // The ctx is a user owned piece of context.
  // The response is a MessageBuilder to handle the response from server
  // A negative value of the timeout means no timeout.
  public void sendRequest(Message request, Object context, Message.Builder responseBuilder,
                          Duration timeout) {
    // Pack it as a no-timeout request and send it!
    final REQID rid = REQID.generate();
    contextMap.put(rid, Objects.nonNull(context) ? context : DUMMY); // Fix NPE
    responseMessageMap.put(rid, responseBuilder);

    // Add timeout for this request if necessary
    if (timeout.getSeconds() > 0) {
      registerTimerEvent(timeout, new Runnable() {
        @Override
        public void run() {
          handleTimeout(rid);
        }
      });
    }

    OutgoingPacket opk = new OutgoingPacket(rid, request);
    socketChannelHelper.sendPacket(opk);
  }

  // Convenience method of the above method with no timeout or context
  public void sendRequest(Message request, Message.Builder responseBuilder) {
    sendRequest(request, null, responseBuilder, Duration.ZERO);
  }

  // This method is used if you want to communicate with the other end
  // on a non-request-response based communication.
  public void sendMessage(Message message) {
    OutgoingPacket opk = new OutgoingPacket(REQID.zeroREQID, message);
    socketChannelHelper.sendPacket(opk);
  }

  public boolean isConnected() {
    return isConnected;
  }

  public NIOLooper getNIOLooper() {
    return nioLooper;
  }

  // Add a timer to be invoked after timer duration.
  private void registerTimerEvent(Duration timer, Runnable task) {
    nioLooper.registerTimerEvent(timer, task);
  }

  @Override
  public void handleAccept(SelectableChannel channel) {
    throw new RuntimeException("Client does not implement accept");
  }

  @Override
  public void handleConnect(SelectableChannel channel) {
    try {
      if (socketChannel.finishConnect()) {
        // If we finishConnect(), we have to unregisterConnect, otherwise there will be a bug
        // http://bugs.java.com/bugdatabase/view_bug.do?bug_id=4960791
        nioLooper.unregisterConnect(channel);
      }
    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Failed to FinishConnect to endpoint: " + endpoint, e);
      Runnable r = new Runnable() {
        public void run() {
          onConnect(StatusCode.CONNECT_ERROR);
        }
      };
      nioLooper.registerTimerEvent(Duration.ZERO, r);
      return;
    }

    // Construct the ChannelHelper and by default it would:
    // 1. always read
    // 2. write if # of packets to send > 0
    socketChannelHelper = new SocketChannelHelper(nioLooper, this, socketChannel, socketOptions);

    // Only when we fully connected, we set isConnected true
    isConnected = true;

    onConnect(StatusCode.OK);
  }

  /**
   * Handle an incomingPacket and if necessary,
   * convert it to Message and call onIncomingMessage() to handle it
   */
  protected void handlePacket(IncomingPacket incomingPacket) {
    String typeName = incomingPacket.unpackString();
    REQID rid = incomingPacket.unpackREQID();
    if (contextMap.containsKey(rid)) {
      // This incomingPacket contains the response of Request
      Object ctx = contextMap.get(rid);
      Message.Builder bldr = responseMessageMap.get(rid);
      contextMap.remove(rid);
      responseMessageMap.remove(rid);
      incomingPacket.unpackMessage(bldr);
      // Call onResponse to handle it
      if (bldr.isInitialized()) {
        Message response = bldr.build();
        onResponse(StatusCode.OK, ctx, response);
        return;
      } else {
        onResponse(StatusCode.INVALID_PACKET, ctx, null);
        return;
      }
    } else if (rid.equals(REQID.zeroREQID)) {
      // If rid is REQID.zeroREQID, this is a Message, e.g. no need send back response.
      // Convert it into message and call onIncomingMessage() to handle it
      Message.Builder bldr = messageMap.get(typeName);
      if (bldr != null) {
        bldr.clear();
        incomingPacket.unpackMessage(bldr);
        if (bldr.isInitialized()) {
          onIncomingMessage(bldr.build());
        } else {
          // We just need to log here
          // TODO:- log
        }
      } else {
        // We got a message but we didn't register
        // TODO:- log here
      }
    } else {
      // This might be a timeout response
      // TODO:- log here
    }
  }

  // Handle the timeout for a particular REQID
  protected void handleTimeout(REQID rid) {
    if (contextMap.containsKey(rid)) {
      Object ctx = contextMap.get(rid);
      contextMap.remove(rid);
      responseMessageMap.remove(rid);
      onResponse(StatusCode.TIMEOUT_ERROR, ctx, null);
    } else {
      // Since we dont do cancel timer, this is because we already have
      // the response. So just disregard this timeout
      // TODO:- implement cancel timer to avoid this overhead
    }
  }

  // Clean the stuff when meeting some errors
  public void handleError(SelectableChannel channel) {
    LOG.info("Handling Error. Cleaning states in HeronClient.");
    contextMap.clear();
    responseMessageMap.clear();
    messageMap.clear();
    socketChannelHelper.clear();
    nioLooper.removeAllInterest(channel);

    try {
      channel.close();
      LOG.info("Successfully closed the channel: " + channel);
    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Failed to close connection in handleError", e);
    }

    // Since we closed the channel, we set isConnected false
    isConnected = false;
    onError();
  }

  public void startReading() {
    socketChannelHelper.enableReading();
  }

  public void stopReading() {
    socketChannelHelper.disableReading();
  }

  public void startWriting() {
    socketChannelHelper.enableWriting();
  }

  public void stopWriting() {
    socketChannelHelper.disableWriting();
  }

  public int getOutstandingPackets() {
    return socketChannelHelper.getOutstandingPackets();
  }

  // Force to flush all data to be sent by HeronClient
  public void forceFlushWithBestEffort() {
    socketChannelHelper.forceFlushWithBestEffort();
  }

  /////////////////////////////////////////////////////////
  // This is the interface that needs to be implemented by
  // all Heron Clients.
  /////////////////////////////////////////////////////////

  // What action do you want to take when the client meets errors
  public abstract void onError();

  // What action do you want to take when connecting to a new server
  public abstract void onConnect(StatusCode status);

  // What action do you want to take when you get a new
  // response from a particular server
  public abstract void onResponse(StatusCode status, Object ctx, Message response);

  // What action do you want to take when you get a new
  // message from a particular server
  public abstract void onIncomingMessage(Message message);

  // What action do you want to take when we want to stop this client
  public abstract void onClose();

  /////////////////////////////////////////////////////////
  // Following protected methods are just used for testing
  /////////////////////////////////////////////////////////
  protected Map<String, Message.Builder> getMessageMap() {
    return new ConcurrentHashMap<String, Message.Builder>(messageMap);
  }

  protected Map<REQID, Message.Builder> getResponseMessageMap() {
    return new ConcurrentHashMap<REQID, Message.Builder>(responseMessageMap);
  }

  protected Map<REQID, Object> getContextMap() {
    return new ConcurrentHashMap<>(contextMap);
  }

  protected SocketChannelHelper getSocketChannelHelper() {
    return socketChannelHelper;
  }

  protected SocketChannel getSocketChannel() {
    return socketChannel;
  }

}
