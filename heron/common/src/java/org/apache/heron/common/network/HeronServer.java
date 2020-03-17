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
import java.net.SocketAddress;
import java.nio.channels.SelectableChannel;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.protobuf.Message;

import org.apache.heron.common.basics.ISelectHandler;
import org.apache.heron.common.basics.NIOLooper;

/**
 * Any Heron Server should implement this abstract class.
 * In particular it should implement
 * a) onConnect method called when a new client connects to us
 * b) onClose method called when a existing client closes
 * c) onRequest method called when we have a new request
 * d) onMessage method called when we have a new message
 */
public abstract class HeronServer implements ISelectHandler {
  private static final Logger LOG = Logger.getLogger(HeronServer.class.getName());
  // The socket that we will use for accepting connections
  private ServerSocketChannel acceptChannel;
  // Define the address where we need to listen on
  private InetSocketAddress endpoint;
  private HeronSocketOptions socketOptions;
  // Our own looper
  private NIOLooper nioLooper;
  // All the clients that we have connected
  private Map<SocketChannel, SocketChannelHelper> activeConnections;
  // Map from protobuf message's name to protobuf message's builder
  private Map<String, Message.Builder> requestMap;
  private Map<String, Message.Builder> messageMap;

  /**
   * Constructor
   *
   * @param s the NIOLooper bind with this socket server
   * @param host the host of remote endpoint to communicate with
   * @param port the port of remote endpoint to communicate with
   */
  public HeronServer(NIOLooper s, String host, int port, HeronSocketOptions options) {
    nioLooper = s;
    endpoint = new InetSocketAddress(host, port);
    socketOptions = options;
    requestMap = new ConcurrentHashMap<String, Message.Builder>();
    messageMap = new ConcurrentHashMap<String, Message.Builder>();
    activeConnections = new ConcurrentHashMap<SocketChannel, SocketChannelHelper>();
  }

  public InetSocketAddress getEndpoint() {
    return endpoint;
  }

  // Register the protobuf Message's name with protobuf Message
  public void registerOnMessage(Message.Builder builder) {
    messageMap.put(builder.getDescriptorForType().getFullName(), builder);
  }

  // Register the protobuf Message's name with protobuf Message
  public void registerOnRequest(Message.Builder builder) {
    requestMap.put(builder.getDescriptorForType().getFullName(), builder);
  }

  public boolean start() {
    try {
      acceptChannel = ServerSocketChannel.open();
      acceptChannel.configureBlocking(false);
      acceptChannel.socket().bind(endpoint);
      nioLooper.registerAccept(acceptChannel, this);
      return true;
    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Failed to start server", e);
      return false;
    }
  }

  // Stop the HeronServer and clean relative staff
  public void stop() {
    if (acceptChannel == null || !acceptChannel.isOpen()) {
      LOG.info("Fail to stop server; not yet open.");
      return;
    }
    // Clear all connected socket and related stuff
    for (Map.Entry<SocketChannel, SocketChannelHelper> connections : activeConnections.entrySet()) {
      SocketChannel channel = connections.getKey();
      SocketAddress channelAddress = channel.socket().getRemoteSocketAddress();
      LOG.info("Closing connected channel from client: " + channelAddress);
      LOG.info("Removing all interest on channel: " + channelAddress);
      nioLooper.removeAllInterest(channel);

      // Dispatch the child instance
      onClose(channel);
      // Clear the SocketChannelHelper
      connections.getValue().clear();
    }

    // Clear state inside the HeronServer
    activeConnections.clear();
    requestMap.clear();
    messageMap.clear();
    try {
      acceptChannel.close();
    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Failed to close server", e);
    }
  }

  @Override
  public void handleAccept(SelectableChannel channel) {
    try {
      SocketChannel socketChannel = acceptChannel.accept();
      if (socketChannel != null) {
        socketChannel.configureBlocking(false);
        // Set the maximum possible send and receive buffers
        socketChannel.socket().setSendBufferSize(
            (int) socketOptions.getSocketSendBufferSize().asBytes());
        socketChannel.socket().setReceiveBufferSize(
            (int) socketOptions.getSocketReceivedBufferSize().asBytes());
        socketChannel.socket().setTcpNoDelay(true);
        SocketChannelHelper helper = new SocketChannelHelper(nioLooper, this, socketChannel,
            socketOptions);
        activeConnections.put(socketChannel, helper);
        onConnect(socketChannel);
      }
    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Error while accepting a new connection ", e);
      // Note:- we are not calling onError
    }
  }

  @Override
  public void handleRead(SelectableChannel channel) {
    SocketChannelHelper helper = activeConnections.get(channel);
    if (helper == null) {
      LOG.severe("Unknown connection is ready for read");
      return;
    }
    List<IncomingPacket> packets = helper.read();
    for (IncomingPacket ipt : packets) {
      handlePacket(channel, ipt);
    }
  }

  @Override
  public void handleWrite(SelectableChannel channel) {
    SocketChannelHelper helper = activeConnections.get(channel);
    if (helper == null) {
      LOG.severe("Unknown connection is ready for read");
      return;
    }
    helper.write();
  }

  @Override
  public void handleConnect(SelectableChannel channel) {
    throw new RuntimeException("Server cannot have handleConnect");
  }

  /**
   * Handle an incomingPacket and invoke either onRequest or
   * onMessage() to handle it
   */
  private void handlePacket(SelectableChannel channel, IncomingPacket incomingPacket) {
    String typeName = incomingPacket.unpackString();
    REQID rid = incomingPacket.unpackREQID();
    Message.Builder bldr = requestMap.get(typeName);
    boolean isRequest = false;
    if (bldr != null) {
      // This is a request
      isRequest = true;
    } else {
      bldr = messageMap.get(typeName);
    }
    if (bldr != null) {
      // Clear the earlier state of Message.Builder
      // Otherwise it would merge new Message with old state
      bldr.clear();

      incomingPacket.unpackMessage(bldr);
      if (bldr.isInitialized()) {
        Message msg = bldr.build();
        if (isRequest) {
          onRequest(rid, (SocketChannel) channel, msg);
        } else {
          onMessage((SocketChannel) channel, msg);
        }
      } else {
        // Message failed to be deser
        LOG.severe("Could not deserialize protobuf of type " + typeName);
        handleError(channel);
      }
      return;
    } else {
      LOG.severe("Unexpected protobuf type received " + typeName);
      handleError(channel);
    }
  }

  // Clean the stuff when meeting some errors
  public void handleError(SelectableChannel channel) {
    SocketAddress channelAddress = ((SocketChannel) channel).socket().getRemoteSocketAddress();
    LOG.info("Handling error from channel: " + channelAddress);
    SocketChannelHelper helper = activeConnections.get(channel);
    if (helper == null) {
      LOG.severe("Inactive channel had error?");
      return;
    }
    helper.clear();
    LOG.info("Removing all interest on channel: " + channelAddress);
    nioLooper.removeAllInterest(channel);
    try {
      channel.close();
    } catch (IOException e) {
      LOG.severe("Error closing connection in handleError");
    }
    activeConnections.remove(channel);
    onClose((SocketChannel) channel);
  }

  // Send back the response to the client.
  // A false return value means that the response could not be sent.
  // Upon returning true, it does not mean that the response was actually
  // sent out, merely that the response was queueud to be sent out.
  // Actual send occurs when the socket becomes writable and all prev
  // responses/messages are sent.
  public boolean sendResponse(REQID rid, SocketChannel channel, Message response) {
    SocketChannelHelper helper = activeConnections.get(channel);
    if (helper == null) {
      LOG.severe("Trying to send a response on an unknown connection");
      return false;
    }
    OutgoingPacket opk = new OutgoingPacket(rid, response);
    helper.sendPacket(opk);
    return true;
  }

  // This method is used if you want to communicate with the other end
  // on a non-request-response based communication.
  public boolean sendMessage(SocketChannel channel, Message message) {
    return sendResponse(REQID.zeroREQID, channel, message);
  }

  public NIOLooper getNIOLooper() {
    return nioLooper;
  }

  // Add a timer to be invoked after timer duration.
  public void registerTimerEvent(Duration timer, Runnable task) {
    nioLooper.registerTimerEvent(timer, task);
  }

  /////////////////////////////////////////////////////////
  // This is the interface that needs to be implemented by
  // all Heron Servers.
  /////////////////////////////////////////////////////////

  // What action do you want to take when a new client
  // connects to you.
  public abstract void onConnect(SocketChannel channel);

  // What action do you want to take when you get a new
  // request from a particular client
  public abstract void onRequest(REQID rid, SocketChannel channel, Message request);

  // What action do you want to take when you get a new
  // message from a particular client
  public abstract void onMessage(SocketChannel channel, Message message);

  // What action do you want to take when a client
  // closes its connection to you.
  public abstract void onClose(SocketChannel channel);

  /////////////////////////////////////////////////////////
  // Following protected methods are just used for testing
  /////////////////////////////////////////////////////////
  public Map<String, Message.Builder> getMessageMap() {
    return messageMap;
  }

  public Map<String, Message.Builder> getRequestMap() {
    return requestMap;
  }

  public ServerSocketChannel getAcceptChannel() {
    return acceptChannel;
  }

  public Map<SocketChannel, SocketChannelHelper> getActiveConnections() {
    return activeConnections;
  }
}
