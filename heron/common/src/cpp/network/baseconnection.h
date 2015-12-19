/*
 * Copyright 2015 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

///////////////////////////////////////////////////////////////////////////////
// This defines a very thin interface for basic functions that any connection
// should support. See connection.h for actual
// examples.
///////////////////////////////////////////////////////////////////////////////

#ifndef HERON_COMMON_SRC_CPP_NETWORK_BASECONNECTION_H_
#define HERON_COMMON_SRC_CPP_NETWORK_BASECONNECTION_H_

#include <sys/un.h>
#include <arpa/inet.h>
#include <functional>
#include "basics/basics.h"
#include "network/event_loop.h"
#include "network/network_error.h"

/**
 * This denotes the endpoint of an connection. It consists of the socket
 * address of the endpoint as well as the file descriptor that we use
 * for read/writes.
 */
class ConnectionEndPoint {
 public:
  explicit ConnectionEndPoint(bool _unix_socket) {
    fd_ = -1;
    unix_socket_ = _unix_socket;
  }
  struct sockaddr* addr() {
    if (unix_socket_) {
      return (struct sockaddr*)&un_addr_;
    } else {
      return (struct sockaddr*)&in_addr_;
    }
  }
  socklen_t addrlen() {
    if (unix_socket_)
      return sizeof(un_addr_);
    else
      return sizeof(in_addr_);
  }
  void set_fd(sp_int32 _fd) { fd_ = _fd; }
  sp_int32 get_fd() { return fd_; }
  bool is_unix_socket() const { return unix_socket_; }

 private:
  sp_int32 fd_;
  bool unix_socket_;
  struct sockaddr_in in_addr_;
  struct sockaddr_un un_addr_;
};

/**
 * Options that the server passes to the Connection.
 * Currently we just have the maximum packet size allowed.
 */
struct ConnectionOptions {
  sp_uint32 max_packet_size_;
};

/*
 * An abstract base class to represent a network connection between 2 endpoints.
 * Provides support for non-blocking reads and writes.
 */
class BaseConnection {
 public:
  // The state of the connection
  enum State {
    // This is the state of the connection when its created.
    INIT = 0,
    // Connected. Read/Write going fine
    CONNECTED,
    // socket disconnected. No read writes happening
    DISCONNECTED,
    // socket is marked to be disconnected.
    TO_BE_DISCONNECTED,
  };

  // Whether a read/write would block?
  enum ReadWriteState { NOTREGISTERED, READY, NOTREADY, ERROR };

  BaseConnection(ConnectionEndPoint* _endpoint, ConnectionOptions* _options, EventLoop* eventLoop);
  virtual ~BaseConnection();

  /**
   * Start the connection. Should only be called when in INIT state.
   * Upon success, moves the state from INIT to CONNECTED.
   * Return 0 -> success (ready to send/receive packets)
   * Return -1 -> failure.
   *
   * TODO (vikasr): return the reason for failure (error codes)
   */
  sp_int32 start();

  /**
   * Close the connection. This will disable the connection from receiving and sending the
   * packets. This will also close the underlying socket structures and invoke a close callback
   * if any was registered. Moves the state to DISCONNECTED.
   */
  void closeConnection();

  /**
   * Register a callback to be called upon connection close
   */
  void registerForClose(VCallback<NetworkErrorCode> cb);

  sp_string getIPAddress();

  sp_int32 getPort();

 protected:
  /**
   * Writes data out on this connection
   * This method is called by the base class if the connection fd is
   * registeredForWrite and it is writable.
   *
   * A return value of:
   *  - 0 indicates the data is successfully written.
   *  - negative indicates some error.
   */
  virtual sp_int32 writeIntoEndPoint(sp_int32 _fd) = 0;

  /**
   * A way for base class to know if the derived class still has data to be written.
   * This is called after WriteIntoEndPoint.
   * If true, base class will registerForWrite until this method returns false.
   */
  virtual bool stillHaveDataToWrite() = 0;

  /**
   * Called after WriteIntoEndPoint is successful.
   * Usually meant for cleaning up packets that have been sent.
   */
  virtual void handleDataWritten() = 0;

  /**
   * Called by the base class when the connection fd is readable.
   * The derived classes read in the data coming into the connection.
   *
  * A return value of:
   *  - 0 indicates the data is successfully read.
   *  - negative indicates some error.
   */
  virtual sp_int32 readFromEndPoint(sp_int32 _fd) = 0;

  /**
   * Called after ReadFromEndPoint is successful.
   * Meant for processing packets that have been received.
   */
  virtual void handleDataRead() = 0;

  /*
   * Derived class calls this method when there is data to be sent over the connection.
   * Base class will registerForWrite on the connection fd to be notified when it is writable.
   */
  sp_int32 registerForWrite();

  // Get the fd
  sp_int32 getConnectionFd() const { return mEndpoint->get_fd(); }

  // Endpoint read registration
  sp_int32 unregisterEndpointForRead();
  sp_int32 registerEndpointForRead();

  // Connection otions.
  ConnectionOptions* mOptions;

 private:
  // Internal callback that is invoked when a read event happens on a
  // connected sate.
  void handleRead(EventLoop::Status);

  // Internal callback that is invoked when a write event happens on a
  // connected sate. In this routine we actually send the packets out.
  void handleWrite(EventLoop::Status);

  // A Connection can get closed by the connection class itself(because
  // of an io error). This is the method used to do that.
  void internalClose();

  // Connect status of this connection
  State mState;

  // Whether we are ready to read.
  ReadWriteState mReadState;

  // Whether we are ready to write.
  ReadWriteState mWriteState;

  // The user registered callbacks
  VCallback<NetworkErrorCode> mOnClose;

  // Our own callbacks that we register for internal reads/write events.
  VCallback<EventLoop::Status> mOnRead;
  VCallback<EventLoop::Status> mOnWrite;

  // Connection Endpoint
  ConnectionEndPoint* mEndpoint;

  // The underlying event loop
  EventLoop* mEventLoop;
  bool mCanCloseConnection;
};

#endif  // HERON_COMMON_SRC_CPP_NETWORK_BASECONNECTION_H_
