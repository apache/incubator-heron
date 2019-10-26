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

struct bufferevent;

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
 */
struct ConnectionOptions {
  sp_uint32 max_packet_size_;
  sp_int64 high_watermark_;
  sp_int64 low_watermark_;
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

  BaseConnection(ConnectionEndPoint* _endpoint, ConnectionOptions* _options,
          std::shared_ptr<EventLoop> eventLoop);
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

  /**
   * Gets the total outstanding bytes pending to be sent
   */
  sp_int32 getOutstandingBytes() const;

  /**
   * Set rate limiting (bytes per second). Both arguments should be positive.
   * The function can be called multiple times. Return false when it fails to
   * apply the new rate limit.
   */
  bool setRateLimit(const sp_int64 _read_bps, const sp_int64 _burst_read_bps);

  /**
   * Disable rate limiting.
   */
  void disableRateLimit();

 protected:
  /**
   * Appends the evbuffer _buffer to the output buffer of bufferevent
   * Note that we may not be able to write out immediately.
   * Thus when the function returns, the bytes might not have
   * been sent out, but will be sent later when the socket becomes
   * writable.
   *
   * A return value of:
   *  - 0 indicates the data is successfully appended to be written
   *  - negative indicates some error.
   */
  sp_int32 write(struct evbuffer* _buffer);

  /**
   * Called by the base class when the connection has something readable
   * The derived classes read in the data coming into the connection.
   *
  * A return value of:
   *  - 0 indicates the data is successfully read.
   *  - negative indicates some error.
   */
  virtual sp_int32 readFromEndPoint(bufferevent* _buffer) = 0;

  /**
   * Called by the base class when the amount of data pending
   * in the write buffer falls below a low watermark.
   */
  virtual void releiveBackPressure() = 0;

  // Get the fd
  sp_int32 getConnectionFd() const { return mEndpoint->get_fd(); }

  // Endpoint read registration
  sp_int32 unregisterEndpointForRead();
  sp_int32 registerEndpointForRead();

  // Connection otions.
  ConnectionOptions* mOptions;

 private:
  // friend classes that can access the protected functions
  friend void readcb(struct bufferevent *bev, void *ctx);
  friend void writecb(struct bufferevent *bev, void *ctx);
  friend void eventcb(struct bufferevent *bev, sp_int16 events, void *ctx);

  // Called by readcb above
  void handleRead();

  // Called by writecb above
  void handleWrite();

  // Called by eventcb above
  void handleEvent(sp_int16 events);

  // A Connection can get closed by the connection class itself(because
  // of an io error). This is the method used to do that.
  void internalClose(NetworkErrorCode status);

  // Connect status of this connection
  State mState;

  // The user registered callbacks
  VCallback<NetworkErrorCode> mOnClose;

  // Connection Endpoint
  ConnectionEndPoint* mEndpoint;

  // The underlying event loop
  std::shared_ptr<EventLoop> mEventLoop;
  // The underlying bufferevent
  struct bufferevent* buffer_;

  // The config for rate limit (bytes per second) on read
  sp_int64 read_bps_;
  sp_int64 burst_read_bps_;
  struct ev_token_bucket_cfg* rate_limit_cfg_;
};

#endif  // HERON_COMMON_SRC_CPP_NETWORK_BASECONNECTION_H_
