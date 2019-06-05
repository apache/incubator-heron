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
//
// This file defines the BaseServer class.
// Every type of heron server inherits from the BaseServer class.
// The various derivations differ in what sort of protocol they speak.
// Currently only one derived server class exists that all heron
// servers use. That is defined in server.h and uses protocol messages
// to communicate
// The BaseServer class knows how to bind and listen on a particular port.
// It opens up a new Connection when a client connects to that port. It
// catches connection closes and calls HandleConnectionClose_Base.
// Please see server.h for details on how to use the Server class.
//
// NOTE:- No one should derive from
// the BaseServer class outside the network directory.
///////////////////////////////////////////////////////////////////////////////

#ifndef BASESERVER_H_
#define BASESERVER_H_

#include <functional>
#include <unordered_set>
#include "network/baseconnection.h"
#include "network/event_loop.h"
#include "network/networkoptions.h"
#include "network/network_error.h"
#include "basics/basics.h"

namespace std {
template <>
struct hash<BaseConnection*> {
  size_t operator()(BaseConnection* const& x) const {
    hash<void*> h;
    return h(reinterpret_cast<void*>(x));
  }
};
}

class PCThread_Group;

class BaseServer {
 public:
  // Constructor
  // The Constructor simply inits the member variable.
  // Users must call Start method to start sending/receiving packets.
  BaseServer(std::shared_ptr<EventLoop> eventLoop, const NetworkOptions& options);

  // Destructor.
  virtual ~BaseServer();

  // Start listening on the host port pair for new requests
  // A zero return value means success. A negative value implies
  // that the server could not bind/listen on the port.
  // If the port is 0, the actual port is stored in options_
  // after Start_Base() called, which can be fetched through
  // get_serveroptions()
  sp_int32 Start_Base();

  // Close all active connections and stop listening.
  // A zero return value means success. No more new connections
  // will be accepted from this point onwards. All active
  // connections will be closed. This might result in responses
  // that were sent using SendResponse but not yet acked by HandleSentResponse
  // being discarded.
  // A negative return value implies some error happened.
  sp_int32 Stop_Base();

  // Close a connection. This function doesn't return anything.
  // When the connection is attempted to be closed(which can happen
  // at a later time if using thread pool), The HandleConnectionClose
  // will contain a status of how the closing process went.
  void CloseConnection_Base(BaseConnection* connection);

  // Add a timer function to be called after msecs microseconds.
  void AddTimer_Base(VCallback<> cb, sp_int64 msecs);

  const NetworkOptions& get_serveroptions() const { return options_; }

  // friend functions
  friend void CallHandleConnectionCloseAndDelete(BaseServer*, BaseConnection*, NetworkErrorCode);

 protected:
  // Instantiate a new Connection
  virtual BaseConnection* CreateConnection(ConnectionEndPoint* endpoint, ConnectionOptions* options,
                                           std::shared_ptr<EventLoop> eventLoop) = 0;

  // Called when a new connection is accepted.
  virtual void HandleNewConnection_Base(BaseConnection* newConnection) = 0;

  // Called when a connection is closed.
  // The connection object must not be used by the application after this call.
  virtual void HandleConnectionClose_Base(BaseConnection* connection, NetworkErrorCode _status) = 0;

  // The underlying EventLoop
  std::shared_ptr<EventLoop> eventLoop_;

  // The set of active connections
  std::unordered_set<BaseConnection*> active_connections_;

 private:
  // Internal helper function to initialize things
  void Init(std::shared_ptr<EventLoop> eventLoop, const NetworkOptions& options);

  // Internal method to be called when a write event happens on listen_fd_
  void OnNewConnection(EventLoop::Status status);

  // When a Connection closes, this is invoked by the Connection
  void OnConnectionClose(BaseConnection* connection, NetworkErrorCode status);

  // When EventLoop invokes upon a timer
  void OnTimer(VCallback<> cb, EventLoop::Status status);

  // Internal functions which do most of the API related activities in the
  // main thread
  void InternalCloseConnection(BaseConnection* _connection);
  void InternalAddTimer(VCallback<> cb, sp_int64 msecs);

  // The socket that we are listening on
  sp_int32 listen_fd_;

  // When we create a Connection structure, we use the following options
  ConnectionOptions connection_options_;

  // The options of this server.
  // If the port is 0, the actual port can be fetched through
  // get_serveroptions() after Start_Base() called.
  NetworkOptions options_;

  // Placeholders for various callbacks that we pass to the Connection.
  // They are kept here so that they can be cleaned up in the end
  VCallback<EventLoop::Status> on_new_connection_callback_;
};

#endif  // BASESERVER_H_
