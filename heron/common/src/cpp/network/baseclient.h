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
// This file defines the BaseClient class.
// Each type of Heron client inherits from this class. The various
// derivations differ in what sort of protocol they speak. The only
// implementation is the Client class that speaks the Heron
// ProtocolBuffer Packets.
// The BaseClient knows how to bind and listen to a particular port.
// It tries to connect to a destination server and calls various
// callbacks like HandleConnect_Base and HandleClose_Base when
// the connection connects/closes.
// Derived classes differ in what sort of connections they init, and
// how requests are handled. Please see client.h
// for details on how to use the BaseClient class. No other classes should
// derive from the BaseClient class.
///////////////////////////////////////////////////////////////////////////////

#ifndef BASECLIENT_H_
#define BASECLIENT_H_

#include <functional>
#include "network/baseconnection.h"
#include "network/event_loop.h"
#include "network/networkoptions.h"
#include "network/network_error.h"
#include "basics/basics.h"

/*
 * BaseClient class definition
 */
class BaseClient {
 public:
  enum State { DISCONNECTED = 0, CONNECTING, CONNECTED };

  // Constructor/Destructor
  // Note that constructor doesn't do much beyond initializing some members.
  // Users must explicitly invoke the Start method to be able to send requests
  // and receive responses.
  BaseClient(std::shared_ptr<EventLoop> eventLoop, const NetworkOptions& options);
  virtual ~BaseClient();

  // This starts the connect opereation.
  // A return of this function doesnt mean that the client is ready to go.
  // It just means that the connect operation is proceeding and we
  // will be informed with the HandleConnect_Base method how things went.
  void Start_Base();

  // This one closes the underlying connection. No new responses will
  // be delivered to the client after this call. The set of existing
  // requests may not be sent by the client depending on whether they
  // have already been sent out of wire or not. You might receive
  // error notifications via the HandleSentRequest.
  // A return from this doesn't mean that the underlying sockets have been
  // closed. Merely that the process has begun. When the actual close
  // happens, HandleClose_Base will be called.
  void Stop_Base();

  // Add a timer to be invoked after msecs microseconds. Returns the timer_id
  sp_int64 AddTimer_Base(VCallback<> cb, sp_int64 msecs);
  // Removes a given timer with timer_id
  sp_int32 RemoveTimer_Base(sp_int64 timer_id);

  // return the client options.
  const NetworkOptions& get_clientoptions() const { return options_; }

 protected:
  // Instantiate a new connection
  virtual BaseConnection* CreateConnection(ConnectionEndPoint* endpoint, ConnectionOptions* options,
                                           std::shared_ptr<EventLoop> eventLoop) = 0;

  // Derived class should implement this method to handle Connection
  // establishment. a status of OK implies that the Client was
  // successful in connecting to hte client. Requests can now be sent to
  // the server. Any other status implies that the connect failed.
  virtual void HandleConnect_Base(NetworkErrorCode status) = 0;

  // When the underlying socket is closed(either because of an explicit
  // Stop done by derived class or because a read/write failed and
  // the connection closed automatically on us), this method is
  // called. A status of OK means that this was a user initiated
  // Close that successfully went through. A status value of
  // READ_ERROR implies that there was problem reading in the
  // connection and thats why the connection was closed internally.
  // A status value of WRITE_ERROR implies that there was a problem writing
  // in the connection. Derived classes can do any cleanups in this method.
  virtual void HandleClose_Base(NetworkErrorCode status) = 0;

  // The options of this client.
  NetworkOptions options_;

  // The connection represents our link to the server. All reads/writes are
  // done thru it.
  BaseConnection* conn_;
  State state_;

  // The underlying EventLoop
  std::shared_ptr<EventLoop> eventLoop_;

 private:
  // Helper function that inits various objects
  void Init(std::shared_ptr<EventLoop> eventLoop, const NetworkOptions& _options);

  // Internal method to be called by the Connection class when a
  // connect happens.
  void OnConnect(ConnectionEndPoint* endpoint, EventLoop::Status status);

  // Internal method to be called by the Connection class when a close happens.
  void OnClose(NetworkErrorCode status);

  // When the timer event is called by the EventLoop
  void OnTimer(VCallback<> cb, EventLoop::Status status);

  // When we create a Connection structure, we use the following options
  ConnectionOptions connection_options_;
};

#endif  // BASECLIENT_H_
