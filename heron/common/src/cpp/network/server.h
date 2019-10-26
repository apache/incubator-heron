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
// This file defines the Server class.
// Server is a Base class for any heron server. The application server
// derives from the base class by implementing a bunch of virtual methods
// dealing with what to do when a new connection appears, a new packet appears
// and so on.
// The Server class knows how to bind and listen on a particular port.
// It opens up a new Connection when a client connects to that port. It reads
// new requests off of the Connection and passes it on to the Derived class
// via the virtual methods. It also carries responses from the Derived class
// thru the Connection class to the remote client.
// An application Server derives from the Server class and has to implement
// the following methods.
// 1. HandleNewConnection:- When a new connection is established with a remote
//                          client, the server calls this method. Server can
//                          send greeting message and/or establish some state
//                          associated with the particular connection
// 2. HandleConnectionClose:- When a client connection closes(either because
//                          the client hung up, or we explicitly closed the
//                          connection or because of a read/write error, this
//                          virtual method is called. Application servers can
//                          cleanup stuff assoicated with the particular
//                          Connection in this method.
// 3. HandleNewRequest:- This method is called upon receipt of a new Packet
//                       from the server. Application server can parse the
//                       request in this method and queue it for further
//                       processing or send back response.
// 4. HandleSentResponse:- This method is called after a response that is
//                         sent by the Application server using the
//                         SendResponse method is finally sent. Application
//                         server can examine the status code that is sent
//                         in this message to see if the response was sent
//                         properly or not and update their state.
// Application servers can use the SendResponse method to send a
// message/response to the client. They can terminate the connection using
// the CloseConnection method.
// The server class can be used as both a single threaded server or with a
// thread pool to service requests. In case where a thread pool is specified,
// the server will run HandleNewConnection, HandleConnectionClose,
// HandleNewRequest and HandleSentResponse in the thread pool.
// An example server can be seen at network/tests/echoserver.h/cpp
///////////////////////////////////////////////////////////////////////////////
#ifndef SERVER_H_
#define SERVER_H_

#include <google/protobuf/message.h>
#include <google/protobuf/repeated_field.h>
#include <functional>
#include <iostream>
#include <string>
#include <unordered_map>
#include <utility>
#include <typeindex>
#include <list>
#include "basics/basics.h"
#include "glog/logging.h"
#include "network/connection.h"
#include "network/baseserver.h"
#include "network/baseconnection.h"
#include "network/event_loop.h"
#include "network/networkoptions.h"
#include "network/network_error.h"
#include "network/packet.h"

using std::unique_ptr;

/*
 * Server class definition
 * Given a host/port, the server binds and listens on that host/port
 * It calls various virtual methods when events happen. The events are
 * HandleNewConnection:- Upon a new connection accept, we invoke
 *                       this method. In this method derived classes can
 *                       send greeting messages, close the connection, etc
 * HandleConnectionClose:- We call this function after closing the connection.
 *                         Derived classes can cleanup stuff in this method
 * HandleNewRequest:- Whenever a new packet arrives, we call this method.
 *                   Derived classes can parse stuff in this method.
 * HandleSentResponse:- Whenever we send a response to the client, we call
 *                   this method. Derived classes can update their state, etc.
 * Derived classes can use the SendResponse method to send a packet.
 * They can use the CloseConnection to explicitly close a connection.
 * Note that during this method, the Server will call the
 * HandleConnectionClose as well.
 */
class Server : public BaseServer {
 public:
  // Constructor
  // The Constructor simply inits the member variable.
  // Users must call Start method to start sending/receiving packets.
  Server(std::shared_ptr<EventLoop> eventLoop, const NetworkOptions& options);

  // Destructor.
  virtual ~Server();

  // Start listening on the host port pair for new requests
  // A zero return value means success. A negative value implies
  // that the server could not bind/listen on the port.
  sp_int32 Start();

  // Close all active connections and stop listening.
  // A zero return value means success. No more new connections
  // will be accepted from this point onwards. All active
  // connections will be closed. This might result in responses
  // that were sent using SendResponse but not yet acked by HandleSentResponse
  // being discarded.
  // A negative return value implies some error happened.
  sp_int32 Stop();

  // Send a response back to the client. This is the primary way of
  // communicating with the client.
  // When the method returns it doesn't mean that the packet was sent out.
  // but that it was merely queued up. Server now owns the response object
  void SendResponse(REQID id, Connection* connection, const google::protobuf::Message& response);

  // Send a message to initiate a non request-response style communication
  // message is now owned by the Server class
  void SendMessage(Connection* connection, const google::protobuf::Message& message);

  void SendMessage(Connection* _connection,
                   sp_int32 _byte_size,
                   const sp_string _type_name,
                   const char* _message);

  // Close a connection. This function doesn't return anything.
  // When the connection is attempted to be closed(which can happen
  // at a later time if using thread pool), The HandleConnectionClose
  // will contain a status of how the closing process went.
  void CloseConnection(Connection* connection);

  // Add a timer function to be called after msecs microseconds.
  void AddTimer(VCallback<> cb, sp_int64 msecs);

  // Register a handler for a particular request type
  template <typename T, typename M>
  void InstallRequestHandler(void (T::*method)(REQID id, Connection* conn, pool_unique_ptr<M>)) {
    unique_ptr<google::protobuf::Message> m = make_unique<M>();
    T* t = static_cast<T*>(this);
    requestHandlers[m->GetTypeName()] = std::bind(&Server::dispatchRequest<T, M>, this, t, method,
                                                  std::placeholders::_1, std::placeholders::_2);
  }

  // Register a handler for a particular message type
  template <typename T, typename M>
  void InstallMessageHandler(void (T::*method)(Connection* conn, pool_unique_ptr<M>)) {
    unique_ptr<google::protobuf::Message> m = make_unique<M>();
    T* t = static_cast<T*>(this);
    messageHandlers[m->GetTypeName()] = std::bind(&Server::dispatchMessage<T, M>, this, t, method,
                                                  std::placeholders::_1, std::placeholders::_2);
  }

  // One can also send requests to the client
  void SendRequest(Connection* _conn, google::protobuf::Message* _request, void* _ctx,
                   google::protobuf::Message* _response_placeholder);
  void SendRequest(Connection* _conn, google::protobuf::Message* _request, void* _ctx,
                   sp_int64 _msecs, google::protobuf::Message* _response_placeholder);

  // Backpressure handler
  virtual void StartBackPressureConnectionCb(Connection* connection);
  // Backpressure Reliever
  virtual void StopBackPressureConnectionCb(Connection* _connection);

  // Return the underlying EventLoop.
  std::shared_ptr<EventLoop> getEventLoop() { return eventLoop_; }

 protected:
  // Called when a new connection is accepted.
  virtual void HandleNewConnection(Connection* newConnection) = 0;

  // Called when a connection is closed.
  // The connection object must not be used by the application after this call.
  virtual void HandleConnectionClose(Connection* connection, NetworkErrorCode _status) = 0;

  // Handle the responses for any sent requests
  // We provide a basic handler that just deletes the response
  virtual void HandleResponse(google::protobuf::Message* _response, void* _ctx,
                              NetworkErrorCode _status);

 public:
  // The interfaces implemented of the BaseServer

  // Create the connection
  BaseConnection* CreateConnection(ConnectionEndPoint* endpoint, ConnectionOptions* options,
                                   std::shared_ptr<EventLoop> ss);

  // Called when connection is accepted
  virtual void HandleNewConnection_Base(BaseConnection* newConnection);

  // Called when the connection is closed
  virtual void HandleConnectionClose_Base(BaseConnection* connection, NetworkErrorCode _status);

 private:
  // When a new packet arrives on the connection, this is invoked by the Connection
  void OnNewPacket(Connection* connection, IncomingPacket* packet);

  void InternalSendResponse(Connection* _connection, OutgoingPacket* _packet);

  template <typename T, typename M>
  void dispatchRequest(T* _t, void (T::*method)(REQID id, Connection* conn, pool_unique_ptr<M>),
                       Connection* _conn,
                       IncomingPacket* _ipkt) {
    REQID rid;
    CHECK(_ipkt->UnPackREQID(&rid) == 0) << "REQID unpacking failed";

    auto m = make_unique_from_protobuf_pool<M>();

    if (_ipkt->UnPackProtocolBuffer(m.get()) != 0) {
      // We could not decode the pb properly
      std::cerr << "Could not decode protocol buffer of type " << m->GetTypeName();
      CloseConnection(_conn);
      return;
    }

    CHECK(m->IsInitialized());

    auto cb = std::bind(method, _t, rid, _conn, std::placeholders::_1);

    cb(std::move(m));
  }

  template <typename T, typename M>
  void dispatchMessage(T* _t, void (T::*method)(Connection* conn, pool_unique_ptr<M>),
                       Connection* _conn,
                       IncomingPacket* _ipkt) {
    REQID rid;
    CHECK(_ipkt->UnPackREQID(&rid) == 0) << "REQID unpacking failed";

    auto m = make_unique_from_protobuf_pool<M>();

    if (_ipkt->UnPackProtocolBuffer(m.get()) != 0) {
      // We could not decode the pb properly
      std::cerr << "Could not decode protocol buffer of type " << m->GetTypeName();
      CloseConnection(_conn);
      return;
    }

    auto cb = std::bind(method, _t, _conn, std::placeholders::_1);

    cb(std::move(m));
  }

  void InternalSendRequest(Connection* _conn, google::protobuf::Message* _request, sp_int64 _msecs,
                           google::protobuf::Message* _response_placeholder, void* _ctx);
  void OnPacketTimer(REQID _id, EventLoop::Status status);

  typedef std::function<void(Connection*, IncomingPacket*)> handler;
  std::unordered_map<std::string, handler> requestHandlers;
  std::unordered_map<std::string, handler> messageHandlers;

  // For acting like a client
  std::unordered_map<REQID, std::pair<google::protobuf::Message*, void*> > context_map_;
  REQID_Generator* request_rid_gen_;
};

#endif  // SERVER_H_
