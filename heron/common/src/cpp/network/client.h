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
// This file defines the Client class. All Heron clients should use the
// Client class to talk to other Heron servers. The Client class does the
// connection establishment/teardown, sending requests and receiving responses.
// Application classes derive from the Client class and implement methods
// to deal with connection establishment/teardown, query response handling
// and sending requests.
//
///////////////////////////////////////////////////////////////////////////////

#ifndef CLIENT_H_
#define CLIENT_H_

#include <google/protobuf/message.h>
#include <google/protobuf/repeated_field.h>
#include <functional>
#include <iostream>
#include <string>
#include <unordered_map>
#include <utility>
#include <list>
#include <typeindex>
#include "basics/basics.h"
#include "glog/logging.h"
#include "network/connection.h"
#include "network/baseclient.h"
#include "network/baseconnection.h"
#include "network/event_loop.h"
#include "network/networkoptions.h"
#include "network/network_error.h"
#include "network/packet.h"

using std::unique_ptr;

/*
 * Client class definition
 * Given a host/port, the client tries to connect to the host port.
 * It calls various virtual methods when events happen. The events are
 * HandleConnect:- When the connect process completes, this method is invoked.
 *                 Applications can send a greeting message, etc.
 * HandleClose:- We call this function when the underlying connection closes.
 *               A connection can close either because the user explictly
 *               did a close using the Stop API or because of a read/write
 *               error encountered by the underlying connection.
 *               Derived classes can cleanup stuff in this method
 * HandleResponse:- Whenever a response from the server arrives, this method
 *                  is called. Derived classes can parse the response and
 *                  use the response in this method.
 * Derived classes can use the SendRequest method to send a request to the
 * server. They can use the Stop to explicitly close a connection.
 */
class Client : public BaseClient {
 public:
  // Constructor/Destructor
  // Note that constructor doesn't do much beyond initializing some members.
  // Users must explicitly invoke the Start method to be able to send requests
  // and receive responses.
  Client(std::shared_ptr<EventLoop> eventLoop, const NetworkOptions& options);
  virtual ~Client();

  // This starts the connect opereation.
  // A return of this function doesnt mean that the client is ready to go.
  // It just means that the connect operation is proceeding and we will
  // be informed with the HandleConnect method how things went.
  void Start();

  // This one closes the underlying connection. No new responses will
  // be delivered to the client after this call. The set of existing
  // requests may not be sent by the client depending on whether they
  // have already been sent out of wire or not. You might receive
  // error notifications via the HandleSentRequest.
  // A return from this doesn't mean that the underlying sockets have been
  // closed. Merely that the process has begun. When the actual close
  // happens, HandleClose will be called.
  void Stop();

  // Send a request to the server with a certain timeout
  // This function doesnt return anything. After this function returns,
  // does not mean that the request actually sent out, merely that the request
  // was successfully queued to be sent out.
  // Actual send occurs when the socket becomes readable and all prev
  // requests are sent. If the packet cannot be sent
  // out or the request is not retired by the client within the timeout
  // period, the HandleResponse is called with the appropriate status.
  // The _request is now owned by the Client class. The ctx is
  // a user owned piece of context that is not interpreted by the
  // client which is passed on to the HandleResponse
  // A negative value of the msecs means no timeout.
  void SendRequest(std::unique_ptr<google::protobuf::Message> _request, void* ctx, sp_int64 msecs);

  // Convinience method of the above function with no timeout
  void SendRequest(std::unique_ptr<google::protobuf::Message> _request, void* ctx);

  // This interface is used if you want to communicate with the other end
  // on a non-request-response based communication.
  void SendMessage(const google::protobuf::Message& _message);

  // Add a timer to be invoked after msecs microseconds. Returns the timer id.
  sp_int64 AddTimer(VCallback<> cb, sp_int64 msecs);
  // Removes a timer with timer_id
  sp_int32 RemoveTimer(sp_int64 timer_id);

  // For server type request handling
  void SendResponse(REQID _id, const google::protobuf::Message& response);

  // Tells if we are connected
  inline bool IsConnected() const { return state_ == CONNECTED; }

  // Tells us if we have caused backpressure
  bool HasCausedBackPressure() const;

  // Register a handler for a particular response type
  template <typename S, typename T, typename M>
  void InstallResponseHandler(unique_ptr<S> _request,
                              void (T::*method)(void* _ctx, pool_unique_ptr<M>,
                              NetworkErrorCode status)) {
    auto m = make_unique<M>();
    T* t = static_cast<T*>(this);
    responseHandlers[m->GetTypeName()] = std::bind(&Client::dispatchResponse<T, M>, this, t, method,
                                                   std::placeholders::_1, std::placeholders::_2);
    requestResponseMap_[_request->GetTypeName()] = m->GetTypeName();
  }

  // Register a handler for a particular message type
  template <typename T, typename M>
  void InstallMessageHandler(void (T::*method)(pool_unique_ptr<M> _message)) {
    google::protobuf::Message* m = new M();
    T* t = static_cast<T*>(this);
    messageHandlers[m->GetTypeName()] =
        std::bind(&Client::dispatchMessage<T, M>, this, t, method, std::placeholders::_1);
    delete m;
  }

  sp_int64 getOutstandingBytes() const {
    if (conn_) {
      return conn_->getOutstandingBytes();
    } else {
      return 0;
    }
  }

  // Return the underlying EventLoop.
  std::shared_ptr<EventLoop> getEventLoop() { return eventLoop_; }

 protected:
  // Derived class should implement this method to handle Connection
  // establishment. a status of OK implies that the Client was
  // successful in connecting to hte client. Requests can now be sent to
  // the server. Any other status implies that the connect failed.
  virtual void HandleConnect(NetworkErrorCode status) = 0;

  // When the underlying socket is closed(either because of an explicit
  // Stop done by derived class or because a read/write failed and
  // the connection closed automatically on us), this method is
  // called. A status of OK means that this was a user initiated
  // Close that successfully went through. A status value of
  // READ_ERROR implies that there was problem reading in the
  // connection and thats why the connection was closed internally.
  // A status value of WRITE_ERROR implies that there was a problem writing
  // in the connection. Derived classes can do any cleanups in this method.
  virtual void HandleClose(NetworkErrorCode status) = 0;

  // friend classes that can access the protected functions
  friend void CallHandleSentRequestAndDelete(Client*, google::protobuf::Message*, void* ctx,
                                             NetworkErrorCode);
  // Backpressure handler
  virtual void StartBackPressureConnectionCb(Connection* connection);
  // Backpressure Reliever
  virtual void StopBackPressureConnectionCb(Connection* _connection);

 private:
  //! Imlement methods of BaseClient
  virtual BaseConnection* CreateConnection(ConnectionEndPoint* endpoint, ConnectionOptions* options,
                                           std::shared_ptr<EventLoop> eventLoop);
  virtual void HandleConnect_Base(NetworkErrorCode status);
  virtual void HandleClose_Base(NetworkErrorCode status);

  //! Handle most of the init stuff
  void Init();

  void InternalSendRequest(std::unique_ptr<google::protobuf::Message> _request, void* _ctx,
          sp_int64 _msecs);
  void InternalSendMessage(const google::protobuf::Message& _message);
  void InternalSendResponse(OutgoingPacket* _packet);

  // Internal method to be called by the Connection class
  // when a new packet arrives
  void OnNewPacket(IncomingPacket* packet);

  // Internal method to be called by the EventLoop class
  // when a packet timer expires
  void OnPacketTimer(REQID _id, EventLoop::Status status);

  template <typename T, typename M>
  void dispatchResponse(T* _t, void (T::*method)(void* _ctx, pool_unique_ptr<M>, NetworkErrorCode),
                        IncomingPacket* _ipkt, NetworkErrorCode _code) {
    void* ctx = nullptr;
    pool_unique_ptr<M> m = nullptr;
    NetworkErrorCode status = _code;
    if (status == OK && _ipkt) {
      REQID rid;
      CHECK(_ipkt->UnPackREQID(&rid) == 0) << "REQID unpacking failed";
      if (context_map_.find(rid) != context_map_.end()) {
        // indeed
        ctx = context_map_[rid].second;
        m = make_unique_from_protobuf_pool<M>();
        context_map_.erase(rid);
        _ipkt->UnPackProtocolBuffer(m.get());
      } else {
        // This is either some unknown message type or the response of an
        // already timed out request
        std::cerr << "Dropping an incoming packet because either the message type is unknown "
                  << " or it was a response for an already timed out request" << std::endl;
        return;
      }
    }

    auto cb = std::bind(method, _t, ctx, std::placeholders::_1, status);

    cb(std::move(m));
  }

  template <typename T, typename M>
  void dispatchMessage(T* _t, void (T::*method)(pool_unique_ptr<M>), IncomingPacket* _ipkt) {
    pool_unique_ptr<M> m = make_unique_from_protobuf_pool<M>();

    if (_ipkt->UnPackProtocolBuffer(m.get()) != 0) {
      // We could not decode the pb properly
      std::cerr << "Could not decode protocol buffer of type " << m->GetTypeName();
      return;
    }

    CHECK(m->IsInitialized());

    auto cb = std::bind(method, _t, std::placeholders::_1);

    cb(std::move(m));
  }

  //! Map from reqid to the response/context pair of the request
  std::unordered_map<REQID, std::pair<sp_string, void*> > context_map_;

  typedef std::function<void(IncomingPacket*)> handler;
  std::unordered_map<std::string, handler> messageHandlers;
  typedef std::function<void(IncomingPacket*, NetworkErrorCode)> res_handler;
  std::unordered_map<std::string, res_handler> responseHandlers;
  std::unordered_map<std::string, std::string> requestResponseMap_;

  // REQID generator
  REQID_Generator* message_rid_gen_;
};

#endif  // CLIENT_H_
