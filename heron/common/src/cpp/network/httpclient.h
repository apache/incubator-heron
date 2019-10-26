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
// This file defines the HTTPClient class.
// HTTPClient is actually a misnomer, this is actually an httpclientpool
// managing multiple simultaneous requests to multiple hosts. Takes care of
// very rudimentary hostload stuff by establishing just one connection
// to a particular host at a time.
// Users just use the client as follows
// HTTPClient* client = new HTTPClient(...);
// OutgoingHTTPRequest* request = new OutgoingHTTPRequest(...);
// .....
// Fill in the request
// ...
// CallBack1<IncomingHTTPResponse*>* cb = CreateCallback(...)
// client->SendRequest(request, cb)
//
// The Callback cb is called once the request completes.
// void cb(IncomingHTTPResponse* response)
// {
//   if (response->GetResponseCode() == 200) {
//     sp_string body = response->GetResponseBody();
//   }
//   Error processing
// }
//
// HTTPClient supports Asynchronous dns resolution as well.
//
///////////////////////////////////////////////////////////////////////////////

#ifndef HTTPCLIENT_H_
#define HTTPCLIENT_H_

#include <functional>
#include <unordered_map>
#include <utility>
#include "network/asyncdns.h"
#include "network/httputils.h"
#include "network/event_loop.h"
#include "basics/basics.h"

// Forward Declarations
struct evhttp_connection;
class IncomingHTTPResponse;
class IncomingHTTPResponse;

class HTTPClient {
 public:
  // Constructor
  HTTPClient(std::shared_ptr<EventLoop> eventLoop, AsyncDNS* _dns);

  // Destructor.
  virtual ~HTTPClient();

  //! The main interface of the client
  //! _request now belongs to the HTTPClient. Callers
  // should not touch it after this call.
  // After the request completes, the callback cb is
  // called with the response structure. The IncomingHTTPResponse
  // strucuture now belongs to the callee and should be deleted
  // A return value of LX_OK means the request was queued for sending.
  // It doesnt mean that the request was sent. When the request completes
  // and the response arrives, the callback _cb will be called.
  sp_int32 SendRequest(OutgoingHTTPRequest* _request, VCallback<IncomingHTTPResponse*> _cb);

  // get the underlying select server
  std::shared_ptr<EventLoop> getEventLoop() { return eventLoop_; }

 private:
  //! A structure used for internal purposes
  struct Combo {
    Combo(OutgoingHTTPRequest* _request, HTTPClient* _client) {
      request_ = _request;
      client_ = _client;
    }
    ~Combo() {}
    OutgoingHTTPRequest* request_;
    HTTPClient* client_;
  };

  //! A Callback called when the http request gets done
  void HandleRequestDone(OutgoingHTTPRequest* _req, IncomingHTTPResponse* _response);

  //! A Callback called when a http connection closes
  void HandleConnectionClose(struct evhttp_connection* _connection);

  //! Utility function to create the connection
  struct evhttp_connection* CreateConnection(const sp_string& _host, sp_int32 _port);

  //! Utility function to fill the request
  struct evhttp_request* CreateUnderlyingRequest(OutgoingHTTPRequest* _req, Combo* _combo);

  // Map from the host/port pair to the connections
  std::unordered_map<std::pair<sp_string, sp_int32>, struct evhttp_connection*> connections_;
  // Map from the connections to host/port pair
  std::unordered_map<void*, std::pair<sp_string, sp_int32>> rconnections_;

  // Map from the OutgoingHTTPRequest to the callback
  std::unordered_map<void*, VCallback<IncomingHTTPResponse*>> inflight_urls_;

  //! the EventLoop and the aysnc dns pointers
  std::shared_ptr<EventLoop> eventLoop_;
  AsyncDNS* dns_;

  //! friend functions
  friend void httpdonecb(struct evhttp_request*, void*);
  friend void httpconnectionclose(struct evhttp_connection*, void*);
};

#endif  // HTTPCLIENT_H_
