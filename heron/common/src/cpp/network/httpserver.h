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
// This file defines the HTTPServer class. This is just a thin wrapper
// around the libevent http functions.
///////////////////////////////////////////////////////////////////////////////

#ifndef HTTPSERVER_H_
#define HTTPSERVER_H_

#include <functional>
#include <unordered_map>
#include <utility>
#include <vector>
#include "network/httputils.h"
#include "network/event_loop.h"
#include "network/networkoptions.h"
#include "basics/basics.h"

class IncomingHTTPRequest;
class OutgoingHTTPResponse;

using std::unique_ptr;

class HTTPServer {
 public:
  // Constructor
  // The Constructor simply inits the member variable.
  // Users must call Start method to start sending/receiving packets.
  HTTPServer(std::shared_ptr<EventLoop> eventLoop, const NetworkOptions& options);

  // Destructor.
  virtual ~HTTPServer();

  // binds the server on the host/port in the ServerOptions, then
  // sets the underlying EventLoop to listen via Loop().
  sp_int32 Start();

  // When a request for url comes in call this callback with the request.
  void InstallCallBack(const sp_string& _url, VCallback<IncomingHTTPRequest*> cb);
  void InstallGenericCallBack(VCallback<IncomingHTTPRequest*> cb);

  void SendReply(IncomingHTTPRequest* _request, sp_int32 status_code,
                 unique_ptr<OutgoingHTTPResponse> _response);
  void SendErrorReply(IncomingHTTPRequest* _request, sp_int32 status_code);
  void SendErrorReply(IncomingHTTPRequest* _request, sp_int32 status_code,
                      const sp_string& _reason);

  // Accessors
  std::shared_ptr<EventLoop> getEventLoop() { return eventLoop_; }

 private:
  void HandleHTTPRequest(struct evhttp_request* _request);
  void HandleGenericHTTPRequest(struct evhttp_request* _request);

  // The underlying http server from libevent
  struct evhttp* http_;
  std::vector<std::pair<sp_string, sp_int32>> hostports_;
  std::unordered_map<sp_string, VCallback<IncomingHTTPRequest*>> cbs_;
  VCallback<IncomingHTTPRequest*> generic_cb_;
  NetworkOptions options_;
  std::shared_ptr<EventLoop> eventLoop_;

  // friend declarations
  friend void HTTPServerRequestCallback(struct evhttp_request*, void*);
};

#endif  // HTTPSERVER_H_
