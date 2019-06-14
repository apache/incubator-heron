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

////////////////////////////////////////////////////////////////////////////////
// Implements the HTTPServer class. See httpserver.h for details on the API
////////////////////////////////////////////////////////////////////////////////

#include "network/httpserver.h"
#include <evhttp.h>
#include "glog/logging.h"
#include "basics/basics.h"

// 'C' style callback for evhttpd callbacks
void HTTPServerRequestCallback(struct evhttp_request* _request, void* _arg) {
  HTTPServer* server = reinterpret_cast<HTTPServer*>(_arg);
  server->HandleHTTPRequest(_request);
}

HTTPServer::HTTPServer(std::shared_ptr<EventLoop> eventLoop, const NetworkOptions& _options) {
  eventLoop_ = eventLoop;
  options_ = _options;
  http_ = evhttp_new(eventLoop->dispatcher());
  evhttp_set_gencb(http_, &HTTPServerRequestCallback, this);
  generic_cb_ = NULL;
}

HTTPServer::~HTTPServer() { evhttp_free(http_); }

sp_int32 HTTPServer::Start() {
  sp_string host = options_.get_host();
  sp_int32 port = options_.get_port();

  LOG(INFO) << "Starting Http Server bound to "
            << "0.0.0.0"
            << ":" << port;
  // Bind to INADDR_ANY instead of using the hostname
  sp_int32 retval = evhttp_bind_socket(http_, "0.0.0.0", port);
  if (retval == 0) {
    // record the successfully bound hostport
    hostports_.push_back(std::make_pair(host, port));
  } else {
    // failed to bind on the socket
    return retval;
  }
  evhttp_set_max_body_size(http_, options_.get_max_packet_size());
  return SP_OK;
}

void HTTPServer::InstallCallBack(const sp_string& _uri, VCallback<IncomingHTTPRequest*> cb) {
  cbs_[_uri] = std::move(cb);
}

void HTTPServer::InstallGenericCallBack(VCallback<IncomingHTTPRequest*> cb) {
  generic_cb_ = std::move(cb);
}

void HTTPServer::HandleHTTPRequest(struct evhttp_request* _request) {
  IncomingHTTPRequest* request = new IncomingHTTPRequest(_request);

  VCallback<IncomingHTTPRequest*> cb = NULL;
  if (cbs_.find(request->GetQuery()) == cbs_.end()) {
    cb = generic_cb_;
  } else {
    cb = cbs_[request->GetQuery()];
  }
  if (!cb) {
    delete request;
    evhttp_send_error(_request, HTTP_NOTFOUND, "Not Supported");
    return;
  } else {
    cb(request);
  }
}

void HTTPServer::SendReply(IncomingHTTPRequest* _request, sp_int32 _code,
                           unique_ptr<OutgoingHTTPResponse> _response) {
  CHECK(_request->underlying_request() == _response->underlying_response());
  evhttp_send_reply(_request->underlying_request(), _code, "", NULL);
}

void HTTPServer::SendErrorReply(IncomingHTTPRequest* _request, sp_int32 _code) {
  evhttp_send_error(_request->underlying_request(), _code, "");
}

void HTTPServer::SendErrorReply(IncomingHTTPRequest* _request, sp_int32 _code,
                                const sp_string& _reason) {
  evhttp_send_error(_request->underlying_request(), _code, _reason.c_str());
}
