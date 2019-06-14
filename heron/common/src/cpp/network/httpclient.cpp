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
// Implements the HTTPClient class. See httpclient.h for details on the API
////////////////////////////////////////////////////////////////////////////////

#include "network/httpclient.h"
#include <evhttp.h>
#include <utility>
#include "glog/logging.h"
#include "basics/basics.h"

const sp_int32 HTTPClientTimeoutInSecs = 3600;

evhttp_cmd_type GetRequestType(BaseHTTPRequest::HTTPRequestType _type) {
  switch (_type) {
    case BaseHTTPRequest::GET:
      return EVHTTP_REQ_GET;
    case BaseHTTPRequest::POST:
      return EVHTTP_REQ_POST;
    case BaseHTTPRequest::HEAD:
      return EVHTTP_REQ_HEAD;
    case BaseHTTPRequest::PUT:
      return EVHTTP_REQ_PUT;
    case BaseHTTPRequest::DELETE:
      return EVHTTP_REQ_DELETE;
    default:
      CHECK(false) << "Unhandled http request";
      return EVHTTP_REQ_GET;
  }
}

// 'C' style callbacks for the libevent
void httpdonecb(struct evhttp_request* _request, void* _arg) {
  HTTPClient::Combo* combo = static_cast<HTTPClient::Combo*>(_arg);
  HTTPClient* client = combo->client_;
  OutgoingHTTPRequest* request = combo->request_;
  delete combo;
  if (client) {
    auto response = new IncomingHTTPResponse(_request);
    client->HandleRequestDone(request, response);
  }
}

void httpconnectionclose(struct evhttp_connection* _connection, void* _arg) {
  HTTPClient* client = static_cast<HTTPClient*>(_arg);
  client->HandleConnectionClose(_connection);
}

HTTPClient::HTTPClient(std::shared_ptr<EventLoop> eventLoop,
        AsyncDNS* _dns) : eventLoop_(eventLoop), dns_(_dns) {}

HTTPClient::~HTTPClient() {
  for (auto iter = connections_.begin(); iter != connections_.end(); ++iter) {
    evhttp_connection_set_closecb(iter->second, NULL, NULL);
    evhttp_connection_free(iter->second);
  }
}

sp_int32 HTTPClient::SendRequest(OutgoingHTTPRequest* _request,
                                 VCallback<IncomingHTTPResponse*> cb) {
  struct evhttp_connection* connection = NULL;
  std::pair<sp_string, sp_int32> pr = make_pair(_request->host(), _request->port());
  if (connections_.find(pr) == connections_.end()) {
    connection = CreateConnection(_request->host(), _request->port());
    if (connection == NULL) {
      return SP_NOTOK;
    }
    connections_[pr] = connection;
    rconnections_[connection] = pr;
  } else {
    connection = connections_[pr];
  }

  auto combo = new Combo(_request, this);
  struct evhttp_request* httprequest = CreateUnderlyingRequest(_request, combo);
  if (!httprequest) {
    delete combo;
    return SP_NOTOK;
  }
  inflight_urls_[_request] = std::move(cb);
  if (evhttp_make_request(connection, httprequest, GetRequestType(_request->type()),
                          _request->query().c_str()) != 0) {
    inflight_urls_.erase(_request);
    delete combo;
    return SP_NOTOK;
  }
  return SP_OK;
}

void HTTPClient::HandleRequestDone(OutgoingHTTPRequest* _request, IncomingHTTPResponse* _response) {
  if (inflight_urls_.find(_request) == inflight_urls_.end()) {
    // This is strange! We dont have any account for this url.
    LOG(ERROR) << "Unknown HTTPrequest completed\n";
    return;
  }
  VCallback<IncomingHTTPResponse*> cb = std::move(inflight_urls_[_request]);
  inflight_urls_.erase(_request);
  delete _request;
  cb(_response);
}

void HTTPClient::HandleConnectionClose(struct evhttp_connection* _connection) {
  if (rconnections_.find(_connection) == rconnections_.end()) {
    LOG(ERROR) << "Unknown connection closed on us!\n";
    return;
  }
  std::pair<sp_string, sp_int32> pr = rconnections_[_connection];
  connections_.erase(pr);
  rconnections_.erase(_connection);
}

struct evhttp_connection* HTTPClient::CreateConnection(const sp_string& _host, sp_int32 _port) {
  unsigned short port = _port;
  struct evhttp_connection* connection =
      evhttp_connection_base_new(eventLoop_->dispatcher(), dns_->dns(), _host.c_str(), port);
  if (!connection) return NULL;
  evhttp_connection_set_closecb(connection, httpconnectionclose, this);
  evhttp_connection_set_timeout(connection, HTTPClientTimeoutInSecs);
  return connection;
}

struct evhttp_request* HTTPClient::CreateUnderlyingRequest(OutgoingHTTPRequest* _request,
                                                           HTTPClient::Combo* _combo) {
  // First create the libevent structure
  struct evhttp_request* request = evhttp_request_new(httpdonecb, _combo);
  if (!request) {
    return NULL;
  }

  // Next add all reqeusted headers
  const auto& header = _request->header();
  for (auto iter = header.begin(); iter != header.end(); ++iter) {
    if (evhttp_add_header(request->output_headers, iter->first.c_str(), iter->second.c_str()) !=
        0) {
      evhttp_request_free(request);
      return NULL;
    }
  }

  // Add compulsory headers
  evhttp_add_header(request->output_headers, "Accept-Encoding", "identity");
  std::ostringstream hoststr;
  hoststr << _request->host() << ":" << _request->port();
  evhttp_add_header(request->output_headers, "Host", hoststr.str().c_str());

  sp_string body;
  bool first = true;
  const HTTPKeyValuePairs& kv = _request->kv();
  for (size_t i = 0; i < kv.size(); ++i) {
    if (first) {
      body += BaseHTTPRequest::http_encode(kv[i].first) + "=" +
              BaseHTTPRequest::http_encode(kv[i].second);
      first = false;
    } else {
      body += "&" + BaseHTTPRequest::http_encode(kv[i].first) + "=" +
              BaseHTTPRequest::http_encode(kv[i].second);
    }
  }
  if (_request->type() == BaseHTTPRequest::POST || _request->type() == BaseHTTPRequest::PUT) {
    std::ostringstream o;
    o << body.size();
    evhttp_add_header(request->output_headers, "Content-Length", o.str().c_str());
    CHECK_GE(evbuffer_add_printf(request->output_buffer, "%s", body.c_str()), 0);
  } else {
    _request->ExtendQuery(body);
  }
  return request;
}
