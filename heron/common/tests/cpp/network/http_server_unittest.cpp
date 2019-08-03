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

#include "network/http_server_unittest.h"
#include "network/host_unittest.h"
#include "gtest/gtest.h"
#include "basics/basics.h"
#include "errors/errors.h"
#include "threads/threads.h"
#include "network/network.h"

static sp_uint32 nkeys = 0;

TestHttpServer::TestHttpServer(std::shared_ptr<EventLoopImpl> eventLoop, NetworkOptions& _options) {
  server_ = new HTTPServer(eventLoop, _options);
  server_->InstallCallBack(
      "/meta", [this](IncomingHTTPRequest* request) { this->HandleMetaRequest(request); });

  server_->InstallCallBack("/terminate", [this](IncomingHTTPRequest* request) {
    this->HandleTerminateRequest(request);
  });

  server_->InstallGenericCallBack(
      [this](IncomingHTTPRequest* request) { this->HandleGenericRequest(request); });

  EXPECT_EQ(SP_OK, server_->Start());
}

TestHttpServer::~TestHttpServer() { delete server_; }

void TestHttpServer::HandleMetaRequest(IncomingHTTPRequest* _request) {
  if (_request->type() != BaseHTTPRequest::GET) {
    // We only accept get requests
    server_->SendErrorReply(_request, 400);
    return;
  }

  std::cerr << "Got a meta request" << std::endl;

  const HTTPKeyValuePairs& keyvalues = _request->keyvalues();
  EXPECT_EQ(nkeys, keyvalues.size());

  for (size_t i = 0; i < keyvalues.size(); ++i) {
    std::ostringstream key, value;
    key << "key" << i;
    value << "value" << i;

    EXPECT_EQ(key.str(), keyvalues[i].first);
    EXPECT_EQ(value.str(), keyvalues[i].second);
  }

  auto response = make_unique<OutgoingHTTPResponse>(_request);
  response->AddResponse("This is response for meta object\r\n");
  server_->SendReply(_request, 200, std::move(response));
}

void TestHttpServer::HandleGenericRequest(IncomingHTTPRequest* _request) {
  std::cerr << "Got a generic request" << std::endl;

  const HTTPKeyValuePairs& keyvalues = _request->keyvalues();
  for (size_t i = 0; i < keyvalues.size(); ++i) {
    std::cout << "Key : " << keyvalues[i].first << " "
              << "Value: " << keyvalues[i].second << " " << std::endl;
  }
  server_->SendErrorReply(_request, 404);
}

void TestHttpServer::HandleTerminateRequest(IncomingHTTPRequest* _request) {
  server_->getEventLoop()->loopExit();
}

void start_http_server(sp_uint32 _port, sp_uint32 _nkeys, int fd) {
  nkeys = _nkeys;

  auto ss = std::make_shared<EventLoopImpl>();

  // set host, port and packet size
  NetworkOptions options;
  options.set_host(LOCALHOST);
  options.set_port(_port);
  options.set_max_packet_size(BUFSIZ << 4);

  // start the server
  TestHttpServer http_server(ss, options);

  // use pipe to block clients before server enters event loop
  int sent;
  write(fd, &sent, sizeof(int));

  ss->loop();
}
