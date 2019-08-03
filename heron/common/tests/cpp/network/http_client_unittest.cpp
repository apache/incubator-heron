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

#include "gtest/gtest.h"
#include "network/host_unittest.h"
#include "basics/basics.h"
#include "errors/errors.h"
#include "threads/threads.h"
#include "network/network.h"

static sp_uint64 ntotal = 0;
static sp_uint64 nreqs = 0;
static sp_uint64 nresps = 0;
static sp_uint32 port = -1;
static sp_uint32 nkeys = 0;

void RequestDone(HTTPClient* _client, IncomingHTTPResponse* _response);

void SendRequest(HTTPClient* client) {
  HTTPKeyValuePairs kvs;

  for (sp_uint32 i = 0; i < nkeys; i++) {
    std::ostringstream key, value;
    key << "key" << i;
    value << "value" << i;
    kvs.push_back(make_pair(key.str(), value.str()));
  }

  OutgoingHTTPRequest* request =
      new OutgoingHTTPRequest(LOCALHOST, port, "/meta", BaseHTTPRequest::GET, kvs);

  auto cb = [client](IncomingHTTPResponse* response) { RequestDone(client, response); };

  if (client->SendRequest(request, std::move(cb)) != SP_OK) {
    GTEST_FAIL();
  }

  nreqs++;
}

void RequestDone(HTTPClient* _client, IncomingHTTPResponse* _response) {
  nresps++;
  EXPECT_EQ(200, _response->response_code());

  delete _response;

  if (nreqs < ntotal) {
    SendRequest(_client);
    return;
  }

  _client->getEventLoop()->loopExit();
}

void start_http_client(sp_uint32 _port, sp_uint64 _requests, sp_uint32 _nkeys) {
  port = _port;
  ntotal = _requests;
  nkeys = _nkeys;

  auto ss = std::make_shared<EventLoopImpl>();
  AsyncDNS dns(ss);
  HTTPClient client(ss, &dns);
  SendRequest(&client);
  ss->loop();
}
