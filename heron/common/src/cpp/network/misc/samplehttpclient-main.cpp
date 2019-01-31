/*
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

#include <iostream>

#include "core/common/public/common.h"
#include "core/errors/public/errors.h"
#include "core/threads/public/threads.h"
#include "core/network/public/network.h"

lx_int32 port = -1;

void SendRequest(HTTPClient* _client);

void RequestDone(HTTPClient* _client, IncomingHTTPResponse* _response)
{
  if (_response->response_code() != 200) {
    cout << "Error response quitting\n";
  } else {
    cout << "The body is " << _response->body() << "\n";
    SendRequest(_client);
  }
  delete _response;
}


void SendRequest(HTTPClient* _client)
{
  HTTPKeyValuePairs kvs;
  kvs.push_back(make_pair("key", "value"));
  OutgoingHTTPRequest* request = new OutgoingHTTPRequest("127.0.0.1", port,
                                                         "/meta",
                                                         BaseHTTPRequest::GET, kvs);
  if (_client->SendRequest(request, CreateCallback(&RequestDone, _client)) != LX_OK) {
    cout << "Unable to send the request\n";
  }
}

int main(int argc, char* argv[])
{
  Init("SampleHTTPClient", argc, argv);

  if (argc < 2) {
    cout << "Usage " << argv[0] << " <port>\n";
    exit(1);
  }
  port = atoi(argv[1]);

  EventLoopImpl ss;
  AsyncDNS dns(&ss);
  HTTPClient client(&ss, &dns);
  SendRequest(&client);
  ss.loop();
  return 0;
}
