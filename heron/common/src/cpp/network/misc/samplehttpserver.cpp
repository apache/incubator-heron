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

#include "heron/common/src/cpp/network/misc/samplehttpserver.h"
#include "core/common/public/common.h"
#include "core/errors/public/errors.h"
#include "core/threads/public/threads.h"
#include "core/network/public/network.h"

SampleHTTPServer::SampleHTTPServer(EventLoopImpl* eventLoop, ServerOptions* _options)
{
  server_ = new HTTPServer(eventLoop, _options);
  server_->InstallCallBack("/meta", CreatePermanentCallback(this, &SampleHTTPServer::HandleMetaRequest));
  server_->InstallGenericCallBack(CreatePermanentCallback(this, &SampleHTTPServer::HandleGenericRequest));
  server_->Start();
}

SampleHTTPServer::~SampleHTTPServer()
{
  delete server_;
}

void SampleHTTPServer::HandleMetaRequest(IncomingHTTPRequest* _request)
{
  cout << "Got a new meta request\n";
  if (_request->type() != BaseHTTPRequest::GET) {
    // We only accept get requests
    server_->SendErrorReply(_request, 400);
    return;
  }
  const HTTPKeyValuePairs& keyvalues = _request->keyvalues();
  cout << "The keyvalue count is " << keyvalues.size() << "\n";
  for (size_t i = 0; i < keyvalues.size(); ++i) {
    cout << "Key : " << keyvalues[i].first << " Value: "
         << keyvalues[i].second << "\n";
  }
  OutgoingHTTPResponse* response = new OutgoingHTTPResponse(_request);
  response->AddResponse("This is response for Meta Object\r\n");
  server_->SendReply(_request, 200, response);
}

void SampleHTTPServer::HandleGenericRequest(IncomingHTTPRequest* _request)
{
  cout << "Got a generic request for uri " << _request->GetQuery() << "\n";
  const HTTPKeyValuePairs& keyvalues = _request->keyvalues();
  cout << "The keyvalue count is " << keyvalues.size() << "\n";
  for (size_t i = 0; i < keyvalues.size(); ++i) {
    cout << "Key : " << keyvalues[i].first << " Value: "
         << keyvalues[i].second << "\n";
  }
  server_->SendErrorReply(_request, 404);
}
