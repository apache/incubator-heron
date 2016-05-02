/*
 * Copyright 2015 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef __TEST_HTTP_SERVER_H_
#define __TEST_HTTP_SERVER_H_

#include "network/network_error.h"
#include "network/network.h"
#include "basics/basics.h"

class TestHttpServer {
 public:
  // Constructor
  TestHttpServer(EventLoopImpl* ss, NetworkOptions& options);

  // Destructor
  ~TestHttpServer();

  // Handle for a URL meta request
  void HandleMetaRequest(IncomingHTTPRequest* _request);

  // Handler for any type of generic request
  void HandleGenericRequest(IncomingHTTPRequest* _request);

  // Handle for a URL terminate request
  void HandleTerminateRequest(IncomingHTTPRequest* _request);

 private:
  HTTPServer* server_;
};

#endif
