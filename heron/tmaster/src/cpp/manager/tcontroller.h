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

#ifndef __TCONTROLLER_H_
#define __TCONTROLLER_H_

#include "network/network.h"
#include "proto/tmaster.pb.h"
#include "basics/basics.h"

namespace heron {
namespace tmaster {

class TMaster;

class TController {
 public:
  TController(EventLoop* eventLoop, const NetworkOptions& options, TMaster* tmaster);
  virtual ~TController();

  // Starts the controller
  sp_int32 Start();

  const NetworkOptions& get_serveroptions() const { return http_server_->get_serveroptions(); }

 private:
  // Handlers for the requests
  // In all the below handlers, the incoming _request
  // parameter is now owned by the
  // TController class as is the norm with HeronServer.

  void HandleActivateRequest(IncomingHTTPRequest* request);
  void HandleActivateRequestDone(IncomingHTTPRequest* request, proto::system::StatusCode);
  void HandleDeActivateRequest(IncomingHTTPRequest* request);
  void HandleDeActivateRequestDone(IncomingHTTPRequest* request, proto::system::StatusCode);

  // We are a http server
  HTTPServer* http_server_;

  // our tmaster
  TMaster* tmaster_;
};
}  // namespace tmaster
}  // namespace heron

#endif
