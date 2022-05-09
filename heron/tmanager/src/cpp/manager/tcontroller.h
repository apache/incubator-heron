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

#ifndef __TCONTROLLER_H_
#define __TCONTROLLER_H_

#include <map>
#include <string>
#include <vector>

#include "network/network.h"
#include "proto/tmanager.pb.h"
#include "basics/basics.h"

namespace heron {
namespace tmanager {

using std::shared_ptr;

class TManager;

class TController {
 public:
  TController(shared_ptr<EventLoop> eventLoop, const NetworkOptions& options, TManager* tmanager);
  virtual ~TController();

  // Starts the controller
  sp_int32 Start();

  // Called by the tmanager when it gets response form ckptmgr
  void HandleCleanStatefulCheckpointResponse(proto::system::StatusCode _status);

  // Parse and build a map of component name to config kv map from incoming runtime configs.
  // The incoming runtime configs should have this format: (COMPONENT_NAME|topology):(CONFIG_NAME)
  // Return false if the configs have bad format.
  static bool ParseRuntimeConfig(const std::vector<std::string>& paramters,
      std::map<std::string, std::map<std::string, std::string>>& configMap);

 private:
  // Handlers for the requests
  // In all the below handlers, the incoming _request
  // parameter is now owned by the
  // TController class as is the norm with HeronServer.

  void HandleActivateRequest(IncomingHTTPRequest* request);
  void HandleActivateRequestDone(IncomingHTTPRequest* request, proto::system::StatusCode);
  void HandleDeActivateRequest(IncomingHTTPRequest* request);
  void HandleDeActivateRequestDone(IncomingHTTPRequest* request, proto::system::StatusCode);
  void HandleCleanStatefulCheckpointRequest(IncomingHTTPRequest* request);
  void HandleCleanStatefulCheckpointRequestDone(IncomingHTTPRequest* request,
                                                proto::system::StatusCode);
  void HandleUpdateRuntimeConfigRequest(IncomingHTTPRequest* request);
  void HandleUpdateRuntimeConfigRequestDone(IncomingHTTPRequest* request,
                                            proto::system::StatusCode);
  void HandleGetCurPPlanRequest(IncomingHTTPRequest* request);

  // We are a http server
  unique_ptr<HTTPServer> http_server_;

  // our tmanager
  TManager* tmanager_;

  // The callback to be called upon receiving clean stateful checkpoint response
  std::function<void(proto::system::StatusCode)> clean_stateful_checkpoint_cb_;

  // Validate basic topology data
  // - topology id is available and matches the current topology
  // - physical plan is available
  // Detailed errors are logged in the function and returned in the ValidationResult object.
  // return true when passed; return false if any issue is found.
  class ValidationResult {
   public:
    ValidationResult() { code_ = 0; message_ = ""; }
    void SetResult(sp_int32 code, const std::string& message) {
      code_ = code;
      message_ = message;
    }
    sp_int32 GetCode() { return code_; }
    std::string& GetMessage() { return message_; }

   private:
    sp_int32 code_;
    std::string message_;
  };

  bool ValidateTopology(const IncomingHTTPRequest* request, ValidationResult& result);
};
}  // namespace tmanager
}  // namespace heron

#endif
