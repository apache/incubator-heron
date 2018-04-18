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

#include "manager/tcontroller.h"

#include <iostream>
#include <map>
#include <string>
#include <utility>
#include <vector>
#include "basics/basics.h"
#include "basics/strutils.h"
#include "config/topology-config-helper.h"
#include "errors/errors.h"
#include "manager/tmaster.h"
#include "network/network.h"
#include "proto/messages.h"
#include "threads/threads.h"

namespace heron {
namespace tmaster {

/*
 * HTTP service controller.
 */
TController::TController(EventLoop* eventLoop, const NetworkOptions& options, TMaster* tmaster)
    : tmaster_(tmaster) {
  http_server_ = new HTTPServer(eventLoop, options);
  /*
   * Install the handlers
   */
  // Activate and deactivate
  auto cbActivate = [this](IncomingHTTPRequest* request) { this->HandleActivateRequest(request); };
  http_server_->InstallCallBack("/activate", std::move(cbActivate));

  auto cbDeActivate = [this](IncomingHTTPRequest* request) {
    this->HandleDeActivateRequest(request);
  };
  http_server_->InstallCallBack("/deactivate", std::move(cbDeActivate));

  // Clear checkpoint
  auto cbCleanState = [this](IncomingHTTPRequest* request) {
    this->HandleCleanStatefulCheckpointRequest(request);
  };
  http_server_->InstallCallBack("/clean_all_stateful_checkpoints", std::move(cbCleanState));

  // Runtime config
  auto cbUpdateRuntimeConfg = [this](IncomingHTTPRequest* request) {
    this->HandleUpdateRuntimeConfigRequest(request);
  };
  http_server_->InstallCallBack("/runtime_config/update", std::move(cbUpdateRuntimeConfg));
}

TController::~TController() { delete http_server_; }

sp_int32 TController::Start() { return http_server_->Start(); }

void TController::HandleActivateRequest(IncomingHTTPRequest* request) {
  LOG(INFO) << "Got a activate topology request from " << request->GetRemoteHost() << ":"
            << request->GetRemotePort();
  // Validation
  ValidationResult result;
  if (!ValidateTopology(request, result)) {
    http_server_->SendErrorReply(request, result.GetCode(), result.GetMessage());
    delete request;
    return;
  }

  if (tmaster_->GetTopologyState() != proto::api::PAUSED) {
    LOG(ERROR) << "Topology not in paused state";
    http_server_->SendErrorReply(request, 400);
    delete request;
    return;
  }

  auto cb = [request, this](proto::system::StatusCode status) {
    this->HandleActivateRequestDone(request, status);
  };

  tmaster_->ActivateTopology(std::move(cb));
}

void TController::HandleActivateRequestDone(IncomingHTTPRequest* request,
                                            proto::system::StatusCode _status) {
  if (_status != proto::system::OK) {
    LOG(ERROR) << "Unable to Activate topology " << _status;
    http_server_->SendErrorReply(request, 500);
  } else {
    std::string s = "Topology successfully activated";
    LOG(INFO) << s;
    OutgoingHTTPResponse* response = new OutgoingHTTPResponse(request);
    response->AddResponse(s);
    http_server_->SendReply(request, 200, response);
  }
  delete request;
}

void TController::HandleDeActivateRequest(IncomingHTTPRequest* request) {
  LOG(INFO) << "Got a deactivate topology request from " << request->GetRemoteHost() << ":"
            << request->GetRemotePort();
  ValidationResult result;
  if (!ValidateTopology(request, result)) {
    http_server_->SendErrorReply(request, result.GetCode(), result.GetMessage());
    delete request;
    return;
  }

  if (tmaster_->GetTopologyState() != proto::api::RUNNING) {
    LOG(ERROR) << "Topology not in running state";
    http_server_->SendErrorReply(request, 400);
    delete request;
    return;
  }

  auto cb = [request, this](proto::system::StatusCode status) {
    this->HandleDeActivateRequestDone(request, status);
  };

  tmaster_->DeActivateTopology(std::move(cb));
}

void TController::HandleDeActivateRequestDone(IncomingHTTPRequest* request,
                                              proto::system::StatusCode _status) {
  if (_status != proto::system::OK) {
    LOG(ERROR) << "Unable to DeActivate topology " << _status;
    http_server_->SendErrorReply(request, 500);
  } else {
    std::string s = "Topology successfully deactivated";
    LOG(INFO) << s;
    OutgoingHTTPResponse* response = new OutgoingHTTPResponse(request);
    response->AddResponse(s);
    http_server_->SendReply(request, 200, response);
  }
  delete request;
}

void TController::HandleCleanStatefulCheckpointRequest(IncomingHTTPRequest* request) {
  LOG(INFO) << "Got a CleanStatefulCheckpoint request from " << request->GetRemoteHost() << ":"
            << request->GetRemotePort();
  ValidationResult result;
  if (!ValidateTopology(request, result)) {
    http_server_->SendErrorReply(request, result.GetCode(), result.GetMessage());
    delete request;
    return;
  }

  if (clean_stateful_checkpoint_cb_) {
    LOG(ERROR) << "Another clean request is already pending";
    http_server_->SendErrorReply(request, 400);
    delete request;
    return;
  }

  clean_stateful_checkpoint_cb_ = [request, this](proto::system::StatusCode status) {
    this->HandleCleanStatefulCheckpointRequestDone(request, status);
  };

  tmaster_->CleanAllStatefulCheckpoint();
}

void TController::HandleCleanStatefulCheckpointResponse(proto::system::StatusCode _status) {
  if (clean_stateful_checkpoint_cb_) {
    clean_stateful_checkpoint_cb_(_status);
    clean_stateful_checkpoint_cb_ = nullptr;
  }
}

void TController::HandleCleanStatefulCheckpointRequestDone(IncomingHTTPRequest* request,
                                              proto::system::StatusCode _status) {
  LOG(INFO) << "Done with CleanStatefulCheckpoint Request with " << _status;
  if (_status != proto::system::OK) {
    LOG(ERROR) << "Unable to CleanStatefulCheckpoint" << _status;
    http_server_->SendErrorReply(request, 500);
  } else {
    std::string msg = "Checkpoints successfully cleaned";
    LOG(INFO) << msg;
    OutgoingHTTPResponse* response = new OutgoingHTTPResponse(request);
    response->AddResponse(msg);
    http_server_->SendReply(request, 200, response);
  }
  delete request;
}

void TController::HandleUpdateRuntimeConfigRequest(IncomingHTTPRequest* request) {
  LOG(INFO) << "Got a RuntimeConfig request from " << request->GetRemoteHost() << ":"
            << request->GetRemotePort();
  ValidationResult result;
  if (!ValidateTopology(request, result)) {
    http_server_->SendErrorReply(request, result.GetCode(), result.GetMessage());
    delete request;
    return;
  }

    // Look for runtime-config parameters
  std::vector<std::string> parameters;
  if (!request->GetAllValues("runtime-config", parameters)) {
    LOG(ERROR) << "No runtime config is found";
    http_server_->SendErrorReply(request, 400, "No runtime config is found."
        " Usage: runtime-config=[COMPONENT:]<CONFIG>:<VALUE>.");
    delete request;
    return;
  }
  LOG(INFO) << "Found " << parameters.size() << " configs in request.";

  // Parse new configs in request
  std::map<std::string, std::map<std::string, std::string>> config;
  if (!ParseRuntimeConfig(parameters, config)) {
    http_server_->SendErrorReply(request, 400, "Failed to parse runtime configs."
        " Possibly bad format. The expected format is [COMPONENT:]<CONFIG>:<VALUE>.");
    delete request;
    return;
  }

  // Validate them before applying
  if (!tmaster_->ValidateRuntimeConfig(config)) {
    http_server_->SendErrorReply(request, 400, "Failed to validate runtime configs");
    delete request;
    return;
  }

  auto cb = [request, this](proto::system::StatusCode status) {
    this->HandleUpdateRuntimeConfigRequestDone(request, status);
  };

  if (!tmaster_->UpdateRuntimeConfig(config, std::move(cb))) {
    http_server_->SendErrorReply(request, 400, "Failed to update runtime configs");
    delete request;
    return;
  }
}

void TController::HandleUpdateRuntimeConfigRequestDone(IncomingHTTPRequest* request,
                                                       proto::system::StatusCode _status) {
  if (_status != proto::system::OK) {
    std::string error = "Failed to update runtime configs ";
    error += _status;
    LOG(ERROR) << error;
    http_server_->SendErrorReply(request, 500, error);
  } else {
    const std::string message("Runtime config updated");
    LOG(INFO) << message;
    OutgoingHTTPResponse* response = new OutgoingHTTPResponse(request);
    response->AddResponse(message);
    http_server_->SendReply(request, 200, response);
  }
  delete request;
}

/*
 * Validate topology.
 * - topology id matches
 * - topology is initialized
 * return true if topology is validated, false otherwise with error details stored in result object
 */
bool TController::ValidateTopology(const IncomingHTTPRequest* request, ValidationResult& result) {
  const std::string& id = request->GetValue("topologyid");
  if (id == "") {
    LOG(ERROR) << "Argument 'topologyid' not specified in the request";
    result.SetResult(400, "Missing 'topologyid' argument in the request");
    return false;
  }
  if (id != tmaster_->GetTopologyId()) {
    LOG(ERROR) << "Topology id does not match";
    result.SetResult(400, "Topology id does not match");
    return false;
  }
  if (tmaster_->getPhysicalPlan() == NULL) {
    LOG(ERROR) << "Tmaster still not initialized (physical plan is not available)";
    result.SetResult(500, "Tmaster still not initialized (physical plan is not available)");
    return false;
  }

  return true;
}

bool TController::ParseRuntimeConfig(const std::vector<std::string>& paramters,
    std::map<std::string, std::map<std::string, std::string>>& retval) {
  // Configs are in the followingconfigMap format: [component:]config:value.
  // It seems the regex implementation in c++ 11 is not very stable (The regex
  // pattern("^([\\w\\.-]+:)?[\\w\\.-]+:[\\w\\.-]+$") runs ok in osx but throws exception in
  // linux). Therefore we are manually searching ':' here (there should be 1 or 2 ':'s) instead of
  // using regex matching.
  std::vector<std::string>::const_iterator iter;
  for (iter = paramters.begin(); iter != paramters.end(); ++iter) {
    // Split using ':'
    std::vector<std::string> segments = StrUtils::split(*iter, ":");
    if (segments.size() == 2) {
      // Topology level config
      const char* topology_key = config::TopologyConfigHelper::GetReservedTopologyConfigKey();
      retval[topology_key][segments[0]] = segments[1];
    } else if (segments.size() == 3) {
      // Component level config
      retval[segments[0]][segments[1]] = segments[2];
    } else {
      // Wrong number of ':'s
      LOG(ERROR) << "Bad format detected in " << *iter << "."
          << " Each config should be in this format: [component:]config:value";
      return false;
    }
  }
  return true;
}

}  // namespace tmaster
}  // namespace heron
