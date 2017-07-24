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
#include "manager/tmaster.h"
#include "proto/messages.h"
#include "basics/basics.h"
#include "errors/errors.h"
#include "threads/threads.h"
#include "network/network.h"

namespace heron {
namespace tmaster {

TController::TController(EventLoop* eventLoop, const NetworkOptions& options, TMaster* tmaster)
    : tmaster_(tmaster) {
  http_server_ = new HTTPServer(eventLoop, options);
  // Install the handlers
  auto cbActivate = [this](IncomingHTTPRequest* request) { this->HandleActivateRequest(request); };

  http_server_->InstallCallBack("/activate", std::move(cbActivate));

  auto cbDeActivate = [this](IncomingHTTPRequest* request) {
    this->HandleDeActivateRequest(request);
  };

  http_server_->InstallCallBack("/deactivate", std::move(cbDeActivate));

  auto cbCleanState = [this](IncomingHTTPRequest* request) {
    this->HandleCleanStatefulCheckpointRequest(request);
  };

  http_server_->InstallCallBack("/clean_all_stateful_checkpoints", std::move(cbCleanState));
}

TController::~TController() { delete http_server_; }

sp_int32 TController::Start() { return http_server_->Start(); }

void TController::HandleActivateRequest(IncomingHTTPRequest* request) {
  LOG(INFO) << "Got a activate topology request from " << request->GetRemoteHost() << ":"
            << request->GetRemotePort();
  const sp_string& id = request->GetValue("topologyid");
  if (id == "") {
    LOG(ERROR) << "topologyid not specified in the request";
    http_server_->SendErrorReply(request, 400);
    delete request;
    return;
  }
  if (tmaster_->getPhysicalPlan() == NULL) {
    LOG(ERROR) << "Tmaster still not initialized";
    http_server_->SendErrorReply(request, 500);
    delete request;
    return;
  }
  if (id != tmaster_->GetTopologyId()) {
    LOG(ERROR) << "Topology id does not match";
    http_server_->SendErrorReply(request, 400);
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
    sp_string s = "Topology successfully activated";
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
  const sp_string& id = request->GetValue("topologyid");
  if (id == "") {
    LOG(ERROR) << "Request does not contain topology id";
    http_server_->SendErrorReply(request, 400);
    delete request;
    return;
  }
  if (tmaster_->getPhysicalPlan() == NULL) {
    LOG(ERROR) << "Tmaster still not initialized";
    http_server_->SendErrorReply(request, 500);
    delete request;
    return;
  }
  if (id != tmaster_->GetTopologyId()) {
    LOG(ERROR) << "Topology id does not match";
    http_server_->SendErrorReply(request, 400);
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
    sp_string s = "Topology successfully deactivated";
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
  const sp_string& id = request->GetValue("topologyid");
  if (id == "") {
    LOG(ERROR) << "Request does not contain topology id";
    http_server_->SendErrorReply(request, 400);
    delete request;
    return;
  }
  if (tmaster_->getPhysicalPlan() == nullptr) {
    LOG(ERROR) << "Tmaster still not initialized";
    http_server_->SendErrorReply(request, 500);
    delete request;
    return;
  }
  if (id != tmaster_->GetTopologyId()) {
    LOG(ERROR) << "Topology id does not match";
    http_server_->SendErrorReply(request, 400);
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
    sp_string msg = "Checkpoints successfully cleaned";
    LOG(INFO) << msg;
    OutgoingHTTPResponse* response = new OutgoingHTTPResponse(request);
    response->AddResponse(msg);
    http_server_->SendReply(request, 200, response);
  }
  delete request;
}
}  // namespace tmaster
}  // namespace heron
