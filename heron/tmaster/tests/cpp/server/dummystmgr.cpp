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

#include "server/dummystmgr.h"
#include <stdio.h>
#include <iostream>
#include <string>
#include <vector>
#include "proto/messages.h"
#include "basics/basics.h"
#include "errors/errors.h"
#include "threads/threads.h"
#include "network/network.h"

namespace heron {
namespace testing {

DummyStMgr::DummyStMgr(EventLoop* eventLoop, const NetworkOptions& options,
                       const sp_string& stmgr_id, const sp_string& myhost, sp_int32 myport,
                       const std::vector<proto::system::Instance*>& instances)
    : Client(eventLoop, options),
      my_id_(stmgr_id),
      my_host_(myhost),
      my_port_(myport),
      instances_(instances),
      pplan_(NULL) {
  InstallResponseHandler(new proto::tmaster::StMgrRegisterRequest(),
                         &DummyStMgr::HandleRegisterResponse);
  InstallResponseHandler(new proto::tmaster::StMgrHeartbeatRequest(),
                         &DummyStMgr::HandleHeartbeatResponse);
  InstallMessageHandler(&DummyStMgr::HandleNewAssignmentMessage);
}

DummyStMgr::~DummyStMgr() {}

void DummyStMgr::HandleConnect(NetworkErrorCode status) {
  if (status == OK) {
    LOG(INFO) << "Connected to " << get_clientoptions().get_host() << ":"
              << get_clientoptions().get_port();
    SendRegisterRequest();
  } else {
    LOG(ERROR) << "Could not connect to " << get_clientoptions().get_host() << ":"
               << get_clientoptions().get_port();
    AddTimer([this]() { this->OnReConnectTimer(); }, 10000000);
  }
}

void DummyStMgr::HandleClose(NetworkErrorCode code) {
  if (code == OK) {
    eventLoop_->loopExit();
  } else {
    LOG(ERROR) << "Server connection closed with code " << code;
    LOG(ERROR) << "Will try to reconnect again after 10 seconds";
    AddTimer([this]() { this->OnReConnectTimer(); }, 10000000);
  }
}

void DummyStMgr::HandleRegisterResponse(void*, proto::tmaster::StMgrRegisterResponse* response,
                                        NetworkErrorCode status) {
  if (status != OK) {
    LOG(ERROR) << "NonOK response message for Register Response";
    delete response;
    Stop();
    ::exit(1);
    return;
  }
  proto::system::StatusCode st = response->status().status();
  bool has_assignment = response->has_pplan();
  delete response;
  if (st != proto::system::OK) {
    LOG(ERROR) << "Register failed with status " << st;
    ::exit(1);
    return Stop();
  } else {
    if (has_assignment) {
      HandleNewPhysicalPlan(response->pplan());
      LOG(INFO) << "Register returned with an already existing pplan";
    } else {
      LOG(INFO) << "Register returned with no existing pplan";
    }
    AddTimer([this]() { this->OnHeartbeatTimer(); }, 10000000);
  }
}

void DummyStMgr::HandleHeartbeatResponse(void*, proto::tmaster::StMgrHeartbeatResponse* response,
                                         NetworkErrorCode status) {
  if (status != OK) {
    LOG(ERROR) << "NonOK response message for Register Response";
    delete response;
    Stop();
    return;
  }
  proto::system::StatusCode st = response->status().status();
  delete response;
  if (st != proto::system::OK) {
    LOG(ERROR) << "Heartbeat failed with status " << st;
    return Stop();
  } else {
    AddTimer([this]() { this->OnHeartbeatTimer(); }, 10000000);
  }
}

void DummyStMgr::HandleNewAssignmentMessage(proto::stmgr::NewPhysicalPlanMessage* message) {
  LOG(INFO) << "Got a new assignment";
  HandleNewPhysicalPlan(message->new_pplan());
  delete message;
}

void DummyStMgr::HandleNewPhysicalPlan(const proto::system::PhysicalPlan& pplan) {
  delete pplan_;
  pplan_ = new proto::system::PhysicalPlan(pplan);
}

void DummyStMgr::OnReConnectTimer() { Start(); }

void DummyStMgr::OnHeartbeatTimer() {
  LOG(INFO) << "Sending heartbeat";
  SendHeartbeatRequest();
}

void DummyStMgr::SendRegisterRequest() {
  proto::tmaster::StMgrRegisterRequest* request = new proto::tmaster::StMgrRegisterRequest();
  proto::system::StMgr* stmgr = request->mutable_stmgr();
  stmgr->set_id(my_id_);
  stmgr->set_host_name(my_host_);
  stmgr->set_data_port(my_port_);
  stmgr->set_local_endpoint("/tmp/somename");
  for (std::vector<proto::system::Instance*>::iterator iter = instances_.begin();
       iter != instances_.end(); ++iter) {
    request->add_instances()->CopyFrom(**iter);
  }
  SendRequest(request, NULL);
  return;
}

void DummyStMgr::SendHeartbeatRequest() {
  proto::tmaster::StMgrHeartbeatRequest* request = new proto::tmaster::StMgrHeartbeatRequest();
  request->set_heartbeat_time(time(NULL));
  request->mutable_stats();
  SendRequest(request, NULL);
  return;
}

proto::system::PhysicalPlan* DummyStMgr::GetPhysicalPlan() { return pplan_; }
}  // namespace testing
}  // namespace heron
