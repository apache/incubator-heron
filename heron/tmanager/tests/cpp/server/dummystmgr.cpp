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

DummyStMgr::DummyStMgr(std::shared_ptr<EventLoop> eventLoop, const NetworkOptions& options,
                       const sp_string& stmgr_id, const sp_string& myhost, sp_int32 myport,
                       const std::vector<proto::system::Instance*>& instances)
    : Client(eventLoop, options),
      my_id_(stmgr_id),
      my_host_(myhost),
      my_port_(myport),
      instances_(instances),
      pplan_(nullptr),
      got_restore_message_(false),
      got_start_message_(false) {
  InstallResponseHandler(make_unique<proto::tmanager::StMgrRegisterRequest>(),
                         &DummyStMgr::HandleRegisterResponse);
  InstallResponseHandler(make_unique<proto::tmanager::StMgrHeartbeatRequest>(),
                         &DummyStMgr::HandleHeartbeatResponse);
  InstallMessageHandler(&DummyStMgr::HandleNewAssignmentMessage);
  InstallMessageHandler(&DummyStMgr::HandleRestoreTopologyStateRequest);
  InstallMessageHandler(&DummyStMgr::HandleStartProcessingMessage);
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

void DummyStMgr::HandleRegisterResponse(
                                    void*,
                                    pool_unique_ptr<proto::tmanager::StMgrRegisterResponse> response,
                                    NetworkErrorCode status) {
  if (status != OK) {
    LOG(ERROR) << "NonOK response message for Register Response";
    Stop();
    ::exit(1);
    return;
  }

  proto::system::StatusCode st = response->status().status();

  bool has_assignment = response->has_pplan();

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

void DummyStMgr::HandleHeartbeatResponse(
                                  void*,
                                  pool_unique_ptr<proto::tmanager::StMgrHeartbeatResponse> response,
                                  NetworkErrorCode status) {
  if (status != OK) {
    LOG(ERROR) << "NonOK response message for Register Response";
    Stop();
    return;
  }

  proto::system::StatusCode st = response->status().status();

  if (st != proto::system::OK) {
    LOG(ERROR) << "Heartbeat failed with status " << st;
    return Stop();
  } else {
    AddTimer([this]() { this->OnHeartbeatTimer(); }, 10000000);
  }
}

void DummyStMgr::HandleNewAssignmentMessage(
                                    pool_unique_ptr<proto::stmgr::NewPhysicalPlanMessage> message) {
  LOG(INFO) << "Got a new assignment";
  HandleNewPhysicalPlan(message->new_pplan());
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
  auto request = make_unique<proto::tmanager::StMgrRegisterRequest>();
  proto::system::StMgr* stmgr = request->mutable_stmgr();
  stmgr->set_id(my_id_);
  stmgr->set_host_name(my_host_);
  stmgr->set_data_port(my_port_);
  stmgr->set_local_endpoint("/tmp/somename");
  for (std::vector<proto::system::Instance*>::iterator iter = instances_.begin();
       iter != instances_.end(); ++iter) {
    request->add_instances()->CopyFrom(**iter);
  }
  SendRequest(std::move(request), nullptr);
  return;
}

void DummyStMgr::SendHeartbeatRequest() {
  auto request = make_unique<proto::tmanager::StMgrHeartbeatRequest>();
  request->set_heartbeat_time(time(NULL));
  request->mutable_stats();
  SendRequest(std::move(request), nullptr);
  return;
}

void DummyStMgr::HandleRestoreTopologyStateRequest(
                                  pool_unique_ptr<proto::ckptmgr::RestoreTopologyStateRequest> _m) {
  got_restore_message_ = true;
}

void DummyStMgr::HandleStartProcessingMessage(
        pool_unique_ptr<proto::ckptmgr::StartStmgrStatefulProcessing> _m) {
  got_start_message_ = true;
}

proto::system::PhysicalPlan* DummyStMgr::GetPhysicalPlan() { return pplan_; }
}  // namespace testing
}  // namespace heron
