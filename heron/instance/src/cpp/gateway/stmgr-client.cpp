/*
 * Copyright 2017 Twitter, Inc.
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

#include <stdio.h>
#include <iostream>
#include <string>

#include "gateway/stmgr-client.h"
#include "proto/messages.h"
#include "basics/basics.h"
#include "threads/threads.h"
#include "network/network.h"
#include "config/heron-internals-config-reader.h"


namespace heron {
namespace instance {

StMgrClient::StMgrClient(EventLoop* eventLoop, const NetworkOptions& options,
                         const std::string& topologyName, const std::string& topologyId,
                         const proto::system::Instance& instanceProto,
                         std::shared_ptr<GatewayMetrics> gatewayMetrics,
                         std::function<void(proto::system::PhysicalPlan*)> pplanWatcher,
                         std::function<void(proto::system::HeronTupleSet2*)> tupleWatcher)
    : Client(eventLoop, options),
      topologyName_(topologyName),
      topologyId_(topologyId),
      instanceProto_(instanceProto),
      gatewayMetrics_(gatewayMetrics),
      pplanWatcher_(std::move(pplanWatcher)),
      tupleWatcher_(std::move(tupleWatcher)),
      ndropped_messages_(0) {
  reconnect_interval_ = config::HeronInternalsConfigReader::Instance()
                               ->GetHeronInstanceReconnectStreammgrIntervalSec();
  max_reconnect_times_ = config::HeronInternalsConfigReader::Instance()
                               ->GetHeronInstanceReconnectStreammgrTimes();
  reconnect_attempts_ = 0;
  InstallResponseHandler(new proto::stmgr::RegisterInstanceRequest(),
                         &StMgrClient::HandleRegisterResponse);
  InstallMessageHandler(&StMgrClient::HandlePhysicalPlan);
  InstallMessageHandler(&StMgrClient::HandleTupleMessage);
}

StMgrClient::~StMgrClient() {
  Stop();
}

void StMgrClient::HandleConnect(NetworkErrorCode status) {
  if (status == OK) {
    LOG(INFO) << "Connected to stmgr " << instanceProto_.stmgr_id() << " running at "
              << get_clientoptions().get_host() << ":" << get_clientoptions().get_port()
              << std::endl;
    reconnect_attempts_ = 0;
    SendRegisterRequest();
  } else {
    LOG(WARNING) << "Could not connect to stmgr " << instanceProto_.stmgr_id() << " running at "
                 << get_clientoptions().get_host() << ":" << get_clientoptions().get_port()
                 << " due to: " << status << std::endl;
    ++reconnect_attempts_;
    if (reconnect_attempts_ > max_reconnect_times_) {
      LOG(FATAL) << "Could not connect to stmgr " << instanceProto_.stmgr_id() << " running at "
                   << get_clientoptions().get_host() << ":" << get_clientoptions().get_port()
                   << " after repeated attempts. Dying...";
    }
    LOG(INFO) << "Retrying again..." << std::endl;
    AddTimer([this]() { this->OnReconnectTimer(); }, reconnect_interval_ * 1000 * 1000);
  }
}

void StMgrClient::HandleClose(NetworkErrorCode code) {
  if (code == OK) {
    LOG(INFO) << "We closed our server connection with stmgr " << instanceProto_.stmgr_id()
              << " running at " << get_clientoptions().get_host()
              << ":" << get_clientoptions().get_port();
  } else {
    LOG(INFO) << "Stmgr " << instanceProto_.stmgr_id() << " running at "
              << get_clientoptions().get_host()
              << ":" << get_clientoptions().get_port() << " closed connection with code " << code;
  }
  LOG(INFO) << "Will try to reconnect again" << std::endl;
  AddTimer([this]() { this->OnReconnectTimer(); }, reconnect_interval_ * 1000 * 1000);
}

void StMgrClient::HandleRegisterResponse(void*, proto::stmgr::RegisterInstanceResponse* response,
                                         NetworkErrorCode status) {
  if (status != OK) {
    LOG(ERROR) << "NonOK network code " << status << " for register response from stmgr "
               << instanceProto_.stmgr_id() << " running at " << get_clientoptions().get_host()
               << ":" << get_clientoptions().get_port();
    delete response;
    Stop();
    return;
  }
  proto::system::StatusCode stat = response->status().status();
  if (stat != proto::system::OK) {
    LOG(ERROR) << "NonOK register response " << stat << " from stmgr "
               << instanceProto_.stmgr_id() << " running at "
               << get_clientoptions().get_host() << ":" << get_clientoptions().get_port();
    delete response;
    Stop();
    return;
  }
  LOG(INFO) << "Registered with our stmgr " << instanceProto_.stmgr_id() << " running at "
              << get_clientoptions().get_host() << ":" << get_clientoptions().get_port();
  if (response->has_pplan()) {
    LOG(INFO) << "Registration response had a pplan";
    pplanWatcher_(response->release_pplan());
  }
  delete response;
}

void StMgrClient::OnReconnectTimer() { Start(); }

void StMgrClient::SendRegisterRequest() {
  auto request = new proto::stmgr::RegisterInstanceRequest();
  request->set_topology_name(topologyName_);
  request->set_topology_id(topologyId_);
  request->mutable_instance()->CopyFrom(instanceProto_);
  SendRequest(request, NULL);
  return;
}

void StMgrClient::HandlePhysicalPlan(proto::stmgr::NewInstanceAssignmentMessage* msg) {
  LOG(INFO) << "Got a Physical Plan from our stmgr " << instanceProto_.stmgr_id() << " running at "
              << get_clientoptions().get_host() << ":" << get_clientoptions().get_port();
  pplanWatcher_(msg->release_pplan());
  delete msg;
}

void StMgrClient::HandleTupleMessage(proto::system::HeronTupleSet2* msg) {
  gatewayMetrics_->updateReceivedPacketsCount(1);
  gatewayMetrics_->updateReceivedPacketsSize(msg->ByteSize());
  tupleWatcher_(msg);
}

void StMgrClient::SendTupleMessage(const proto::system::HeronTupleSet& msg) {
  if (IsConnected()) {
    gatewayMetrics_->updateSentPacketsCount(1);
    gatewayMetrics_->updateSentPacketsSize(msg.ByteSize());
    SendMessage(msg);
  } else {
    gatewayMetrics_->updateDroppedPacketsCount(1);
    gatewayMetrics_->updateDroppedPacketsSize(msg.ByteSize());
    if (++ndropped_messages_ % 100 == 0) {
      LOG(INFO) << "Dropping " << ndropped_messages_ << "th tuple message to stmgr "
                << instanceProto_.stmgr_id() << " because it is not connected";
    }
  }
}

void StMgrClient::putBackPressure() {
  auto conn = static_cast<Connection*>(conn_);
  if (!conn->isUnderBackPressure()) {
    LOG(INFO) << "Buffer to Slave Thread at maximum capacity. Clamping down on reads from Stmgr";
    conn->putBackPressure();
  }
}

void StMgrClient::removeBackPressure() {
  auto conn = static_cast<Connection*>(conn_);
  if (conn->isUnderBackPressure()) {
    LOG(INFO) << "Buffer to Slave Thread less than capacity. Resuming reads from stmgr";
    conn->removeBackPressure();
  }
}

}  // namespace instance
}  // namespace heron
