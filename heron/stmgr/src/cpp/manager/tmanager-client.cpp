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

#include "manager/tmanager-client.h"
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <string>
#include <vector>
#include <iostream>
#include "manager/stmgr.h"
#include "proto/messages.h"
#include "basics/basics.h"
#include "errors/errors.h"
#include "threads/threads.h"
#include "network/network.h"
#include "config/heron-internals-config-reader.h"

namespace heron {
namespace stmgr {

TManagerClient::TManagerClient(shared_ptr<EventLoop> eventLoop, const NetworkOptions& _options,
                             const sp_string& _stmgr_id, const sp_string& _stmgr_host,
                             sp_int32 _data_port, sp_int32 _local_data_port, sp_int32 _shell_port,
                             VCallback<shared_ptr<proto::system::PhysicalPlan>> _pplan_watch,
                             VCallback<sp_string> _stateful_checkpoint_watch,
                             VCallback<sp_string, sp_int64> _restore_topology_watch,
                             VCallback<sp_string> _start_stateful_watch,
                             VCallback<const proto::ckptmgr::StatefulConsistentCheckpointSaved&>
                                 _broadcast_checkpoint_saved)
    : Client(eventLoop, _options),
      stmgr_id_(_stmgr_id),
      stmgr_host_(_stmgr_host),
      data_port_(_data_port),
      local_data_port_(_local_data_port),
      shell_port_(_shell_port),
      to_die_(false),
      pplan_watch_(std::move(_pplan_watch)),
      stateful_checkpoint_watch_(std::move(_stateful_checkpoint_watch)),
      restore_topology_watch_(std::move(_restore_topology_watch)),
      start_stateful_watch_(std::move(_start_stateful_watch)),
      broadcast_checkpoint_saved_(_broadcast_checkpoint_saved),
      reconnect_timer_id(0),
      heartbeat_timer_id(0),
      reconnect_attempts_(0) {
  reconnect_tmanager_interval_sec_ = config::HeronInternalsConfigReader::Instance()
      ->GetHeronStreammgrClientReconnectTmanagerIntervalSec();
  stream_to_tmanager_heartbeat_interval_sec_ = config::HeronInternalsConfigReader::Instance()
      ->GetHeronStreammgrTmanagerHeartbeatIntervalSec();
  reconnect_max_attempt_ = config::HeronInternalsConfigReader::Instance()
      ->GetHeronStreammgrClientReconnectTmanagerMaxAttempts();

  reconnect_timer_cb = [this]() { this->OnReConnectTimer(); };
  heartbeat_timer_cb = [this]() { this->OnHeartbeatTimer(); };

  InstallResponseHandler(make_unique<proto::tmanager::StMgrRegisterRequest>(),
                         &TManagerClient::HandleRegisterResponse);
  InstallResponseHandler(make_unique<proto::tmanager::StMgrHeartbeatRequest>(),
                         &TManagerClient::HandleHeartbeatResponse);
  InstallMessageHandler(&TManagerClient::HandleNewAssignmentMessage);
  InstallMessageHandler(&TManagerClient::HandleStatefulCheckpointMessage);
  InstallMessageHandler(&TManagerClient::HandleRestoreTopologyStateRequest);
  InstallMessageHandler(&TManagerClient::HandleStartStmgrStatefulProcessing);
  InstallMessageHandler(&TManagerClient::HandleStatefulCheckpointSavedMessage);
}

TManagerClient::~TManagerClient() {
  CleanInstances();
}

void TManagerClient::Die() {
  LOG(INFO) << "Tmanager client is being destroyed " << std::endl;
  to_die_ = true;
  Stop();
  // Unregister the timers
  if (reconnect_timer_id > 0) {
    RemoveTimer(reconnect_timer_id);
  }

  if (heartbeat_timer_id > 0) {
    RemoveTimer(heartbeat_timer_id);
  }
}

sp_string TManagerClient::getTmanagerHostPort() {
  return options_.get_host() + ":" + std::to_string(options_.get_port());
}

void TManagerClient::HandleConnect(NetworkErrorCode _status) {
  if (_status == OK) {
    // reset the reconnect attempt once connection established
    reconnect_attempts_ = 0;

    if (to_die_) {
      Stop();
      return;
    }
    LOG(INFO) << "Connected to tmanager running at " << get_clientoptions().get_host() << ":"
              << get_clientoptions().get_port() << std::endl;
    SendRegisterRequest();
  } else {
    if (to_die_) {
      delete this;
      return;
    }
    LOG(ERROR) << "Could not connect to tmanager at " << get_clientoptions().get_host() << ":"
               << get_clientoptions().get_port() << ", Status code: " << _status << std::endl;
    LOG(INFO) << "Will retry again..." << std::endl;
    // Shouldn't be in a state where a previous timer is not cleared yet.
    if (reconnect_timer_id == 0) {
      reconnect_timer_id = AddTimer(reconnect_timer_cb, reconnect_tmanager_interval_sec_ * 1000000);
    }
  }
}

void TManagerClient::HandleClose(NetworkErrorCode _code) {
  if (to_die_) {
    delete this;
    return;
  }
  LOG(INFO) << "TManager connection closed with code " << _code << std::endl;
  LOG(INFO) << "Will try to reconnect again after " << reconnect_tmanager_interval_sec_ << "seconds"
            << std::endl;
  // Shouldn't be in a state where a previous timer is not cleared yet.
  CHECK_EQ(reconnect_timer_id, 0);

  // Remove the heartbeat timer since we have disconnected
  if (heartbeat_timer_id > 0) {
    RemoveTimer(heartbeat_timer_id);
    heartbeat_timer_id = 0;
  }

  reconnect_timer_id = AddTimer(reconnect_timer_cb, reconnect_tmanager_interval_sec_ * 1000000);
}

void TManagerClient::HandleRegisterResponse(
                                  void*,
                                  pool_unique_ptr<proto::tmanager::StMgrRegisterResponse> _response,
                                  NetworkErrorCode _status) {
  if (_status != OK) {
    LOG(ERROR) << "non ok network stack code for Register Response from Tmanager" << std::endl;
    Stop();
    return;
  }

  proto::system::StatusCode status = _response->status().status();

  if (status != proto::system::OK) {
    LOG(ERROR) << "Register with Tmanager failed with status " << status << std::endl;
    Stop();
  } else {
    LOG(INFO) << "Registered successfully with Tmanager" << std::endl;
    if (_response->has_pplan()) {
      pplan_watch_(shared_ptr<proto::system::PhysicalPlan>(_response->release_pplan()));
    }
    // Shouldn't be in a state where a previous timer is not cleared yet.
    CHECK_EQ(heartbeat_timer_id, 0);
    heartbeat_timer_id =
        AddTimer(heartbeat_timer_cb, stream_to_tmanager_heartbeat_interval_sec_ * 1000000);
  }
}

void TManagerClient::HandleHeartbeatResponse(
                                  void*,
                                  pool_unique_ptr<proto::tmanager::StMgrHeartbeatResponse> _response,
                                  NetworkErrorCode _status) {
  if (_status != OK) {
    LOG(ERROR) << "NonOK response message for heartbeat Response" << std::endl;
    Stop();
    return;
  }

  proto::system::StatusCode status = _response->status().status();

  if (status != proto::system::OK) {
    LOG(ERROR) << "Heartbeat failed with status " << status << std::endl;
    return Stop();
  } else {
    // Shouldn't be in a state where a previous timer is not cleared yet.
    CHECK_EQ(heartbeat_timer_id, 0);
    heartbeat_timer_id =
        AddTimer(heartbeat_timer_cb, stream_to_tmanager_heartbeat_interval_sec_ * 1000000);
  }
}

void TManagerClient::HandleNewAssignmentMessage(
        pool_unique_ptr<proto::stmgr::NewPhysicalPlanMessage> _message) {
  LOG(INFO) << "Got a new assignment" << std::endl;
  pplan_watch_(shared_ptr<proto::system::PhysicalPlan>(_message->release_new_pplan()));
}

void TManagerClient::HandleStatefulCheckpointMessage(
        pool_unique_ptr<proto::ckptmgr::StartStatefulCheckpoint> _message) {
  LOG(INFO) << "Got a new start stateful checkpoint message from tmanager with id "
            << _message->checkpoint_id();
  stateful_checkpoint_watch_(_message->checkpoint_id());
}

void TManagerClient::OnReConnectTimer() {
  // The timer has triggered the callback, so reset the timer_id;
  reconnect_timer_id = 0;

  if (++reconnect_attempts_ < reconnect_max_attempt_) {
    Start();
  } else {
    LOG(FATAL) << "Could not connect to tmanager after reaching"
               << " the max reconnect attempts" << reconnect_max_attempt_
               << ". Quitting...";
  }
}

void TManagerClient::OnHeartbeatTimer() {
  DLOG(INFO) << "Sending heartbeat" << std::endl;
  // The timer has triggered the callback, so reset the timer_id;
  heartbeat_timer_id = 0;
  SendHeartbeatRequest();
}

void TManagerClient::CleanInstances() {
  instances_.clear();
}

void TManagerClient::SendRegisterRequest() {
  auto request = make_unique<proto::tmanager::StMgrRegisterRequest>();

  sp_string cwd;
  FileUtils::getCwd(cwd);
  proto::system::StMgr* stmgr = request->mutable_stmgr();
  stmgr->set_id(stmgr_id_);
  stmgr->set_host_name(stmgr_host_);
  stmgr->set_data_port(data_port_);
  stmgr->set_local_data_port(local_data_port_);
  stmgr->set_local_endpoint("/unused");
  stmgr->set_cwd(cwd);
  stmgr->set_pid((sp_int32)ProcessUtils::getPid());
  stmgr->set_shell_port(shell_port_);
  for (auto iter = instances_.begin(); iter != instances_.end(); ++iter) {
    request->add_instances()->CopyFrom(*(*iter));
  }

  SendRequest(std::move(request), nullptr);
  return;
}

void TManagerClient::SetInstanceInfo(const std::vector<proto::system::Instance*>& _instances) {
    if (!instances_.empty()) {
      CleanInstances();
    }

    for (auto iter = _instances.begin(); iter != _instances.end(); ++iter) {
      auto instance = make_unique<proto::system::Instance>();
      instance->CopyFrom(*(*iter));
      instances_.insert(std::move(instance));
    }
}

void TManagerClient::SendHeartbeatRequest() {
  auto request = make_unique<proto::tmanager::StMgrHeartbeatRequest>();
  request->set_heartbeat_time(time(nullptr));
  // TODO(vikasr) Send actual stats
  request->mutable_stats();
  SendRequest(std::move(request), nullptr);
  return;
}

void TManagerClient::SavedInstanceState(const proto::system::Instance& _instance,
                                       const std::string& _checkpoint_id) {
  proto::ckptmgr::InstanceStateStored message;
  message.set_checkpoint_id(_checkpoint_id);
  message.mutable_instance()->CopyFrom(_instance);
  SendMessage(message);
}

void TManagerClient::SendRestoreTopologyStateResponse(proto::system::StatusCode _status,
                                                     const std::string& _ckpt_id,
                                                     sp_int64 _txid) {
  proto::ckptmgr::RestoreTopologyStateResponse message;
  message.mutable_status()->set_status(_status);
  message.set_checkpoint_id(_ckpt_id);
  message.set_restore_txid(_txid);
  SendMessage(message);
}

void TManagerClient::HandleRestoreTopologyStateRequest(
        pool_unique_ptr<proto::ckptmgr::RestoreTopologyStateRequest> _message) {
  restore_topology_watch_(_message->checkpoint_id(), _message->restore_txid());
}

void TManagerClient::HandleStartStmgrStatefulProcessing(
        pool_unique_ptr<proto::ckptmgr::StartStmgrStatefulProcessing> _message) {
  start_stateful_watch_(_message->checkpoint_id());
}

void TManagerClient::HandleStatefulCheckpointSavedMessage(
    pool_unique_ptr<proto::ckptmgr::StatefulConsistentCheckpointSaved> _msg) {
  broadcast_checkpoint_saved_(*_msg);
}

void TManagerClient::SendResetTopologyState(const std::string& _dead_stmgr,
                                           int32_t _dead_task,
                                           const std::string& _reason) {
  proto::ckptmgr::ResetTopologyState message;
  message.set_reason(_reason);
  if (!_dead_stmgr.empty()) {
    message.set_dead_stmgr(_dead_stmgr);
  }
  if (_dead_task >= 0) {
    message.set_dead_taskid(_dead_task);
  }
  SendMessage(message);
}
}  // namespace stmgr
}  // namespace heron
