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

#include "manager/ckptmgr-client.h"
#include <iostream>
#include <string>
#include "errors/errors.h"
#include "threads/threads.h"
#include "config/heron-internals-config-reader.h"

namespace heron {
namespace stmgr {

using std::unique_ptr;
using proto::ckptmgr::SaveInstanceStateRequest;

CkptMgrClient::CkptMgrClient(std::shared_ptr<EventLoop> eventloop, const NetworkOptions& _options,
                             const sp_string& _topology_name, const sp_string& _topology_id,
                             const sp_string& _ckptmgr_id, const sp_string& _stmgr_id,
                             std::function<void(const proto::system::Instance&,
                                                const std::string&)> _ckpt_saved_watcher,
                             std::function<void(proto::system::StatusCode, sp_int32, sp_string,
                               const proto::ckptmgr::InstanceStateCheckpoint&)>
                               _ckpt_get_watcher,
                             std::function<void()> _register_watcher)
    : Client(eventloop, _options),
      topology_name_(_topology_name),
      topology_id_(_topology_id),
      ckptmgr_id_(_ckptmgr_id),
      stmgr_id_(_stmgr_id),
      quit_(false),
      ckpt_saved_watcher_(_ckpt_saved_watcher),
      ckpt_get_watcher_(_ckpt_get_watcher),
      register_watcher_(_register_watcher),
      pplan_(nullptr) {

  // TODO(nlu): take the value from config
  reconnect_cpktmgr_interval_sec_ = 10;

  InstallResponseHandler(make_unique<proto::ckptmgr::RegisterStMgrRequest>(),
                         &CkptMgrClient::HandleRegisterStMgrResponse);
  InstallResponseHandler(make_unique<proto::ckptmgr::SaveInstanceStateRequest>(),
                         &CkptMgrClient::HandleSaveInstanceStateResponse);
  InstallResponseHandler(make_unique<proto::ckptmgr::GetInstanceStateRequest>(),
                         &CkptMgrClient::HandleGetInstanceStateResponse);
}

CkptMgrClient::~CkptMgrClient() {
  Stop();
}

void CkptMgrClient::Quit() {
  quit_ = true;
  Stop();
}

void CkptMgrClient::HandleConnect(NetworkErrorCode _status) {
  if (_status == OK) {
    LOG(INFO) << "Connected to ckptmgr " << ckptmgr_id_ << " running at "
              << get_clientoptions().get_host() << ":" << get_clientoptions().get_port()
              << std::endl;
    if (quit_) {
      Stop();
    } else {
      SendRegisterRequest();
    }
  } else {
    LOG(WARNING) << "Could not connect to cpktmgr" << ckptmgr_id_ << " running at "
                 << get_clientoptions().get_host() << ":" << get_clientoptions().get_port()
                 << " due to: " << _status << std::endl;
    if (quit_) {
      LOG(ERROR) << "Quitting";
      delete this;
      return;
    } else {
      LOG(INFO) << "Retrying again..." << std::endl;
      AddTimer([this]() { this->OnReconnectTimer(); },
               reconnect_cpktmgr_interval_sec_ * 1000 * 1000);
    }
  }
}

void CkptMgrClient::HandleClose(NetworkErrorCode _code) {
  if (_code == OK) {
    LOG(INFO) << "We closed our server connection with cpktmgr " << ckptmgr_id_ << " running at "
              << get_clientoptions().get_host() << ":" << get_clientoptions().get_port()
              << std::endl;
  } else {
    LOG(INFO) << "Ckptmgr" << ckptmgr_id_ << " running at "
              << get_clientoptions().get_host() << ":" << get_clientoptions().get_port()
              << " closed connection with code " << _code << std::endl;
  }
  if (quit_) {
    delete this;
  } else {
    LOG(INFO) << "Will try to reconnect again..." << std::endl;
    AddTimer([this]() { this->OnReconnectTimer(); },
             reconnect_cpktmgr_interval_sec_ * 1000 * 1000);
  }
}

void CkptMgrClient::HandleRegisterStMgrResponse(
                                  void*,
                                  pool_unique_ptr<proto::ckptmgr::RegisterStMgrResponse> _response,
                                  NetworkErrorCode _status) {
  if (_status != OK) {
    LOG(ERROR) << "NonOK network code " << _status << " for register response from ckptmgr "
               << ckptmgr_id_ << "running at " << get_clientoptions().get_host() << ":"
               << get_clientoptions().get_port();
    Stop();
    return;
  }

  proto::system::StatusCode status = _response->status().status();

  if (status != proto::system::OK) {
    LOG(ERROR) << "NonOK register response " << status << " from ckptmgr " << ckptmgr_id_
               << " running at " << get_clientoptions().get_host() << ":"
               << get_clientoptions().get_port();
    Stop();
  } else {
    LOG(INFO) << "Register request got response OK from ckptmgr " << ckptmgr_id_
              << " running at " << get_clientoptions().get_host() << ":"
              << get_clientoptions().get_port();
    register_watcher_();
  }
}

void CkptMgrClient::OnReconnectTimer() { Start(); }

void CkptMgrClient::SendRegisterRequest() {
  auto request = make_unique<proto::ckptmgr::RegisterStMgrRequest>();
  request->set_topology_name(topology_name_);
  request->set_topology_id(topology_id_);
  request->set_stmgr_id(stmgr_id_);
  request->mutable_physical_plan()->CopyFrom(*pplan_);
  SendRequest(std::move(request), nullptr);
}

void CkptMgrClient::SaveInstanceState(unique_ptr<SaveInstanceStateRequest> _request) {
  LOG(INFO) << "Sending SaveInstanceState to ckptmgr" << std::endl;
  SendRequest(std::move(_request), nullptr);
}

void CkptMgrClient::SetPhysicalPlan(proto::system::PhysicalPlan& _pplan) {
  pplan_ = &_pplan;
}

void CkptMgrClient::GetInstanceState(const proto::system::Instance& _instance,
                                     const std::string& _checkpoint_id) {
  LOG(INFO) << "Sending GetInstanceState to ckptmgr for task_id " << _instance.info().task_id()
            << " and checkpoint_id " << _checkpoint_id;
  int32_t* nattempts = new int32_t;
  *nattempts = 0;
  return GetInstanceState(_instance, _checkpoint_id, nattempts);
}

void CkptMgrClient::GetInstanceState(const proto::system::Instance& _instance,
                                     const std::string& _checkpoint_id,
                                     int32_t* _nattempts) {
  auto request = make_unique<proto::ckptmgr::GetInstanceStateRequest>();
  request->mutable_instance()->CopyFrom(_instance);
  request->set_checkpoint_id(_checkpoint_id);
  SendRequest(std::move(request), _nattempts);
}

void CkptMgrClient::HandleSaveInstanceStateResponse(
                             void*,
                             pool_unique_ptr<proto::ckptmgr::SaveInstanceStateResponse> _response,
                             NetworkErrorCode _status) {
  if (_status != OK) {
    LOG(ERROR) << "NonOK response message for SaveInstanceStateResponse";
    Stop();
    return;
  }

  if (_response->status().status() != proto::system::OK) {
    LOG(ERROR) << "CkptMgr could not save " << _response->status().status();
    return;
  }

  ckpt_saved_watcher_(_response->instance(), _response->checkpoint_id());
}

void CkptMgrClient::HandleGetInstanceStateResponse(
                             void* _ctx,
                             pool_unique_ptr<proto::ckptmgr::GetInstanceStateResponse> _response,
                             NetworkErrorCode _status) {
  int32_t* nattempts = static_cast<int32_t*>(_ctx);
  if (_status != OK) {
    LOG(ERROR) << "NonOK response message for GetInstanceStateResponse";
    delete nattempts;
    Stop();
    return;
  }
  if (_response->status().status() != proto::system::OK) {
    LOG(ERROR) << "CkptMgr could not get checkpoint for "
               << _response->instance().info().task_id()
               << " and checkpoint_id " << _response->checkpoint_id()
               << " because of reason: " << _response->status().status();
    *nattempts = *nattempts + 1;
    if (*nattempts >= 5) {
      LOG(ERROR) << "Not Retrying because already tried too many times";
      delete nattempts;
      ckpt_get_watcher_(_response->status().status(),
                        _response->instance().info().task_id(),
                        _response->checkpoint_id(), _response->checkpoint());
    } else {
      LOG(INFO) << "Retrying...";
      GetInstanceState(_response->instance(), _response->checkpoint_id(), nattempts);
    }
  } else {
    ckpt_get_watcher_(_response->status().status(),
                      _response->instance().info().task_id(), _response->checkpoint_id(),
                      _response->checkpoint());
  }
}
}  // namespace stmgr
}  // namespace heron
