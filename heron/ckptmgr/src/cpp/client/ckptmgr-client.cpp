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

#include "client/ckptmgr-client.h"
#include <iostream>
#include <string>
#include "basics/basics.h"
#include "proto/messages.h"
#include "errors/errors.h"
#include "threads/threads.h"
#include "network/network.h"
#include "config/heron-internals-config-reader.h"

namespace heron {
namespace ckptmgr {

CkptMgrClient::CkptMgrClient(EventLoop* eventloop, const NetworkOptions& _options,
                             const sp_string& _topology_name, const sp_string& _topology_id,
                             const sp_string& _ckptmgr_id, const sp_string& _stmgr_id)
    : Client(eventloop, _options),
      topology_name_(_topology_name),
      topology_id_(_topology_id),
      ckptmgr_id_(_ckptmgr_id),
      stmgr_id_(_stmgr_id),
      quit_(false) {

  reconnect_cpktmgr_interval_sec_ =
    config::HeronInternalsConfigReader::Instance()->GetHeronStreammgrClientReconnectIntervalSec();

  InstallResponseHandler(new proto::ckptmgr::RegisterStMgrRequest(),
                         &CkptMgrClient::HandleStMgrRegisterResponse);
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
    LOG(INFO) << "Connected to ckptmgr " << ckptmgr_id_ << " running at"
              << get_clientoptions().get_host() << ":" << get_clientoptions().get_port()
              << std::endl;
    if (quit_) {
      Stop();
    } else {
      SendRegisterRequest();
    }
  } else {
    LOG(WARNING) << "Could not connect to cpktmgr" << " running at "
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
    LOG(INFO) << "We closed our server connection with cpktmgr " << ckptmgr_id_ << "running at "
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

void CkptMgrClient::HandleStMgrRegisterResponse(void*,
                                                proto::ckptmgr::RegisterStMgrResponse* _response,
                                                NetworkErrorCode _status) {
  if (_status != OK) {
    LOG(ERROR) << "NonOk network code " << _status << " for register response from ckptmgr "
               << ckptmgr_id_ << "running at " << get_clientoptions().get_host() << ":"
               << get_clientoptions().get_port();
    delete _response;
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
    LOG(INFO) << "Hello request got response OK from ckptmgr" << ckptmgr_id_
              << " running at " << get_clientoptions().get_host() << ":"
              << get_clientoptions().get_port();
  }
  delete _response;
}

void CkptMgrClient::OnReconnectTimer() { Start(); }

void CkptMgrClient::SendRegisterRequest() {
  auto request = new proto::ckptmgr::RegisterStMgrRequest();
  request->set_topology_name(topology_name_);
  request->set_topology_id(topology_id_);
  request->set_stmgr(stmgr_id_);
  SendRequest(request, NULL);
  return;
}


void CkptMgrClient::SaveStateCheckpoint(proto::ckptmgr::SaveStateCheckpoint* _message) {
  SendMessage(*_message);

  delete _message;
}

}  // namespace ckptmgr
}  // namespace heron

