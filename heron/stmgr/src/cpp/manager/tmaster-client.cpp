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

#include "manager/tmaster-client.h"
#include <stdio.h>
#include <string.h>
#include <unistd.h>
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

TMasterClient::TMasterClient(EventLoop* eventLoop, const NetworkOptions& _options,
                             const sp_string& _stmgr_id, sp_int32 _stmgr_port, sp_int32 _shell_port,
                             VCallback<proto::system::PhysicalPlan*> _pplan_watch)
    : Client(eventLoop, _options),
      stmgr_id_(_stmgr_id),
      stmgr_port_(_stmgr_port),
      shell_port_(_shell_port),
      to_die_(false),
      pplan_watch_(std::move(_pplan_watch)),
      reconnect_timer_id(0),
      heartbeat_timer_id(0) {
  reconnect_tmaster_interval_sec_ = config::HeronInternalsConfigReader::Instance()
                                        ->GetHeronStreammgrClientReconnectTmasterIntervalSec();
  stream_to_tmaster_heartbeat_interval_sec_ = config::HeronInternalsConfigReader::Instance()
                                                  ->GetHeronStreammgrTmasterHeartbeatIntervalSec();

  reconnect_timer_cb = [this]() { this->OnReConnectTimer(); };
  heartbeat_timer_cb = [this]() { this->OnHeartbeatTimer(); };

  char hostname[1024];
  CHECK_EQ(gethostname(hostname, 1023), 0);
  stmgr_host_ = hostname;
  InstallResponseHandler(new proto::tmaster::StMgrRegisterRequest(),
                         &TMasterClient::HandleRegisterResponse);
  InstallResponseHandler(new proto::tmaster::StMgrHeartbeatRequest(),
                         &TMasterClient::HandleHeartbeatResponse);
  InstallMessageHandler(&TMasterClient::HandleNewAssignmentMessage);
}

TMasterClient::~TMasterClient() {}

void TMasterClient::Die() {
  LOG(INFO) << "Tmaster client is being destroyed " << std::endl;
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

sp_string TMasterClient::getTmasterHostPort() {
  return options_.get_host() + ":" + std::to_string(options_.get_port());
}

void TMasterClient::HandleConnect(NetworkErrorCode _status) {
  if (_status == OK) {
    if (to_die_) {
      Stop();
      return;
    }
    LOG(INFO) << "Connected to tmaster running at " << get_clientoptions().get_host() << ":"
              << get_clientoptions().get_port() << std::endl;
    SendRegisterRequest();
  } else {
    if (to_die_) {
      delete this;
      return;
    }
    LOG(ERROR) << "Could not connect to tmaster at " << get_clientoptions().get_host() << ":"
               << get_clientoptions().get_port() << std::endl;
    LOG(INFO) << "Will retry again..." << std::endl;
    // Shouldn't be in a state where a previous timer is not cleared yet.
    CHECK_EQ(reconnect_timer_id, 0);
    reconnect_timer_id = AddTimer(reconnect_timer_cb, reconnect_tmaster_interval_sec_ * 1000000);
  }
}

void TMasterClient::HandleClose(NetworkErrorCode _code) {
  if (to_die_) {
    delete this;
    return;
  }
  LOG(INFO) << "TMaster connection closed with code " << _code << std::endl;
  LOG(INFO) << "Will try to reconnect again after " << reconnect_tmaster_interval_sec_ << "seconds"
            << std::endl;
  // Shouldn't be in a state where a previous timer is not cleared yet.
  CHECK_EQ(reconnect_timer_id, 0);

  // Remove the heartbeat timer since we have disconnected
  if (heartbeat_timer_id > 0) {
    RemoveTimer(heartbeat_timer_id);
    heartbeat_timer_id = 0;
  }

  reconnect_timer_id = AddTimer(reconnect_timer_cb, reconnect_tmaster_interval_sec_ * 1000000);
}

void TMasterClient::HandleRegisterResponse(void*, proto::tmaster::StMgrRegisterResponse* _response,
                                           NetworkErrorCode _status) {
  if (_status != OK) {
    LOG(ERROR) << "non ok network stack code for Register Response from Tmaster" << std::endl;
    delete _response;
    Stop();
    return;
  }
  proto::system::StatusCode status = _response->status().status();
  if (status != proto::system::OK) {
    LOG(ERROR) << "Register with Tmaster failed with status " << status << std::endl;
    Stop();
  } else {
    LOG(INFO) << "Registered successfully with Tmaster" << std::endl;
    if (_response->has_pplan()) {
      pplan_watch_(_response->release_pplan());
    }
    // Shouldn't be in a state where a previous timer is not cleared yet.
    CHECK_EQ(heartbeat_timer_id, 0);
    heartbeat_timer_id =
        AddTimer(heartbeat_timer_cb, stream_to_tmaster_heartbeat_interval_sec_ * 1000000);
  }
  delete _response;
}

void TMasterClient::HandleHeartbeatResponse(void*,
                                            proto::tmaster::StMgrHeartbeatResponse* _response,
                                            NetworkErrorCode _status) {
  if (_status != OK) {
    LOG(ERROR) << "NonOK response message for heartbeat Response" << std::endl;
    delete _response;
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
        AddTimer(heartbeat_timer_cb, stream_to_tmaster_heartbeat_interval_sec_ * 1000000);
  }
  delete _response;
}

void TMasterClient::HandleNewAssignmentMessage(proto::stmgr::NewPhysicalPlanMessage* _message) {
  LOG(INFO) << "Got a new assignment" << std::endl;
  pplan_watch_(_message->release_new_pplan());
  delete _message;
}

void TMasterClient::OnReConnectTimer() {
  // The timer has triggered the callback, so reset the timer_id;
  reconnect_timer_id = 0;
  Start();
}

void TMasterClient::OnHeartbeatTimer() {
  LOG(INFO) << "Sending heartbeat" << std::endl;
  // The timer has triggered the callback, so reset the timer_id;
  heartbeat_timer_id = 0;
  SendHeartbeatRequest();
}

void TMasterClient::SendRegisterRequest() {
  auto request = new proto::tmaster::StMgrRegisterRequest();

  sp_string cwd;
  FileUtils::getCwd(cwd);
  proto::system::StMgr* stmgr = request->mutable_stmgr();
  stmgr->set_id(stmgr_id_);
  stmgr->set_host_name(stmgr_host_);
  stmgr->set_data_port(stmgr_port_);
  stmgr->set_local_endpoint("/unused");
  stmgr->set_cwd(cwd);
  stmgr->set_pid((sp_int32)ProcessUtils::getPid());
  stmgr->set_shell_port(shell_port_);
  for (auto iter = instances_.begin(); iter != instances_.end(); ++iter) {
    request->add_instances()->CopyFrom(*(*iter));
  }
  SendRequest(request, NULL);
  return;
}

void TMasterClient::SendHeartbeatRequest() {
  auto request = new proto::tmaster::StMgrHeartbeatRequest();
  request->set_heartbeat_time(time(NULL));
  // TODO(vikasr) Send actual stats
  request->mutable_stats();
  SendRequest(request, NULL);
  return;
}

}  // namespace stmgr
}  // namespace heron
