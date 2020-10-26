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

#include <iostream>
#include <vector>
#include "proto/messages.h"
#include "basics/basics.h"
#include "errors/errors.h"
#include "threads/threads.h"
#include "network/network.h"
#include "server/dummy_stmgr.h"

using std::shared_ptr;

///////////////////////////// DummyTManagerClient ///////////////////////////////////////////
DummyTManagerClient::DummyTManagerClient(
    shared_ptr<EventLoopImpl> eventLoop, const NetworkOptions& _options, const sp_string& stmgr_id,
    const sp_string& stmgr_host, sp_int32 stmgr_port, sp_int32 shell_port,
    const std::vector<shared_ptr<heron::proto::system::Instance>>& _instances)
    : Client(eventLoop, _options),
      stmgr_id_(stmgr_id),
      stmgr_host_(stmgr_host),
      stmgr_port_(stmgr_port),
      shell_port_(shell_port),
      instances_(_instances) {
  InstallResponseHandler(make_unique<heron::proto::tmanager::StMgrRegisterRequest>(),
                         &DummyTManagerClient::HandleRegisterResponse);
  // Setup the call back function to be invoked when retrying
  retry_cb_ = [this]() { this->Retry(); };
}

DummyTManagerClient::~DummyTManagerClient() {}

void DummyTManagerClient::HandleRegisterResponse(
    void*,
    pool_unique_ptr<heron::proto::tmanager::StMgrRegisterResponse> response,
    NetworkErrorCode) {
}

void DummyTManagerClient::HandleConnect(NetworkErrorCode _status) {
  if (_status == OK) {
    CreateAndSendRegisterRequest();
  } else {
    // Retry after some time
    AddTimer(retry_cb_, 100);
  }
}

void DummyTManagerClient::HandleClose(NetworkErrorCode) {}

void DummyTManagerClient::CreateAndSendRegisterRequest() {
  auto request = make_unique<heron::proto::tmanager::StMgrRegisterRequest>();
  heron::proto::system::StMgr* stmgr = request->mutable_stmgr();
  sp_string cwd;
  stmgr->set_id(stmgr_id_);
  stmgr->set_host_name(stmgr_host_);
  stmgr->set_data_port(stmgr_port_);
  stmgr->set_local_endpoint("/unused");
  stmgr->set_cwd(cwd);
  stmgr->set_pid((sp_int32)ProcessUtils::getPid());
  stmgr->set_shell_port(shell_port_);
  for (auto iter = instances_.begin(); iter != instances_.end(); ++iter) {
    request->add_instances()->CopyFrom(**iter);
  }
  SendRequest(std::move(request), nullptr);
}

///////////////////////////// DummyStMgr /////////////////////////////////////////////////
DummyStMgr::DummyStMgr(shared_ptr<EventLoopImpl> ss, const NetworkOptions& options,
                       const sp_string& stmgr_id,
                       const sp_string& stmgr_host, sp_int32 stmgr_port,
                       const sp_string& tmanager_host, sp_int32 tmanager_port, sp_int32 shell_port,
                       const std::vector<shared_ptr<heron::proto::system::Instance>>& _instances)
    : Server(ss, options), num_start_bp_(0), num_stop_bp_(0) {
  NetworkOptions tmanager_options;
  tmanager_options.set_host(tmanager_host);
  tmanager_options.set_port(tmanager_port);
  tmanager_options.set_max_packet_size(1_MB);
  tmanager_options.set_socket_family(PF_INET);

  tmanager_client_ = new DummyTManagerClient(ss, tmanager_options, stmgr_id, stmgr_host, stmgr_port,
                                           shell_port, _instances);
  InstallRequestHandler(&DummyStMgr::HandleStMgrHelloRequest);
  InstallMessageHandler(&DummyStMgr::HandleStartBackPressureMessage);
  InstallMessageHandler(&DummyStMgr::HandleStopBackPressureMessage);
}

DummyStMgr::~DummyStMgr() {
  tmanager_client_->Stop();
  delete tmanager_client_;
}

sp_int32 DummyStMgr::Start() {
  if (SP_OK == Server::Start()) {
    tmanager_client_->setStmgrPort(get_serveroptions().get_port());
    tmanager_client_->Start();
    return SP_OK;
  } else {
    return SP_NOTOK;
  }
}

void DummyStMgr::HandleNewConnection(Connection* conn) {}

void DummyStMgr::HandleConnectionClose(Connection*, NetworkErrorCode) {}

void DummyStMgr::HandleStMgrHelloRequest(REQID _id, Connection* _conn,
                               pool_unique_ptr<heron::proto::stmgr::StrMgrHelloRequest> _request) {
  other_stmgrs_ids_.push_back(_request->stmgr());
  heron::proto::stmgr::StrMgrHelloResponse response;
  response.mutable_status()->set_status(heron::proto::system::OK);
  SendResponse(_id, _conn, response);
}

void DummyStMgr::HandleStartBackPressureMessage(Connection*,
                                  pool_unique_ptr<heron::proto::stmgr::StartBackPressureMessage>) {
  ++num_start_bp_;
}

void DummyStMgr::HandleStopBackPressureMessage(Connection*,
                                   pool_unique_ptr<heron::proto::stmgr::StopBackPressureMessage>) {
  ++num_stop_bp_;
}
