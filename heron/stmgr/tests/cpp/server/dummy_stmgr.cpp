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

#include <iostream>
#include <vector>
#include "proto/messages.h"
#include "basics/basics.h"
#include "errors/errors.h"
#include "threads/threads.h"
#include "network/network.h"
#include "server/dummy_stmgr.h"

///////////////////////////// DummyTMasterClient ///////////////////////////////////////////
DummyTMasterClient::DummyTMasterClient(
    EventLoopImpl* eventLoop, const NetworkOptions& _options, const sp_string& stmgr_id,
    const sp_string& stmgr_host, sp_int32 stmgr_port, sp_int32 shell_port,
    const std::vector<heron::proto::system::Instance*>& _instances)
    : Client(eventLoop, _options),
      stmgr_id_(stmgr_id),
      stmgr_host_(stmgr_host),
      stmgr_port_(stmgr_port),
      shell_port_(shell_port),
      instances_(_instances) {
  InstallResponseHandler(new heron::proto::tmaster::StMgrRegisterRequest(),
                         &DummyTMasterClient::HandleRegisterResponse);
  // Setup the call back function to be invoked when retrying
  retry_cb_ = [this]() { this->Retry(); };
}

DummyTMasterClient::~DummyTMasterClient() {}

void DummyTMasterClient::HandleRegisterResponse(
    void*, heron::proto::tmaster::StMgrRegisterResponse* response, NetworkErrorCode) {
  delete response;
}

void DummyTMasterClient::HandleConnect(NetworkErrorCode _status) {
  if (_status == OK) {
    CreateAndSendRegisterRequest();
  } else {
    // Retry after some time
    AddTimer(retry_cb_, 100);
  }
}

void DummyTMasterClient::HandleClose(NetworkErrorCode) {}

void DummyTMasterClient::CreateAndSendRegisterRequest() {
  heron::proto::tmaster::StMgrRegisterRequest* request =
      new heron::proto::tmaster::StMgrRegisterRequest();
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
  SendRequest(request, NULL);
}

///////////////////////////// DummyStMgr /////////////////////////////////////////////////
DummyStMgr::DummyStMgr(EventLoopImpl* ss, const NetworkOptions& options, const sp_string& stmgr_id,
                       const sp_string& stmgr_host, sp_int32 stmgr_port,
                       const sp_string& tmaster_host, sp_int32 tmaster_port, sp_int32 shell_port,
                       const std::vector<heron::proto::system::Instance*>& _instances)
    : Server(ss, options), num_start_bp_(0), num_stop_bp_(0) {
  NetworkOptions tmaster_options;
  tmaster_options.set_host(tmaster_host);
  tmaster_options.set_port(tmaster_port);
  tmaster_options.set_max_packet_size(1024 * 1024);
  tmaster_options.set_socket_family(PF_INET);

  tmaster_client_ = new DummyTMasterClient(ss, tmaster_options, stmgr_id, stmgr_host, stmgr_port,
                                           shell_port, _instances);
  tmaster_client_->Start();
  InstallRequestHandler(&DummyStMgr::HandleStMgrHelloRequest);
  InstallMessageHandler(&DummyStMgr::HandleTupleStreamMessage);
  InstallMessageHandler(&DummyStMgr::HandleStartBackPressureMessage);
  InstallMessageHandler(&DummyStMgr::HandleStopBackPressureMessage);
}

DummyStMgr::~DummyStMgr() {
  tmaster_client_->Stop();
  delete tmaster_client_;
}

void DummyStMgr::HandleNewConnection(Connection* conn) {}

void DummyStMgr::HandleConnectionClose(Connection*, NetworkErrorCode) {}

void DummyStMgr::HandleStMgrHelloRequest(REQID _id, Connection* _conn,
                                         heron::proto::stmgr::StrMgrHelloRequest* _request) {
  other_stmgrs_ids_.push_back(_request->stmgr());
  heron::proto::stmgr::StrMgrHelloResponse response;
  response.mutable_status()->set_status(heron::proto::system::OK);
  SendResponse(_id, _conn, response);
  delete _request;
}

void DummyStMgr::HandleTupleStreamMessage(Connection*, heron::proto::stmgr::TupleStreamMessage*) {}

void DummyStMgr::HandleStartBackPressureMessage(Connection*,
                                                heron::proto::stmgr::StartBackPressureMessage*) {
  ++num_start_bp_;
}

void DummyStMgr::HandleStopBackPressureMessage(Connection*,
                                               heron::proto::stmgr::StopBackPressureMessage*) {
  ++num_stop_bp_;
}
