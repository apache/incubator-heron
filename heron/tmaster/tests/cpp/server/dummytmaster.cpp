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

#include "server/dummytmaster.h"
#include <stdio.h>
#include <iostream>
#include <string>
#include <vector>
#include "proto/messages.h"
#include "basics/basics.h"
#include "errors/errors.h"
#include "threads/threads.h"
#include "network/network.h"
#include "manager/stmgrstate.h"

namespace heron {
namespace testing {

DummyTMaster::DummyTMaster(EventLoop* eventLoop, const NetworkOptions& options)
  : Server(eventLoop, options) {
  InstallRequestHandler(&DummyTMaster::HandleRegisterRequest);
}

DummyTMaster::~DummyTMaster() {}

void DummyTMaster::HandleNewConnection(Connection* _conn) {
  // Do nothing
}

void DummyTMaster::HandleConnectionClose(Connection* _conn, NetworkErrorCode) {
  // Do Nothing
}

void DummyTMaster::HandleRegisterRequest(REQID _id, Connection* _conn,
                                         proto::tmaster::StMgrRegisterRequest* _request) {
  std::vector<proto::system::Instance*> instances;
  stmgrs_[_request->stmgr().id()] =
    new tmaster::StMgrState(_conn, _request->stmgr(), instances, this);
  delete _request;
  proto::tmaster::StMgrRegisterResponse response;
  response.mutable_status()->set_status(proto::system::OK);
  SendResponse(_id, _conn, response);
}

void DummyTMaster::HandleHeartbeatRequest(REQID _id, Connection* _conn,
                                          proto::tmaster::StMgrHeartbeatRequest* _request) {
  delete _request;
  proto::tmaster::StMgrHeartbeatResponse response;
  response.mutable_status()->set_status(proto::system::OK);
  SendResponse(_id, _conn, response);
}

}  // namespace testing
}  // namespace heron
