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

#include "server/dummytmanager.h"
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

DummyTManager::DummyTManager(std::shared_ptr<EventLoop> eventLoop, const NetworkOptions& options)
  : Server(eventLoop, options) {
  InstallRequestHandler(&DummyTManager::HandleRegisterRequest);
}

DummyTManager::~DummyTManager() {}

void DummyTManager::HandleNewConnection(Connection* _conn) {
  // Do nothing
}

void DummyTManager::HandleConnectionClose(Connection* _conn, NetworkErrorCode) {
  // Do Nothing
}

void DummyTManager::HandleRegisterRequest(REQID _id, Connection* _conn,
                                   pool_unique_ptr<proto::tmanager::StMgrRegisterRequest> _request) {
  std::vector<std::shared_ptr<proto::system::Instance>> instances;
  stmgrs_[_request->stmgr().id()] =
          std::make_shared<tmanager::StMgrState>(_conn, _request->stmgr(), instances, *this);
  proto::tmanager::StMgrRegisterResponse response;
  response.mutable_status()->set_status(proto::system::OK);
  SendResponse(_id, _conn, response);
}

void DummyTManager::HandleHeartbeatRequest(REQID _id, Connection* _conn,
                                          proto::tmanager::StMgrHeartbeatRequest* _request) {
  delete _request;
  proto::tmanager::StMgrHeartbeatResponse response;
  response.mutable_status()->set_status(proto::system::OK);
  SendResponse(_id, _conn, response);
}

}  // namespace testing
}  // namespace heron
