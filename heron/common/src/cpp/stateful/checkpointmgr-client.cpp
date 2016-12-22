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

#include "stateful/checkpointmgr-client.h"
#include <stdio.h>
#include <iostream>
#include <string>
#include "proto/messages.h"
#include "basics/basics.h"
#include "errors/errors.h"
#include "threads/threads.h"
#include "network/network.h"

namespace heron {
namespace common {

CheckpointMgrClient::CheckpointMgrClient(EventLoop* eventLoop,
                                         const NetworkOptions& _options)
    : Client(eventLoop, _options) {
  Start();
}

CheckpointMgrClient::~CheckpointMgrClient() { }

void CheckpointMgrClient::HandleConnect(NetworkErrorCode _status) {
  if (_status == OK) {
    LOG(INFO) << "Connected to Checkpoint manager" << std::endl;
  } else {
    LOG(ERROR) << "Could not connect to metrics mgr. Will Retry in 1 second\n";
    AddTimer([this]() { this->ReConnect(); }, 1000000);
  }
}

void CheckpointMgrClient::HandleClose(NetworkErrorCode) {
  LOG(ERROR) << "Metrics Mgr closed connection on us. Will Retry in 1 second\n";
  AddTimer([this]() { this->ReConnect(); }, 1000000);
}

void CheckpointMgrClient::ReConnect() { Start(); }

void CheckpointMgrClient::SaveStateCheckpoint(proto::stmgr::SaveStateCheckpoint* _message) {
  SendMessage(*_message);

  delete _message;
}

}  // namespace common
}  // namespace heron
