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

#include "processor/stmgr-heartbeat-processor.h"
#include <iostream>
#include "processor/tmaster-processor.h"
#include "manager/tmaster.h"
#include "proto/messages.h"
#include "basics/basics.h"
#include "errors/errors.h"
#include "threads/threads.h"
#include "network/network.h"

namespace heron {
namespace tmaster {

StMgrHeartbeatProcessor::StMgrHeartbeatProcessor(REQID reqid, Connection* conn,
                                                 proto::tmaster::StMgrHeartbeatRequest* request,
                                                 TMaster* tmaster, Server* server)
    : Processor(reqid, conn, request, tmaster, server) {}

StMgrHeartbeatProcessor::~StMgrHeartbeatProcessor() {
  // nothing to be done here
}

void StMgrHeartbeatProcessor::Start() {
  proto::tmaster::StMgrHeartbeatRequest* request =
      static_cast<proto::tmaster::StMgrHeartbeatRequest*>(request_);

  proto::system::Status* status = tmaster_->UpdateStMgrHeartbeat(
      GetConnection(), request->heartbeat_time(), request->release_stats());

  proto::tmaster::StMgrHeartbeatResponse response;
  response.set_allocated_status(status);
  SendResponse(response);
  delete this;
}
}  // namespace tmaster
}  // namespace heron
