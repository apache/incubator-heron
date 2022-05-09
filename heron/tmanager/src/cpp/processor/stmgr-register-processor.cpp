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

#include "processor/stmgr-register-processor.h"
#include <iostream>
#include <vector>
#include "processor/tmanager-processor.h"
#include "manager/tmanager.h"
#include "proto/messages.h"
#include "basics/basics.h"
#include "errors/errors.h"
#include "threads/threads.h"
#include "network/network.h"

namespace heron {
namespace tmanager {

StMgrRegisterProcessor::StMgrRegisterProcessor(REQID _reqid, Connection* _conn,
                                     pool_unique_ptr<proto::tmanager::StMgrRegisterRequest> _request,
                                     TManager* _tmanager, Server* _server)
    : Processor(_reqid, _conn, std::move(_request), _tmanager, _server) {}

StMgrRegisterProcessor::~StMgrRegisterProcessor() {
  // nothing to be done here
}

void StMgrRegisterProcessor::Start() {
  // We got a new stream manager registering to us
  // Get the relevant info and ask tmanager to register
  proto::tmanager::StMgrRegisterRequest* request =
      static_cast<proto::tmanager::StMgrRegisterRequest*>(request_.get());
  std::vector<shared_ptr<proto::system::Instance>> instances;
  for (sp_int32 i = 0; i < request->instances_size(); ++i) {
    auto instance = std::make_shared<proto::system::Instance>();
    instance->CopyFrom(request->instances(i));
    instances.push_back(instance);
  }

  shared_ptr<proto::system::PhysicalPlan> pplan;

  proto::system::Status* status =
      tmanager_->RegisterStMgr(request->stmgr(), instances, GetConnection(), pplan);

  // Send the response
  proto::tmanager::StMgrRegisterResponse response;
  response.set_allocated_status(status);
  if (status->status() == proto::system::OK) {
    if (pplan) {
      response.mutable_pplan()->CopyFrom(*pplan);
    }
  }
  SendResponse(response);
  return;
}
}  // namespace tmanager
}  // namespace heron
