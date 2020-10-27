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

#include "processor/tmanager-processor.h"
#include <iostream>
#include "proto/messages.h"
#include "basics/basics.h"
#include "errors/errors.h"
#include "threads/threads.h"
#include "network/network.h"

namespace heron {
namespace tmanager {

Processor::Processor(REQID _reqid, Connection* _conn,
                     pool_unique_ptr<google::protobuf::Message> _request,
                     TManager* _tmanager, Server* _server)
    : request_(std::move(_request)), tmanager_(_tmanager), server_(_server),
      reqid_(_reqid), conn_(_conn) {}

Processor::~Processor() {}

void Processor::SendResponse(const google::protobuf::Message& _response) {
  server_->SendResponse(reqid_, conn_, _response);
}

void Processor::CloseConnection() { server_->CloseConnection(conn_); }
}  // namespace tmanager
}  // namespace heron
