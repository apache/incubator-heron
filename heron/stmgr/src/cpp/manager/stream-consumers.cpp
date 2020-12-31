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

#include "manager/stream-consumers.h"
#include <functional>
#include <iostream>
#include <list>
#include <vector>
#include "grouping/grouping.h"
#include "proto/messages.h"
#include "basics/basics.h"
#include "errors/errors.h"
#include "threads/threads.h"
#include "network/network.h"

namespace heron {
namespace stmgr {

StreamConsumers::StreamConsumers(const proto::api::InputStream& _is,
                                 const proto::api::StreamSchema& _schema,
                                 const std::vector<sp_int32>& _task_ids) {
  consumers_.push_back(Grouping::Create(_is.gtype(), _is, _schema, _task_ids));
}

StreamConsumers::~StreamConsumers() {
  while (!consumers_.empty()) {
    auto c = std::move(consumers_.front());
    consumers_.pop_front();
  }
}

void StreamConsumers::NewConsumer(const proto::api::InputStream& _is,
                                  const proto::api::StreamSchema& _schema,
                                  const std::vector<sp_int32>& _task_ids) {
  consumers_.push_back(Grouping::Create(_is.gtype(), _is, _schema, _task_ids));
}

void StreamConsumers::GetListToSend(const proto::system::HeronDataTuple& _tuple,
                                    std::vector<sp_int32>& _return) {
  for (auto iter = consumers_.begin(); iter != consumers_.end(); ++iter) {
    (*iter)->GetListToSend(_tuple, _return);
  }
}
}  // namespace stmgr
}  // namespace heron
