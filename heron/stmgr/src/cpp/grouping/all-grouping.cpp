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

#include "grouping/all-grouping.h"
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

AllGrouping::AllGrouping(const std::vector<sp_int32>& _task_ids) : Grouping(_task_ids) {}

AllGrouping::~AllGrouping() {}

void AllGrouping::GetListToSend(const proto::system::HeronDataTuple&,
                                std::vector<sp_int32>& _return) {
  for (sp_uint32 i = 0; i < task_ids_.size(); ++i) {
    _return.push_back(task_ids_[i]);
  }
}

}  // namespace stmgr
}  // namespace heron
