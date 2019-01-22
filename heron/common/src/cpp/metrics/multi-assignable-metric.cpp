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

//////////////////////////////////////////////////////////////////////////////
//
// multi-assignable-metric.cpp
//
// Please see multi-assignable-metric.h for details
//////////////////////////////////////////////////////////////////////////////
#include "metrics/multi-assignable-metric.h"
#include <map>
#include "metrics/assignable-metric.h"
#include "proto/messages.h"
#include "basics/basics.h"
#include "errors/errors.h"
#include "threads/threads.h"
#include "network/network.h"

namespace heron {
namespace common {

MultiAssignableMetric::MultiAssignableMetric() {}

MultiAssignableMetric::~MultiAssignableMetric() {
  for (auto iter = value_.begin(); iter != value_.end(); ++iter) {
    delete iter->second;
  }
}

AssignableMetric* MultiAssignableMetric::scope(const sp_string& _key) {
  auto iter = value_.find(_key);
  if (iter == value_.end()) {
    auto m = new AssignableMetric(0);
    value_[_key] = m;
    return m;
  } else {
    return iter->second;
  }
}

void MultiAssignableMetric::GetAndReset(const sp_string& _prefix,
                                        proto::system::MetricPublisherPublishMessage* _message) {
  for (auto iter = value_.begin(); iter != value_.end(); ++iter) {
    iter->second->GetAndReset(_prefix + "/" + iter->first, _message);
  }
}
}  // namespace common
}  // namespace heron
