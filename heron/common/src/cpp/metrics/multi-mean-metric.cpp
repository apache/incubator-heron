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

//////////////////////////////////////////////////////////////////////////////
//
// multi-mean-metric.cpp
//
// Please see multi-mean-metric.cpp for details
//////////////////////////////////////////////////////////////////////////////
#include "metrics/multi-mean-metric.h"
#include <map>
#include "metrics/imetric.h"
#include "metrics/mean-metric.h"
#include "proto/messages.h"
#include "basics/basics.h"
#include "errors/errors.h"
#include "threads/threads.h"
#include "network/network.h"

namespace heron {
namespace common {

MultiMeanMetric::MultiMeanMetric() {}

MultiMeanMetric::~MultiMeanMetric() {
  for (auto iter = value_.begin(); iter != value_.end(); ++iter) {
    delete iter->second;
  }
}

MeanMetric* MultiMeanMetric::scope(const sp_string& _key) {
  auto iter = value_.find(_key);
  if (iter == value_.end()) {
    auto m = new MeanMetric();
    value_[_key] = m;
    return m;
  } else {
    return iter->second;
  }
}
void MultiMeanMetric::GetAndReset(const sp_string& _prefix,
                                  proto::system::MetricPublisherPublishMessage* _message) {
  for (auto iter = value_.begin(); iter != value_.end(); ++iter) {
    iter->second->GetAndReset(_prefix + "/" + iter->first, _message);
  }
}
}  // namespace common
}  // namespace heron
