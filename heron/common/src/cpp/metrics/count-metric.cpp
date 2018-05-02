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
// count-metric.cpp
//
// Please see count-metric.h for details
//////////////////////////////////////////////////////////////////////////////

#include "metrics/count-metric.h"
#include <string>
#include "metrics/imetric.h"
#include "proto/messages.h"
#include "basics/basics.h"
#include "errors/errors.h"
#include "threads/threads.h"
#include "network/network.h"

namespace heron {
namespace common {

CountMetric::CountMetric() : value_(0) {}

CountMetric::~CountMetric() {}

void CountMetric::incr() { value_++; }

void CountMetric::incr_by(sp_int64 _by) { value_ += _by; }

void CountMetric::GetAndReset(const sp_string& _prefix,
                              proto::system::MetricPublisherPublishMessage* _message) {
  proto::system::MetricDatum* d = _message->add_metrics();
  d->set_name(_prefix);
  d->set_value(std::to_string(value_));
  value_ = 0;
}
}  // namespace common
}  // namespace heron
