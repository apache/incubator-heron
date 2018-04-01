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
// assignable-metric.cpp
//
// Please see assignable-metric.h for details
//////////////////////////////////////////////////////////////////////////////

#include "metrics/assignable-metric.h"
#include <string>
#include "metrics/imetric.h"
#include "basics/basics.h"
#include "errors/errors.h"
#include "proto/messages.h"
#include "network/network.h"
#include "threads/threads.h"

namespace heron {
namespace common {

AssignableMetric::AssignableMetric(sp_int64 _value) { value_ = _value; }

AssignableMetric::~AssignableMetric() {}

void AssignableMetric::SetValue(sp_int64 _value) { value_ = _value; }
void AssignableMetric::GetAndReset(const sp_string& _prefix,
                                   proto::system::MetricPublisherPublishMessage* _message) {
  proto::system::MetricDatum* d = _message->add_metrics();
  d->set_name(_prefix);
  d->set_value(std::to_string(value_));
}
}  // namespace common
}  // namespace heron
