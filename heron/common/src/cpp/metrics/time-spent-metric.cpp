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
// time-spent-metric.cpp
//
// Please see time-spent-metric.h for details
//////////////////////////////////////////////////////////////////////////////
#include "metrics/time-spent-metric.h"
#include <string>
#include "basics/basics.h"
#include "errors/errors.h"
#include "metrics/imetric.h"
#include "network/network.h"
#include "proto/messages.h"
#include "threads/threads.h"

namespace heron {
namespace common {

TimeSpentMetric::TimeSpentMetric() : total_time_msecs_(0), started_(false) {}

TimeSpentMetric::~TimeSpentMetric() {}

using std::chrono::high_resolution_clock;
using std::chrono::duration_cast;
using std::chrono::milliseconds;

void TimeSpentMetric::Start() {
  if (!started_) {
    start_time_ = high_resolution_clock::now();
    started_ = true;
  }
  // If it is already started, just ignore.
}

void TimeSpentMetric::Stop() {
  if (started_) {
    auto end_time = high_resolution_clock::now();
    total_time_msecs_ += duration_cast<milliseconds>(end_time - start_time_).count();
    start_time_ = high_resolution_clock::now();
    started_ = false;
  }
  // If it is already stopped, just ignore.
}

void TimeSpentMetric::GetAndReset(const sp_string& _prefix,
                                  proto::system::MetricPublisherPublishMessage* _message) {
  if (started_) {
    auto now = high_resolution_clock::now();
    total_time_msecs_ += duration_cast<milliseconds>(now - start_time_).count();
    start_time_ = now;
  }
  proto::system::MetricDatum* d = _message->add_metrics();
  d->set_name(_prefix);
  d->set_value(std::to_string(total_time_msecs_));
  total_time_msecs_ = 0;
}
}  // namespace common
}  // namespace heron
