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
// time-spent-metric.h
//
// This metric keeps track of the total time (in msecs) spent in a state. The
// state transitions can be marked with the Start() and Stop() APIs. The total
// time is tracked for every window until the metric is purged at which
// time the total time is reset to 0.
//////////////////////////////////////////////////////////////////////////////
#ifndef __TIME_SPENT_METRIC_H_
#define __TIME_SPENT_METRIC_H_

#include <chrono>
#include "basics/basics.h"
#include "metrics/imetric.h"
#include "proto/messages.h"

namespace heron {
namespace common {

class TimeSpentMetric : public IMetric {
 public:
  TimeSpentMetric();
  virtual ~TimeSpentMetric();
  void Start();
  void Stop();
  virtual void GetAndReset(const sp_string& _prefix,
                           proto::system::MetricPublisherPublishMessage* _message);

 private:
  sp_int64 total_time_msecs_;
  std::chrono::high_resolution_clock::time_point start_time_;
  bool started_;
};
}  // namespace common
}  // namespace heron

#endif
