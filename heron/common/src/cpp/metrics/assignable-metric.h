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
// assignable-metric.h
//
// Duplicate of Assignable metric from Java API
//////////////////////////////////////////////////////////////////////////////
#if !defined(__ASSIGNABLE_METRIC_H_)
#define __ASSIGNABLE_METRIC_H_

#include "metrics/imetric.h"
#include "proto/messages.h"
#include "basics/basics.h"

namespace heron {
namespace common {

class AssignableMetric : public IMetric {
 public:
  explicit AssignableMetric(sp_int64 _value);
  virtual ~AssignableMetric();

  virtual void SetValue(sp_int64 _value);
  virtual void GetAndReset(const sp_string& _prefix,
                           proto::system::MetricPublisherPublishMessage* _message);

 private:
  sp_int64 value_;
};
}  // namespace common
}  // namespace heron

#endif
