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
// multi-count-metric.h
//
// Duplication of MultiCountMetric.java
//////////////////////////////////////////////////////////////////////////////
#ifndef __MULTI_COUNT_METRIC_H_
#define __MULTI_COUNT_METRIC_H_

#include <map>
#include "metrics/count-metric.h"
#include "metrics/imetric.h"
#include "proto/messages.h"
#include "basics/basics.h"

namespace heron {
namespace common {

class MultiCountMetric : public IMetric {
 public:
  MultiCountMetric();
  virtual ~MultiCountMetric();
  CountMetric* scope(const sp_string& _key);
  virtual void GetAndReset(const sp_string& _prefix,
                           proto::system::MetricPublisherPublishMessage* _message);

 private:
  std::map<sp_string, CountMetric*> value_;
};
}  // namespace common
}  // namespace heron

#endif
