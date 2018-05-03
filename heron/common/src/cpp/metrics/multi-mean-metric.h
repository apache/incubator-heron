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
// multi-mean-metric.h
//
// Extension of mean-metric.h
//////////////////////////////////////////////////////////////////////////////
#ifndef __MULTI_MEAN_METRIC_H_
#define __MULTI_MEAN_METRIC_H_

#include <map>
#include "metrics/mean-metric.h"
#include "metrics/imetric.h"
#include "proto/messages.h"
#include "basics/basics.h"

namespace heron {
namespace common {

class MultiMeanMetric : public IMetric {
 public:
  MultiMeanMetric();
  virtual ~MultiMeanMetric();
  MeanMetric* scope(const sp_string& _key);
  virtual void GetAndReset(const sp_string& _prefix,
                           proto::system::MetricPublisherPublishMessage* _message);

 private:
  std::map<sp_string, MeanMetric*> value_;
};
}  // namespace common
}  // namespace heron

#endif
