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
// tmanager-metric.h
//
// Defines all the metrics that needs to be sent to tmanager
//////////////////////////////////////////////////////////////////////////////
#ifndef __TMANAGER_METRICS_H_
#define __TMANAGER_METRICS_H_

#include <map>
#include "metrics/imetric.h"
#include "network/network.h"
#include "proto/messages.h"
#include "basics/basics.h"

namespace heron {
namespace config {
class MetricsSinksReader;
}
}

namespace heron {
namespace common {

class TManagerMetrics {
 public:
  enum MetricAggregationType {
    UNKNOWN = -1,
    SUM = 0,
    AVG,
    LAST  // We only care about the last value
  };
  TManagerMetrics(const sp_string& metrics_sinks, std::shared_ptr<EventLoop> eventLoop);
  ~TManagerMetrics();

  bool IsTManagerMetric(const sp_string& _name);
  MetricAggregationType GetAggregationType(const sp_string& _name);

 private:
  MetricAggregationType TranslateFromString(const sp_string& _type);
  // map from metric prefix to its aggregation form
  std::map<sp_string, MetricAggregationType> metrics_prefixes_;
  config::MetricsSinksReader* sinks_reader_;
};
}  // namespace common
}  // namespace heron

#endif
