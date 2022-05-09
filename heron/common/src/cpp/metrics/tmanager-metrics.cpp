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

#include "metrics/tmanager-metrics.h"
#include <stdio.h>
#include <unistd.h>
#include <iostream>
#include <list>
#include <map>
#include <string>
#include <utility>
#include "config/metrics-sinks-reader.h"
#include "proto/messages.h"
#include "basics/basics.h"
#include "errors/errors.h"
#include "threads/threads.h"
#include "network/network.h"

namespace heron {
namespace common {

using std::shared_ptr;

TManagerMetrics::TManagerMetrics(const sp_string& sinks_filename, shared_ptr<EventLoop> eventLoop) {
  sinks_reader_ = new config::MetricsSinksReader(eventLoop, sinks_filename);
  std::list<std::pair<sp_string, sp_string> > metrics;
  sinks_reader_->GetTManagerMetrics(metrics);
  for (auto iter = metrics.begin(); iter != metrics.end(); ++iter) {
    metrics_prefixes_[iter->first] = TranslateFromString(iter->second);
  }
}

TManagerMetrics::~TManagerMetrics() { delete sinks_reader_; }

bool TManagerMetrics::IsTManagerMetric(const sp_string& _name) {
  for (auto iter = metrics_prefixes_.begin(); iter != metrics_prefixes_.end(); ++iter) {
    if (_name.find(iter->first) == 0) return true;
  }
  return false;
}

TManagerMetrics::MetricAggregationType TManagerMetrics::GetAggregationType(const sp_string& _name) {
  for (auto iter = metrics_prefixes_.begin(); iter != metrics_prefixes_.end(); ++iter) {
    if (_name.find(iter->first) == 0) {
      return iter->second;
    }
  }
  return UNKNOWN;
}

TManagerMetrics::MetricAggregationType TManagerMetrics::TranslateFromString(const sp_string& type) {
  if (type == "SUM") {
    return SUM;
  } else if (type == "AVG") {
    return AVG;
  }  else if (type == "LAST") {
    return LAST;
  } else {
    LOG(FATAL) << "Unknown metrics type in metrics sinks " << type;
    return UNKNOWN;
  }
}
}  // namespace common
}  // namespace heron
