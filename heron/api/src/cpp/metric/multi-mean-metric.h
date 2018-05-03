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

#ifndef HERON_API_METRIC_MULTI_MEAN_METRIC_H_
#define HERON_API_METRIC_MULTI_MEAN_METRIC_H_

#include <string>
#include <map>

#include "metric/imetric.h"
#include "metric/imulti-metric.h"
#include "metric/mean-metric.h"

namespace heron {
namespace api {
namespace metric {

/**
 * Interface for a bunch of name referenced mean metrics
 */
class MultiMeanMetric : public IMultiMetric {
 public:
  MultiMeanMetric() { }
  virtual ~MultiMeanMetric() { }
  std::shared_ptr<MeanMetric> scope(const std::string& key) {
    auto iter = metrics_.begin();
    if (iter == metrics_.end()) {
      std::shared_ptr<MeanMetric> m(new MeanMetric);
      metrics_[key] = m;
      return m;
    } else {
      return iter->second;
    }
  }
  virtual void getValueAndReset(std::map<std::string, std::string>& retval) {
    for (auto& kv : metrics_) {
      retval[kv.first] = kv.second->getValueAndReset();
    }
  }
 private:
  std::map<std::string, std::shared_ptr<MeanMetric>> metrics_;
};

}  // namespace metric
}  // namespace api
}  // namespace heron

#endif  // HERON_API_METRIC_MEAN_METRIC_H_
