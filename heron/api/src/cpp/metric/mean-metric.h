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

#ifndef HERON_API_METRIC_MEAN_METRIC_H_
#define HERON_API_METRIC_MEAN_METRIC_H_

#include <string>

#include "metric/imetric.h"

namespace heron {
namespace api {
namespace metric {

/**
 * Interface for a metric that can be tracked
 */
class MeanMetric : public IMetric {
 public:
  MeanMetric() : numerator_(0), denominator_(0) { }
  virtual ~MeanMetric() { }
  void incr(int64_t num) { numerator_ += num; denominator_++; }
  virtual const std::string getValueAndReset() {
    if (denominator_ == 0) return "0";
    double retval = ((static_cast<double>(numerator_))) / denominator_;
    numerator_ = 0;
    denominator_ = 0;
    return std::to_string(retval);
  }
 private:
  int64_t numerator_;
  int64_t denominator_;
};

}  // namespace metric
}  // namespace api
}  // namespace heron

#endif  // HERON_API_METRIC_MEAN_METRIC_H_
