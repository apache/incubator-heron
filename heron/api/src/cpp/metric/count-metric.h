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

#ifndef HERON_API_METRIC_COUNT_METRIC_H_
#define HERON_API_METRIC_COUNT_METRIC_H_

#include <string>

#include "metric/imetric.h"

namespace heron {
namespace api {
namespace metric {

/**
 * Interface for a metric that can be tracked
 */
class CountMetric : public IMetric {
 public:
  CountMetric() : value_(0) { }
  virtual ~CountMetric() { }
  void incr() { value_++; }
  void incrBy(int64_t by) { value_ += by; }
  virtual const std::string getValueAndReset() {
    int64_t ret = value_;
    value_ = 0;
    return std::to_string(ret);
  }
 private:
  int64_t value_;
};

}  // namespace metric
}  // namespace api
}  // namespace heron

#endif  // HERON_API_METRIC_COUNT_METRIC_H_
