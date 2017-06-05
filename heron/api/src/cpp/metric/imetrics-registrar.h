/*
 * Copyright 2017 Twitter, Inc.
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

#ifndef HERON_API_METRIC_IMETRICS_REGISTRAR_H_
#define HERON_API_METRIC_IMETRICS_REGISTRAR_H_

#include <iostream>
#include <memory>
#include <string>

#include "metric/imetric.h"
#include "metric/imulti-metric.h"

namespace heron {
namespace api {
namespace metric {

/**
 * Interface for a metric that can be tracked
 */
class IMetricsRegistrar {
 public:
  virtual void registerMetric(const std::string& metricName,
                              std::shared_ptr<IMetric> metric, int timeBucketSizeInSecs) = 0;
  virtual void registerMetric(const std::string& metricName,
                              std::shared_ptr<IMultiMetric> metric, int timeBucketSizeInSecs) = 0;
};

}  // namespace metric
}  // namespace api
}  // namespace heron

#endif  // HERON_API_METRIC_IMETRICS_REGISTRAR_H_
