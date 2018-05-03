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

#ifndef HERON_INSTANCE_BOLT_BOLT_METRICS_H_
#define HERON_INSTANCE_BOLT_BOLT_METRICS_H_

#include <string>

#include "proto/messages.h"
#include "network/network.h"
#include "basics/basics.h"

#include "metric/multi-count-metric.h"
#include "metric/multi-mean-metric.h"
#include "metric/imetrics-registrar.h"

namespace heron {
namespace instance {

class BoltMetrics {
 public:
  explicit BoltMetrics(std::shared_ptr<api::metric::IMetricsRegistrar> metricsRegistrar);
  virtual ~BoltMetrics();

  void ackedTuple(const std::string& streamId, const std::string& source,
                  int64_t latency);
  void failedTuple(const std::string& streamId, const std::string& source,
                   int64_t latency);
  void executeTuple(const std::string& streamId, const std::string& source,
                   int64_t latency);
  void emittedTuple(const std::string& streamId);

 private:
  std::shared_ptr<api::metric::MultiCountMetric> ackCount_;
  std::shared_ptr<api::metric::MultiCountMetric> failCount_;
  std::shared_ptr<api::metric::MultiCountMetric> executeCount_;
  std::shared_ptr<api::metric::MultiMeanMetric> processLatency_;
  std::shared_ptr<api::metric::MultiMeanMetric> failLatency_;
  std::shared_ptr<api::metric::MultiMeanMetric> executeLatency_;
  std::shared_ptr<api::metric::MultiCountMetric> executeTime_;
  std::shared_ptr<api::metric::MultiCountMetric> emitCount_;
};

}  // namespace instance
}  // namespace heron

#endif  // HERON_INSTANCE_BOLT_BOLT_METRICS_H_
