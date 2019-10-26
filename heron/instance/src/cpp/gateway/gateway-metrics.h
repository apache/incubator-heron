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

#ifndef HERON_INSTANCE_GATEWAY_GATEWAY_METRICS_H_
#define HERON_INSTANCE_GATEWAY_GATEWAY_METRICS_H_

#include <iostream>
#include <memory>
#include <string>

#include "proto/messages.h"
#include "network/network.h"
#include "basics/basics.h"
#include "metric/count-metric.h"
#include "metrics/metricsmgr-client.h"

namespace heron {
namespace instance {

class GatewayMetrics {
 public:
  GatewayMetrics(std::shared_ptr<common::MetricsMgrClient> metricsMgrClient,
                 std::shared_ptr<EventLoop> eventLoop);
  virtual ~GatewayMetrics();

  void updateReceivedPacketsCount(int64_t count) {
    receivedPacketsCount_->incrBy(count);
  }
  void updateReceivedPacketsSize(int64_t count) {
    receivedPacketsSize_->incrBy(count);
  }
  void updateSentPacketsCount(int64_t count) {
    sentPacketsCount_->incrBy(count);
  }
  void updateSentPacketsSize(int64_t count) {
    sentPacketsSize_->incrBy(count);
  }
  void updateDroppedPacketsCount(int64_t count) {
    droppedPacketsCount_->incrBy(count);
  }
  void updateDroppedPacketsSize(int64_t count) {
    droppedPacketsSize_->incrBy(count);
  }

 private:
  void sendMetrics();
  void addMetrics(proto::system::MetricDatum* datum,
                  const std::string& name,
                  std::shared_ptr<api::metric::IMetric> metric);
  std::shared_ptr<api::metric::CountMetric> receivedPacketsCount_;
  std::shared_ptr<api::metric::CountMetric> receivedPacketsSize_;
  std::shared_ptr<api::metric::CountMetric> sentPacketsCount_;
  std::shared_ptr<api::metric::CountMetric> sentPacketsSize_;
  std::shared_ptr<api::metric::CountMetric> droppedPacketsCount_;
  std::shared_ptr<api::metric::CountMetric> droppedPacketsSize_;
  std::shared_ptr<common::MetricsMgrClient> metricsMgrClient_;
};

}  // namespace instance
}  // namespace heron

#endif  // HERON_INSTANCE_GATEWAY_GATEWAY_METRICS_H_
