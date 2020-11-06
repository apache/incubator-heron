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

#include <map>
#include <list>
#include <string>
#include <utility>
#include "executor/imetrics-registrar-impl.h"
#include "basics/basics.h"
#include "proto/messages.h"
#include "network/network.h"

namespace heron {
namespace instance {

IMetricsRegistrarImpl::IMetricsRegistrarImpl(std::shared_ptr<EventLoop> eventLoop,
           NotifyingCommunicator<google::protobuf::Message*>* metricsFromExecutor)
  : eventLoop_(eventLoop), metricsFromExecutor_(metricsFromExecutor) {
}

IMetricsRegistrarImpl::~IMetricsRegistrarImpl() {
}

void IMetricsRegistrarImpl::registerMetric(const std::string& metricName,
                                           std::shared_ptr<api::metric::IMetric> metric,
                                           int timeBucketSizeInSecs) {
  metrics_[metricName] = metric;
  if (timeBuckets_.find(timeBucketSizeInSecs) == timeBuckets_.end()) {
    timeBuckets_[timeBucketSizeInSecs] = {metricName};
    CHECK_GT(
        eventLoop_->registerTimer(
            [this, timeBucketSizeInSecs](EventLoop::Status status) {
                           this->sendMetrics(timeBucketSizeInSecs);
            }, true, timeBucketSizeInSecs * 1000 * 1000), 0);
  } else {
    timeBuckets_[timeBucketSizeInSecs].push_back(metricName);
  }
}

void IMetricsRegistrarImpl::registerMetric(const std::string& metricName,
                                           std::shared_ptr<api::metric::IMultiMetric> metric,
                                           int timeBucketSizeInSecs) {
  multiMetrics_[metricName] = metric;
  if (timeBuckets_.find(timeBucketSizeInSecs) == timeBuckets_.end()) {
    timeBuckets_[timeBucketSizeInSecs] = {metricName};
    CHECK_GT(
        eventLoop_->registerTimer(
            [this, timeBucketSizeInSecs](EventLoop::Status status) {
                           this->sendMetrics(timeBucketSizeInSecs);
            }, true, timeBucketSizeInSecs * 1000 * 1000), 0);
  } else {
    timeBuckets_[timeBucketSizeInSecs].push_back(metricName);
  }
}

void IMetricsRegistrarImpl::sendMetrics(int timeBucketSizeInSecs) {
  auto iter = timeBuckets_.find(timeBucketSizeInSecs);
  auto msg = new proto::system::MetricPublisherPublishMessage();
  for (auto metricName : iter->second) {
    if (metrics_.find(metricName) != metrics_.end()) {
      const std::string& metricValue = metrics_[metricName]->getValueAndReset();
      auto datum = msg->add_metrics();
      datum->set_name(metricName);
      datum->set_value(metricValue);
    } else {
      CHECK(multiMetrics_.find(metricName) != multiMetrics_.end());
      std::map<std::string, std::string> mmap;
      multiMetrics_[metricName]->getValueAndReset(mmap);
      for (auto& kv : mmap) {
        auto datum = msg->add_metrics();
        datum->set_name(metricName + "/" + kv.first);
        datum->set_value(kv.second);
      }
    }
  }
  metricsFromExecutor_->enqueue(msg);
}

}  // namespace instance
}  // namespace heron
