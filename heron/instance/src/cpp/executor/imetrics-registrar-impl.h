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


#ifndef HERON_INSTANCE_EXECUTOR_IMETRICS_REGISTRAR_IMPL_H_
#define HERON_INSTANCE_EXECUTOR_IMETRICS_REGISTRAR_IMPL_H_

#include <map>
#include <list>
#include <string>
#include <utility>
#include "basics/basics.h"
#include "proto/messages.h"
#include "network/network.h"

#include "utils/notifying-communicator.h"
#include "metric/imetrics-registrar.h"
#include "metric/imetric.h"

namespace heron {
namespace instance {

/**
 * This implements the IMetricsRegistrar interface of the Heron API
 *
 */
class IMetricsRegistrarImpl : public api::metric::IMetricsRegistrar {
 public:
  explicit IMetricsRegistrarImpl(std::shared_ptr<EventLoop> eventLoop,
           NotifyingCommunicator<google::protobuf::Message*>* metricsFromExecutor);
  virtual ~IMetricsRegistrarImpl();
  virtual void registerMetric(const std::string& metricName,
                              std::shared_ptr<api::metric::IMetric> metric,
                              int timeBucketSizeInSecs);
  virtual void registerMetric(const std::string& metricName,
                              std::shared_ptr<api::metric::IMultiMetric> metric,
                              int timeBucketSizeInSecs);

 private:
  void sendMetrics(int timeBucketSizeInSecs);
  std::map<std::string, std::shared_ptr<api::metric::IMetric>> metrics_;
  std::map<std::string, std::shared_ptr<api::metric::IMultiMetric>> multiMetrics_;
  std::map<int, std::list<std::string>> timeBuckets_;
  std::shared_ptr<EventLoop> eventLoop_;
  NotifyingCommunicator<google::protobuf::Message*>* metricsFromExecutor_;
};

}  // namespace instance
}  // namespace heron

#endif
