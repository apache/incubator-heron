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

#include <string>

#include "glog/logging.h"

#include "gateway/gateway-metrics.h"
#include "proto/messages.h"
#include "network/network.h"
#include "basics/basics.h"
#include "config/heron-internals-config-reader.h"

namespace heron {
namespace instance {

GatewayMetrics::GatewayMetrics(std::shared_ptr<common::MetricsMgrClient> metricsMgrClient,
                               std::shared_ptr<EventLoop> eventLoop)
  : metricsMgrClient_(metricsMgrClient) {
  receivedPacketsCount_.reset(new api::metric::CountMetric());
  receivedPacketsSize_.reset(new api::metric::CountMetric());
  sentPacketsCount_.reset(new api::metric::CountMetric());
  sentPacketsSize_.reset(new api::metric::CountMetric());
  droppedPacketsCount_.reset(new api::metric::CountMetric());
  droppedPacketsSize_.reset(new api::metric::CountMetric());
  int interval = config::HeronInternalsConfigReader::Instance()
                 ->GetHeronMetricsExportIntervalSec();
  // Setup timer to periodically send the metrics
  CHECK_GT(
      eventLoop->registerTimer(
          [this](EventLoop::Status status) { this->sendMetrics(); }, true,
          interval * 1000 * 1000), 0);
}

GatewayMetrics::~GatewayMetrics() { }

void GatewayMetrics::sendMetrics() {
  auto msg = new proto::system::MetricPublisherPublishMessage();
  addMetrics(msg->add_metrics(), "__gateway-received-packets-count",
             receivedPacketsCount_);
  addMetrics(msg->add_metrics(), "__gateway-received-packets-size",
             receivedPacketsSize_);
  addMetrics(msg->add_metrics(), "__gateway-sent-packets-count",
             sentPacketsCount_);
  addMetrics(msg->add_metrics(), "__gateway-sent-packets-size",
             sentPacketsSize_);
  addMetrics(msg->add_metrics(), "__gateway-dropped-packets-count",
             droppedPacketsCount_);
  addMetrics(msg->add_metrics(), "__gateway-dropped-packets-size",
             droppedPacketsSize_);
  metricsMgrClient_->SendMetrics(msg);
}

void GatewayMetrics::addMetrics(proto::system::MetricDatum* datum,
                                const std::string& name,
                                std::shared_ptr<api::metric::IMetric> metric) {
  datum->set_name(name);
  datum->set_value(metric->getValueAndReset());
}

}  // namespace instance
}  // namespace heron
