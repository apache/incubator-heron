/*
 * Copyright 2015 Twitter, Inc.
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

//////////////////////////////////////////////////////
//
// metrics-mgr-st.cpp
//
// Please see metrics-mgr-st.h for details.
//////////////////////////////////////////////////////
#include "metrics/metrics-mgr-st.h"
#include <map>
#include "metrics/imetric.h"
#include "metrics/metricsmgr-client.h"
#include "proto/messages.h"
#include "basics/basics.h"
#include "errors/errors.h"
#include "threads/threads.h"
#include "network/network.h"

namespace heron {
namespace common {

MetricsMgrSt::MetricsMgrSt(const sp_string& _my_hostname, sp_int32 _my_port,
                           sp_int32 _metricsmgr_port, const sp_string& _component,
                           const sp_string& _task_id, sp_int32 _interval, EventLoop* eventLoop) {
  NetworkOptions options;
  options.set_host("localhost");
  options.set_port(_metricsmgr_port);
  options.set_max_packet_size(1024 * 1024);
  options.set_socket_family(PF_INET);
  client_ = new MetricsMgrClient(_my_hostname, _my_port, _component, _task_id, eventLoop, options);
  timer_cb_ = [this](EventLoop::Status status) { this->gather_metrics(status); };
  timerid_ = eventLoop->registerTimer(timer_cb_, true, _interval * 1000000);
  CHECK_GE(timerid_, 0);
}

MetricsMgrSt::~MetricsMgrSt() {
  CHECK_EQ(client_->getEventLoop()->unRegisterTimer(timerid_), 0);
  delete client_;
  for (auto iter = metrics_.begin(); iter != metrics_.end(); ++iter) {
    delete iter->second;
  }
}

void MetricsMgrSt::RefreshTMasterLocation(const proto::tmaster::TMasterLocation& location) {
  client_->SendTMasterLocation(location);
}

void MetricsMgrSt::register_metric(const sp_string& _metric_name, IMetric* _metric) {
  metrics_[_metric_name] = _metric;
}

void MetricsMgrSt::unregister_metric(const sp_string& _metric_name) {
  metrics_.erase(_metric_name);
}

void MetricsMgrSt::gather_metrics(EventLoop::Status) {
  using heron::proto::system::MetricPublisherPublishMessage;

  if (metrics_.empty()) return;
  auto message = new MetricPublisherPublishMessage();
  for (auto iter = metrics_.begin(); iter != metrics_.end(); ++iter) {
    iter->second->GetAndReset(iter->first, message);
  }
  client_->SendMetrics(message);
}
}  // namespace common
}  // namespace heron
