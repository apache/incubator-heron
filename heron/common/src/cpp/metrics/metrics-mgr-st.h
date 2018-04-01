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
// metrics-mgr-st.h
//
// Single threaded metrics manager that deals with
// scheduling reporting metrics to the metrics mgr
//////////////////////////////////////////////////////
#ifndef __METRICS_MGR_ST_H_
#define __METRICS_MGR_ST_H_

#include <map>
#include "metrics/imetric.h"
#include "network/network.h"
#include "proto/messages.h"
#include "basics/basics.h"

namespace heron {
namespace proto {
namespace tmaster {
class TMasterLocation;
}
}
}

namespace heron {
namespace common {

class MetricsMgrClient;
class IMetric;

class MetricsMgrSt {
 public:
  MetricsMgrSt(sp_int32 _metricsmgr_port, sp_int32 _interval, EventLoop* eventLoop);
  virtual ~MetricsMgrSt();

  void register_metric(const sp_string& _metric_name, IMetric* _metric);
  void unregister_metric(const sp_string& _metric_name);
  void RefreshTMasterLocation(const proto::tmaster::TMasterLocation& location);
  void RefreshMetricsCacheLocation(const proto::tmaster::MetricsCacheLocation& location);

  /**
      Start MetricsMgrClient object

      @param _my_hostname to build message proto::system::MetricPublisher.
      @param _my_port to build message proto::system::MetricPublisher.
      @param _component to build message proto::system::MetricPublisher.
      @param _instance_id to build message proto::system::MetricPublisher.
  */
  void Start(const sp_string& _my_hostname, sp_int32 _my_port,
             const sp_string& _component_id, const sp_string& _instance_id);

 private:
  void gather_metrics(EventLoop::Status);

  VCallback<EventLoop::Status> timer_cb_;
  std::map<sp_string, IMetric*> metrics_;
  MetricsMgrClient* client_;
  NetworkOptions options_;
  sp_int64 timerid_;
  EventLoop* eventLoop_;
};
}  // namespace common
}  // namespace heron

#endif
