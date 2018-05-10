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

#ifndef __METRICSMGR_CLIENT_H_
#define __METRICSMGR_CLIENT_H_

#include "network/network_error.h"
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

class MetricsMgrClient : public Client {
 public:
  MetricsMgrClient(const sp_string& _hostname, sp_int32 _port, const sp_string& _component_name,
                   const sp_string& _instance_id, int instance_index,
                   EventLoop* eventLoop, const NetworkOptions& options);
  ~MetricsMgrClient();

  void SendMetrics(proto::system::MetricPublisherPublishMessage* _message);
  void SendTMasterLocation(const proto::tmaster::TMasterLocation& location);
  void SendMetricsCacheLocation(const proto::tmaster::MetricsCacheLocation& location);

 protected:
  virtual void HandleConnect(NetworkErrorCode status);
  virtual void HandleClose(NetworkErrorCode status);

 private:
  void InternalSendTMasterLocation();
  void InternalSendMetricsCacheLocation();
  void ReConnect();
  void SendRegisterRequest();
  void HandleRegisterResponse(void* _ctx, proto::system::MetricPublisherRegisterResponse* _respose,
                              NetworkErrorCode _status);

  sp_string hostname_;
  sp_int32 port_;
  sp_string component_name_;
  sp_string instance_id_;
  int instance_index_;
  proto::tmaster::TMasterLocation* tmaster_location_;
  proto::tmaster::MetricsCacheLocation* metricscache_location_;
  // Tells if we have registered to metrics manager or not
  bool registered_;
};
}  // namespace common
}  // namespace heron

#endif
