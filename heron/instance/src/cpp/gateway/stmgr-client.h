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

#ifndef HERON_INSTANCE_GATEWAY_STMGR_CLIENT_H_
#define HERON_INSTANCE_GATEWAY_STMGR_CLIENT_H_

#include <string>

#include "network/network_error.h"
#include "proto/messages.h"
#include "network/network.h"
#include "basics/basics.h"

#include "gateway/gateway-metrics.h"

namespace heron {
namespace instance {

class StMgrClient : public Client {
 public:
  StMgrClient(std::shared_ptr<EventLoop> eventLoop, const NetworkOptions& options,
              const std::string& topologyName,
              const std::string& topologyId, const proto::system::Instance& instance,
              std::shared_ptr<GatewayMetrics> gatewayMetrics,
              std::function<void(pool_unique_ptr<proto::system::PhysicalPlan>)> pplan_watcher,
              std::function<void(pool_unique_ptr<proto::system::HeronTupleSet2>)> tuple_watcher);
  virtual ~StMgrClient();

  void SendTupleMessage(const proto::system::HeronTupleSet& msg);
  void putBackPressure();
  void removeBackPressure();

 protected:
  virtual void HandleConnect(NetworkErrorCode status);
  virtual void HandleClose(NetworkErrorCode status);

 private:
  void HandleRegisterResponse(void*,
                              pool_unique_ptr<proto::stmgr::RegisterInstanceResponse> response,
                              NetworkErrorCode status);
  void HandlePhysicalPlan(pool_unique_ptr<proto::stmgr::NewInstanceAssignmentMessage> msg);
  void HandleTupleMessage(pool_unique_ptr<proto::system::HeronTupleSet2> tupleMessage);

  void OnReconnectTimer();
  void SendRegisterRequest();

  std::string topologyName_;
  std::string topologyId_;
  const proto::system::Instance& instanceProto_;
  std::shared_ptr<GatewayMetrics> gatewayMetrics_;
  std::function<void(pool_unique_ptr<proto::system::PhysicalPlan>)> pplanWatcher_;
  std::function<void(pool_unique_ptr<proto::system::HeronTupleSet2>)> tupleWatcher_;
  int64_t ndropped_messages_;
  int reconnect_interval_;
  int max_reconnect_times_;
  int reconnect_attempts_;
};

}  // namespace instance
}  // namespace heron

#endif  // HERON_INSTANCE_GATEWAY_STMGR_CLIENT_H_
