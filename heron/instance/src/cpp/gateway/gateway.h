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

#ifndef HERON_INSTANCE_GATEWAY_GATEWAY_H_
#define HERON_INSTANCE_GATEWAY_GATEWAY_H_

#include <string>
#include "proto/messages.h"
#include "network/network.h"
#include "basics/basics.h"

#include "utils/notifying-communicator.h"
#include "metric/imetric.h"
#include "gateway/gateway-metrics.h"
#include "gateway/stmgr-client.h"
#include "metrics/metricsmgr-client.h"

namespace heron {
namespace instance {

class Gateway {
 public:
  Gateway(const std::string& topologyName,
        const std::string& topologyId, const std::string& instanceId,
        const std::string& componentName, int taskId, int componentIndex,
        const std::string& stmgrId, int stmgrPort, int metricsMgrPort,
        std::shared_ptr<EventLoop> eventLoop);
  virtual ~Gateway();

  // All kinds of initialization like starting clients
  void Start();

  // Called when Executor indicates that it consumed some data
  void HandleExecutorDataConsumed();

  // Called when we need to consume data from executor
  void HandleExecutorData(google::protobuf::Message* msg);

  // Called when we need to consume metrics from executor
  void HandleExecutorMetrics(google::protobuf::Message* msg);

  std::shared_ptr<EventLoop> eventLoop() { return eventLoop_; }
  void setCommunicators(
          NotifyingCommunicator<pool_unique_ptr<google::protobuf::Message>>* dataToExecutor,
          NotifyingCommunicator<google::protobuf::Message*>* dataFromExecutor,
          NotifyingCommunicator<google::protobuf::Message*>* metricsFromExecutor) {
    dataToExecutor_ = dataToExecutor;
    dataFromExecutor_ = dataFromExecutor;
    metricsFromExecutor_ = metricsFromExecutor;
  }

 private:
  void HandleNewPhysicalPlan(pool_unique_ptr<proto::system::PhysicalPlan> pplan);
  void HandleStMgrTuples(pool_unique_ptr<proto::system::HeronTupleSet2> tuples);
  void ResumeConsumingFromExecutorTimer();
  std::string topologyName_;
  std::string topologyId_;
  int stmgrPort_;
  int metricsMgrPort_;
  proto::system::Instance instanceProto_;
  std::shared_ptr<StMgrClient> stmgrClient_;
  std::shared_ptr<common::MetricsMgrClient> metricsMgrClient_;
  std::shared_ptr<GatewayMetrics> gatewayMetrics_;
  NotifyingCommunicator<pool_unique_ptr<google::protobuf::Message>>* dataToExecutor_;
  NotifyingCommunicator<google::protobuf::Message*>* dataFromExecutor_;
  NotifyingCommunicator<google::protobuf::Message*>* metricsFromExecutor_;
  std::shared_ptr<EventLoop> eventLoop_;
  // This is the max number of outstanding packets that are yet to be
  // consumed by the Executor
  int maxReadBufferSize_;
  // This is the max number of outstanding packets that are buffered
  // to be sent to the stmgr
  int maxWriteBufferSize_;
  // The maximum size of a packet
  int maxPacketSize_;
  // Are we actively reading from executorQueue
  bool readingFromExecutor_;
};

}  // namespace instance
}  // namespace heron

#endif  // HERON_INSTANCE_GATEWAY_GATEWAY_H_
