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

#ifndef HERON_INSTANCE_EXECUTOR_EXECUTOR_H_
#define HERON_INSTANCE_EXECUTOR_EXECUTOR_H_

#include <string>
#include <thread>
#include "proto/messages.h"
#include "network/network.h"
#include "basics/basics.h"

#include "utils/notifying-communicator.h"
#include "executor/task-context-impl.h"
#include "executor/instance-base.h"

namespace heron {
namespace instance {

class Executor {
 public:
  Executor(int myTaskId, const std::string& topologySo);
  ~Executor();

  // This essentially fires a thread with internalStart
  void Start();

  std::shared_ptr<EventLoop> eventLoop() { return eventLoop_; }
  void setCommunicators(
                    NotifyingCommunicator<pool_unique_ptr<google::protobuf::Message>>* dataToExecutor,
                    NotifyingCommunicator<google::protobuf::Message*>* dataFromExecutor,
                    NotifyingCommunicator<google::protobuf::Message*>* metricsFromExecutor);

  // Handles data from gateway thread
  void HandleGatewayData(pool_unique_ptr<google::protobuf::Message> msg);

  // This is the notification that gateway thread consumed something that we wrote
  void HandleGatewayDataConsumed();

  // This is the notification that gateway thread consumed metrics that we sent
  void HandleGatewayMetricsConsumed() { }

 private:
  // This is the one thats running in the executor thread
  void InternalStart();
  // Called when a new phyiscal plan is received
  void HandleNewPhysicalPlan(pool_unique_ptr<proto::system::PhysicalPlan> pplan);
  // Called when we receive new tuple messages from gateway
  void HandleStMgrTuples(pool_unique_ptr<proto::system::HeronTupleSet2> msg);

  int myTaskId_;
  std::shared_ptr<TaskContextImpl> taskContext_;
  NotifyingCommunicator<pool_unique_ptr<google::protobuf::Message>>* dataToExecutor_;
  NotifyingCommunicator<google::protobuf::Message*>* dataFromExecutor_;
  NotifyingCommunicator<google::protobuf::Message*>* metricsFromExecutor_;
  InstanceBase* instance_;
  std::shared_ptr<EventLoop> eventLoop_;
  void* dllHandle_;
  std::string pplan_typename_;
  std::unique_ptr<std::thread> executorThread_;
};

}  // namespace instance
}  // namespace heron

#endif  // HERON_INSTANCE_EXECUTOR_EXECUTOR_H_
