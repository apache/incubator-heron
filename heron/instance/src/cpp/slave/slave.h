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

#ifndef HERON_INSTANCE_SLAVE_SLAVE_H_
#define HERON_INSTANCE_SLAVE_SLAVE_H_

#include <string>
#include <thread>
#include "proto/messages.h"
#include "network/network.h"
#include "basics/basics.h"

#include "utils/notifying-communicator.h"
#include "slave/task-context-impl.h"
#include "slave/instance-base.h"

namespace heron {
namespace instance {

class Slave {
 public:
  Slave(int myTaskId, const std::string& topologySo);
  ~Slave();

  // This essentially fires a thread with internalStart
  void Start();

  EventLoop* eventLoop() { return eventLoop_; }
  void setCommunicators(NotifyingCommunicator<google::protobuf::Message*>* dataToSlave,
                        NotifyingCommunicator<google::protobuf::Message*>* dataFromSlave,
                        NotifyingCommunicator<google::protobuf::Message*>* metricsFromSlave);

  // Handles data from gateway thread
  void HandleGatewayData(google::protobuf::Message* msg);

  // This is the notification that gateway thread consumed something that we wrote
  void HandleGatewayDataConsumed();

  // This is the notification that gateway thread consumed metrics that we sent
  void HandleGatewayMetricsConsumed() { }

 private:
  // This is the one thats running in the slave thread
  void InternalStart();
  // Called when a new phyiscal plan is received
  void HandleNewPhysicalPlan(proto::system::PhysicalPlan* pplan);
  // Called when we receive new tuple messages from gateway
  void HandleStMgrTuples(proto::system::HeronTupleSet2* msg);

  int myTaskId_;
  std::shared_ptr<TaskContextImpl> taskContext_;
  NotifyingCommunicator<google::protobuf::Message*>* dataToSlave_;
  NotifyingCommunicator<google::protobuf::Message*>* dataFromSlave_;
  NotifyingCommunicator<google::protobuf::Message*>* metricsFromSlave_;
  InstanceBase* instance_;
  EventLoop* eventLoop_;
  void* dllHandle_;
  std::string pplan_typename_;
  std::unique_ptr<std::thread> slaveThread_;
};

}  // namespace instance
}  // namespace heron

#endif  // HERON_INSTANCE_SLAVE_SLAVE_H_
