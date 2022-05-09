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

#ifndef HERON_INSTANCE_SPOUT_SPOUT_INSTANCE_H_
#define HERON_INSTANCE_SPOUT_SPOUT_INSTANCE_H_

#include <string>

#include "executor/instance-base.h"

#include "proto/messages.h"
#include "network/network.h"
#include "basics/basics.h"

#include "utils/notifying-communicator.h"
#include "spout/ispout.h"
#include "topology/task-context.h"
#include "serializer/ipluggable-serializer.h"
#include "spoutimpl/spout-output-collector-impl.h"
#include "spoutimpl/spout-metrics.h"

namespace heron {
namespace instance {

class SpoutInstance : public InstanceBase {
 public:
  SpoutInstance(std::shared_ptr<EventLoop> eventLoop, std::shared_ptr<TaskContextImpl> taskContext,
                NotifyingCommunicator<google::protobuf::Message*>* dataFromExecutor,
                void* dllHandle);
  virtual ~SpoutInstance();

  // This essentially sets up the spout and calls open
  virtual void Start();
  virtual void Activate();
  virtual void Deactivate();
  virtual bool IsRunning() { return active_; }
  virtual void DoWork();
  virtual void HandleGatewayTuples(pool_unique_ptr<proto::system::HeronTupleSet2> tupleSet);

 private:
  void lookForTimeouts();
  void produceTuple();
  void doImmediateAcks();
  bool canContinueWork();
  bool canProduceTuple();
  void handleAckTuple(const proto::system::AckTuple& ackTuple, bool isAck);

  std::shared_ptr<TaskContextImpl> taskContext_;
  NotifyingCommunicator<google::protobuf::Message*>* dataFromExecutor_;
  std::shared_ptr<EventLoop> eventLoop_;
  api::spout::ISpout* spout_;
  std::shared_ptr<api::serializer::IPluggableSerializer> serializer_;
  std::shared_ptr<SpoutOutputCollectorImpl> collector_;
  std::shared_ptr<SpoutMetrics> metrics_;
  bool active_;
  bool ackingEnabled_;
  bool enableMessageTimeouts_;
  int64_t lookForTimeoutsTimer_;
  // This is the max number of outstanding packets that are buffered
  // to be sent to the gateway
  int maxWriteBufferSize_;
  // This is the max time to spend in emitting tuple in one go
  int maxEmitBatchIntervalMs_;
  // This is the max number of bytes to emit in one go
  int maxEmitBatchSize_;
};

}  // namespace instance
}  // namespace heron

#endif  // HERON_INSTANCE_SPOUT_SPOUT_INSTANCE_H_
