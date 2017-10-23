/*
 * Copyright 2017 Twitter, Inc.
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

#ifndef HERON_INSTANCE_BOLT_BOLT_INSTANCE_H_
#define HERON_INSTANCE_BOLT_BOLT_INSTANCE_H_

#include <string>

#include "slave/instance-base.h"

#include "proto/messages.h"
#include "network/network.h"
#include "basics/basics.h"

#include "utils/notifying-communicator.h"
#include "bolt/ibolt.h"
#include "topology/task-context.h"
#include "serializer/ipluggable-serializer.h"
#include "boltimpl/bolt-output-collector-impl.h"
#include "boltimpl/bolt-metrics.h"

namespace heron {
namespace instance {

class BoltInstance : public InstanceBase {
 public:
  BoltInstance(EventLoop* eventLoop, std::shared_ptr<TaskContextImpl> taskContext,
               NotifyingCommunicator<google::protobuf::Message*>* dataToSlave,
               NotifyingCommunicator<google::protobuf::Message*>* dataFromSlave,
               void* dllHandle);
  virtual ~BoltInstance();

  // This essentially sets up the bolt and calls open
  virtual void Start();
  virtual void Activate();
  virtual void Deactivate();
  virtual bool IsRunning() { return active_; }
  virtual void DoWork();
  virtual void HandleGatewayTuples(proto::system::HeronTupleSet2* tupleSet);

 private:
  void executeTuple(const proto::api::StreamId& stream,
                    std::shared_ptr<const proto::system::HeronDataTuple> tup);
  void onTickTimer();
  void executeTuple(const proto::api::StreamId& stream,
                    const proto::system::HeronDataTuple& tup);

  std::shared_ptr<TaskContextImpl> taskContext_;
  NotifyingCommunicator<google::protobuf::Message*>* dataToSlave_;
  NotifyingCommunicator<google::protobuf::Message*>* dataFromSlave_;
  EventLoop* eventLoop_;
  api::bolt::IBolt* bolt_;
  std::shared_ptr<api::serializer::IPluggableSerializer> serializer_;
  std::shared_ptr<BoltOutputCollectorImpl> collector_;
  std::shared_ptr<BoltMetrics> metrics_;
  bool active_;
  int64_t tickTimer_;
  // This is the max number of outstanding packets that are buffered
  // to be sent to the gateway
  int maxWriteBufferSize_;
};

}  // namespace instance
}  // namespace heron

#endif  // HERON_INSTANCE_BOLT_BOLT_INSTANCE_H_
