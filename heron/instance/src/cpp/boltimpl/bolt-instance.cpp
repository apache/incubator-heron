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

#include <dlfcn.h>
#include <stdlib.h>

#include <list>
#include <string>

#include "boltimpl/bolt-instance.h"
#include "proto/messages.h"
#include "network/network.h"
#include "basics/basics.h"
#include "config/heron-internals-config-reader.h"

#include "boltimpl/tuple-impl.h"
#include "boltimpl/tick-tuple.h"

namespace heron {
namespace instance {

BoltInstance::BoltInstance(std::shared_ptr<EventLoop> eventLoop,
    std::shared_ptr<TaskContextImpl> taskContext,
    NotifyingCommunicator<pool_unique_ptr<google::protobuf::Message>>* dataToExecutor,
    NotifyingCommunicator<google::protobuf::Message*>* dataFromExecutor,
    void* dllHandle)
  : taskContext_(taskContext), dataToExecutor_(dataToExecutor),
    dataFromExecutor_(dataFromExecutor), eventLoop_(eventLoop), bolt_(NULL), active_(false),
    tickTimer_(-1) {
  maxWriteBufferSize_ = config::HeronInternalsConfigReader::Instance()
                               ->GetHeronInstanceInternalBoltWriteQueueCapacity();
  api::bolt::IBolt* (*creatorFunction)();
  creatorFunction = (api::bolt::IBolt* (*)())dlsym(dllHandle,
                                                   taskContext->getComponentConstructor().c_str());
  if (!creatorFunction) {
    LOG(FATAL) << "dlsym failed for " << taskContext->getComponentConstructor()
               << " with error " << dlerror();
  }
  bolt_ = (*creatorFunction)();
  if (!bolt_) {
    LOG(FATAL) << "Attempt to create bolt using " << taskContext->getComponentConstructor()
               << " failed with error " << dlerror();
  }
  serializer_.reset(api::serializer::IPluggableSerializer::createSerializer(
                                                           taskContext_->getConfig()));
  metrics_.reset(new BoltMetrics(taskContext->getMetricsRegistrar()));
  collector_.reset(new BoltOutputCollectorImpl(serializer_, taskContext_,
                                               dataFromExecutor_, metrics_));
}

BoltInstance::~BoltInstance() {
  bolt_->cleanup();
  delete bolt_;
  if (tickTimer_ > 0) {
    eventLoop_->unRegisterTimer(tickTimer_);
  }
}

void BoltInstance::Start() {
  CHECK(!active_);
  LOG(INFO) << "Starting bolt " << taskContext_->getThisComponentName();
  bolt_->open(taskContext_->getConfig(), taskContext_, collector_);
  if (taskContext_->getConfig()->hasConfig(api::config::Config::TOPOLOGY_TICK_TUPLE_FREQ_SECS)) {
    int tickTimerSecs = atoi(taskContext_->getConfig()
                               ->get(api::config::Config::TOPOLOGY_TICK_TUPLE_FREQ_SECS).c_str());
    int64_t timeoutMs = tickTimerSecs * 1000 * 1000;
    tickTimer_ = eventLoop_->registerTimer(
            [this](EventLoop::Status status) { this->onTickTimer(); }, true, timeoutMs);
      CHECK_GT(tickTimer_, 0);
  }
  active_ = true;
}

void BoltInstance::Activate() {
  LOG(INFO) << "Not doing anything in Bolt Activate";
  CHECK(!active_);
  active_ = true;
}

void BoltInstance::Deactivate() {
  LOG(INFO) << "Not doing anything in Bolt Dacctivate";
  CHECK(active_);
  active_ = false;
}

void BoltInstance::DoWork() {
  dataToExecutor_->resumeConsumption();
}

void BoltInstance::executeTuple(const proto::api::StreamId& stream,
                                std::shared_ptr<const proto::system::HeronDataTuple> tup) {
  std::shared_ptr<TupleImpl> t(new TupleImpl(serializer_, taskContext_, stream, tup));
  int64_t startTime = std::chrono::duration_cast<std::chrono::nanoseconds>(
                                   std::chrono::system_clock::now().time_since_epoch()).count();
  bolt_->execute(t);
  int64_t endTime = std::chrono::duration_cast<std::chrono::nanoseconds>(
                                   std::chrono::system_clock::now().time_since_epoch()).count();
  metrics_->executeTuple(stream.id(), stream.component_name(), endTime - startTime);
}

void BoltInstance::HandleGatewayTuples(pool_unique_ptr<proto::system::HeronTupleSet2> tupleSet) {
  if (tupleSet->has_control()) {
    LOG(FATAL) << "Bolt cannot get incoming control tuples from other components";
  }

  if (tupleSet->has_data()) {
    for (int i = 0; i < tupleSet->data().tuples_size(); ++i) {
      auto t = new proto::system::HeronDataTuple();
      if (!t->ParseFromString(tupleSet->data().tuples(i))) {
        LOG(FATAL) << "Failed to parse protobuf";
      }
      std::shared_ptr<const proto::system::HeronDataTuple> tup(t);
      executeTuple(tupleSet->data().stream(), tup);
    }
  }

  if (dataFromExecutor_->size() > maxWriteBufferSize_) {
    dataToExecutor_->stopConsumption();
  }
}

void BoltInstance::onTickTimer() {
  std::shared_ptr<api::tuple::Tuple> t(new TickTuple(serializer_));
  int64_t startTime = std::chrono::duration_cast<std::chrono::nanoseconds>(
                                   std::chrono::system_clock::now().time_since_epoch()).count();
  bolt_->execute(t);
  int64_t endTime = std::chrono::duration_cast<std::chrono::nanoseconds>(
                                   std::chrono::system_clock::now().time_since_epoch()).count();
  metrics_->executeTuple(t->getSourceStreamId(), t->getSourceComponent(), endTime - startTime);
  collector_->sendOutTuples();
}

}  // namespace instance
}  // namespace heron
