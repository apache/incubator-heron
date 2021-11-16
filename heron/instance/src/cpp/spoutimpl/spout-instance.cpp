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

#include "utils/notifying-communicator.h"
#include "spoutimpl/spout-instance.h"
#include "proto/messages.h"
#include "network/network.h"
#include "basics/basics.h"
#include "config/heron-internals-config-reader.h"

namespace heron {
namespace instance {

SpoutInstance::SpoutInstance(std::shared_ptr<EventLoop> eventLoop,
                             std::shared_ptr<TaskContextImpl> taskContext,
                             NotifyingCommunicator<google::protobuf::Message*>* dataFromExecutor,
                             void* dllHandle)
  : taskContext_(taskContext),
    dataFromExecutor_(dataFromExecutor), eventLoop_(eventLoop), spout_(NULL), active_(false) {
  maxWriteBufferSize_ = config::HeronInternalsConfigReader::Instance()
                               ->GetHeronInstanceInternalSpoutWriteQueueCapacity();
  maxEmitBatchIntervalMs_ = config::HeronInternalsConfigReader::Instance()
                             ->GetHeronInstanceEmitBatchTimeMs();
  maxEmitBatchSize_ = config::HeronInternalsConfigReader::Instance()
                             ->GetHeronInstanceEmitBatchSize();
  ackingEnabled_ = taskContext->isAckingEnabled();
  enableMessageTimeouts_ = taskContext->enableMessageTimeouts();
  lookForTimeoutsTimer_ = -1;
  api::spout::ISpout* (*creatorFunction)();
  creatorFunction = (api::spout::ISpout* (*)())dlsym(dllHandle,
                                                taskContext->getComponentConstructor().c_str());
  if (!creatorFunction) {
    LOG(FATAL) << "dlsym failed for " << taskContext->getComponentConstructor()
               << " with error " << dlerror();
  }
  spout_ = (*creatorFunction)();
  if (!spout_) {
    LOG(FATAL) << "dlsym failed for " << taskContext->getComponentConstructor()
               << " with error " << dlerror();
  }
  serializer_.reset(api::serializer::IPluggableSerializer::createSerializer(
                                                           taskContext_->getConfig()));
  metrics_.reset(new SpoutMetrics(taskContext->getMetricsRegistrar()));
  collector_.reset(new SpoutOutputCollectorImpl(serializer_, taskContext_, dataFromExecutor_));
  LOG(INFO) << "Instantiated spout for component " << taskContext->getThisComponentName()
            << " with task_id " << taskContext->getThisTaskId() << " and maxWriteBufferSize_ "
            << maxWriteBufferSize_ << " and maxEmitBatchIntervalMs " << maxEmitBatchIntervalMs_
            << " and maxEmitBatchSize " << maxEmitBatchSize_;
}

SpoutInstance::~SpoutInstance() {
  spout_->close();
  delete spout_;
  if (lookForTimeoutsTimer_ > 0) {
    eventLoop_->unRegisterTimer(lookForTimeoutsTimer_);
  }
}

void SpoutInstance::Start() {
  CHECK(!active_);
  LOG(INFO) << "Starting spout " << taskContext_->getThisComponentName()
            << " with ackingEnabled?: " << ackingEnabled_
            << " with enableMessageTimeouts?: " << enableMessageTimeouts_;
  spout_->open(taskContext_->getConfig(), taskContext_, collector_);
  if (enableMessageTimeouts_) {
    int nBuckets = config::HeronInternalsConfigReader::Instance()
                             ->GetHeronInstanceAcknowledgementNbuckets();
    int messageTimeout = atoi(taskContext_->getConfig()
                             ->get(api::config::Config::TOPOLOGY_MESSAGE_TIMEOUT_SECS).c_str());
    int64_t timeoutMs = messageTimeout * 1000 * 1000;
    timeoutMs = timeoutMs / nBuckets;
    lookForTimeoutsTimer_ = eventLoop_->registerTimer(
          [this](EventLoop::Status status) { this->lookForTimeouts(); }, true, timeoutMs);
    CHECK_GT(lookForTimeoutsTimer_, 0);
  }
  active_ = true;
}

void SpoutInstance::Activate() {
  LOG(INFO) << "Came in Activate of the spout";
  CHECK(!active_);
  if (spout_) {
    spout_->activate();
  }
  active_ = true;
}

void SpoutInstance::Deactivate() {
  LOG(INFO) << "Came in Deactivate of the spout";
  CHECK(active_);
  if (spout_) {
    spout_->deactivate();
  }
  active_ = false;
}

void SpoutInstance::DoWork() {
  if (canProduceTuple()) {
    produceTuple();
    collector_->sendOutTuples();
  }
  if (!ackingEnabled_) {
    doImmediateAcks();
  }
  if (canContinueWork()) {
    eventLoop_->registerInstantCallback([this]() { this->DoWork(); });
  }
}

bool SpoutInstance::canProduceTuple() {
  return (active_ && dataFromExecutor_->size() < maxWriteBufferSize_);
}

void SpoutInstance::produceTuple() {
  int maxSpoutPending = atoi(taskContext_->getConfig()
                             ->get(api::config::Config::TOPOLOGY_MAX_SPOUT_PENDING).c_str());
  int64_t totalTuplesEmitted = collector_->getTotalDataTuplesEmitted();
  int64_t totalBytesEmitted = collector_->getTotalDataBytesEmitted();
  int64_t startTime = std::chrono::duration_cast<std::chrono::nanoseconds>(
                                   std::chrono::system_clock::now().time_since_epoch()).count();
  int64_t startOfCall = startTime;

  while (!ackingEnabled_ || (maxSpoutPending > collector_->numInFlight())) {
    spout_->nextTuple();
    int64_t currentTime = std::chrono::duration_cast<std::chrono::nanoseconds>(
                                   std::chrono::system_clock::now().time_since_epoch()).count();
    metrics_->nextTuple(currentTime - startOfCall);
    int64_t newTotalTuplesEmitted = collector_->getTotalDataTuplesEmitted();
    int64_t newTotalBytesEmitted = collector_->getTotalDataBytesEmitted();
    if (newTotalTuplesEmitted == totalTuplesEmitted) break;
    totalTuplesEmitted = newTotalTuplesEmitted;
    if (currentTime - startTime > maxEmitBatchIntervalMs_ * 1000 * 1000) break;
    if (newTotalBytesEmitted - totalBytesEmitted > maxEmitBatchSize_) break;
    startOfCall = currentTime;
  }
}

void SpoutInstance::doImmediateAcks() {
  // In this iteration, we will only look at the immediateAcks size
  // Otherwise, it could be that eveytime we do an ack, the spout is
  // doing generating another tuple leading to an infinite loop
  int nAcks = collector_->getImmediateAcksSize();
  for (int i = 0; i < nAcks; ++i) {
    std::shared_ptr<RootTupleInfo> tupleInfo = collector_->getImmediateAcksFront();
    spout_->ack(tupleInfo->getMessageId());
    metrics_->ackedTuple(tupleInfo->getStreamId(), 0);
  }
}

bool SpoutInstance::canContinueWork() {
  int maxSpoutPending = atoi(taskContext_->getConfig()
                             ->get(api::config::Config::TOPOLOGY_MAX_SPOUT_PENDING).c_str());
  return active_ && (
         (!ackingEnabled_ && dataFromExecutor_->size() < maxWriteBufferSize_) ||
         (ackingEnabled_ && dataFromExecutor_->size() < maxWriteBufferSize_ &&
          collector_->numInFlight() < maxSpoutPending));
}

void SpoutInstance::HandleGatewayTuples(pool_unique_ptr<proto::system::HeronTupleSet2> tupleSet) {
  if (tupleSet->has_data()) {
    LOG(FATAL) << "Spout cannot get incoming data tuples from other components";
  }

  if (tupleSet->has_control()) {
    for (auto ack : tupleSet->control().acks()) {
      handleAckTuple(ack, true);
    }
    for (auto ack : tupleSet->control().fails()) {
      handleAckTuple(ack, false);
    }
  }

  if (canContinueWork()) {
    eventLoop_->registerInstantCallback([this]() { this->DoWork(); });
  }
}

void SpoutInstance::handleAckTuple(const proto::system::AckTuple& ackTuple, bool isAck) {
  int64_t currentTime = std::chrono::duration_cast<std::chrono::nanoseconds>(
                                 std::chrono::system_clock::now().time_since_epoch()).count();
  for (auto root : ackTuple.roots()) {
    if (root.taskid() != taskContext_->getThisTaskId()) {
      LOG(FATAL) << "Receiving tuple for task " << root.taskid()
                 << " but our task id is " << taskContext_->getThisTaskId();
    }
    std::shared_ptr<RootTupleInfo> rootTupleInfo = collector_->retireInFlight(root.key());
    if (rootTupleInfo) {
      if (isAck) {
        spout_->ack(rootTupleInfo->getMessageId());
        metrics_->ackedTuple(rootTupleInfo->getStreamId(),
                             currentTime - rootTupleInfo->getInsertionTime());
      } else {
        spout_->fail(rootTupleInfo->getMessageId());
        metrics_->failedTuple(rootTupleInfo->getStreamId(),
                              currentTime - rootTupleInfo->getInsertionTime());
      }
    }
  }
}

void SpoutInstance::lookForTimeouts() {
  int messageTimeout = atoi(taskContext_->getConfig()
                            ->get(api::config::Config::TOPOLOGY_MESSAGE_TIMEOUT_SECS).c_str());
  std::list<std::shared_ptr<RootTupleInfo>> expired;
  collector_->retireExpired(messageTimeout, expired);
  for (auto rootTupleInfo : expired) {
    spout_->fail(rootTupleInfo->getMessageId());
    metrics_->timeoutTuple(rootTupleInfo->getStreamId());
    metrics_->failedTuple(rootTupleInfo->getStreamId(), messageTimeout);
  }
}

}  // namespace instance
}  // namespace heron
