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

#include <string>
#include <thread>

#include "glog/logging.h"

#include "executor/executor.h"
#include "proto/messages.h"
#include "network/network.h"
#include "basics/basics.h"

#include "executor/imetrics-registrar-impl.h"
#include "executor/task-context-impl.h"
#include "spoutimpl/spout-instance.h"
#include "boltimpl/bolt-instance.h"

namespace heron {
namespace instance {

Executor::Executor(int myTaskId, const std::string& topologySo)
  : myTaskId_(myTaskId), taskContext_(new TaskContextImpl(myTaskId_)),
    dataToExecutor_(NULL), dataFromExecutor_(NULL), metricsFromExecutor_(NULL),
    instance_(NULL), eventLoop_(std::make_shared<EventLoopImpl>()) {
  auto pplan = new proto::system::PhysicalPlan();
  pplan_typename_ = pplan->GetTypeName();
  delete pplan;
  dllHandle_ = dlopen(topologySo.c_str(), RTLD_LAZY);
  if (!dllHandle_) {
    LOG(FATAL) << "Could not dlopen " << topologySo << " failed with error "
               << dlerror();
  }
}

Executor::~Executor() {
  if (dlclose(dllHandle_) != 0) {
    LOG(FATAL) << "dlclose failed with error " << dlerror();
  }
}

void Executor::setCommunicators(
                    NotifyingCommunicator<pool_unique_ptr<google::protobuf::Message>>* dataToExecutor,
                    NotifyingCommunicator<google::protobuf::Message*>* dataFromExecutor,
                    NotifyingCommunicator<google::protobuf::Message*>* metricsFromExecutor) {
  dataToExecutor_ = dataToExecutor;
  dataFromExecutor_ = dataFromExecutor;
  metricsFromExecutor_ = metricsFromExecutor;
  std::shared_ptr<api::metric::IMetricsRegistrar> registrar(new IMetricsRegistrarImpl(eventLoop_,
                                                                metricsFromExecutor));
  taskContext_->setMericsRegistrar(registrar);
}

void Executor::Start() {
  LOG(INFO) << "Creating executor thread";
  executorThread_.reset(new std::thread(&Executor::InternalStart, this));
}

// This is the one thats running in the executor thread
void Executor::InternalStart() {
  LOG(INFO) << "Executor thread started up";
  eventLoop_->loop();
}

void Executor::HandleGatewayData(pool_unique_ptr<google::protobuf::Message> msg) {
  if (msg->GetTypeName() == pplan_typename_) {
    LOG(INFO) << "Executor Received a new pplan message from Gateway";
    auto pplan = pool_unique_ptr<proto::system::PhysicalPlan>(
            static_cast<proto::system::PhysicalPlan*>(msg.release()));
    HandleNewPhysicalPlan(std::move(pplan));
  } else {
    auto tupleSet = pool_unique_ptr<proto::system::HeronTupleSet2>(
            static_cast<proto::system::HeronTupleSet2*>(msg.release()));
    HandleStMgrTuples(std::move(tupleSet));
  }
}

void Executor::HandleNewPhysicalPlan(pool_unique_ptr<proto::system::PhysicalPlan> pplan) {
  std::shared_ptr<proto::system::PhysicalPlan> newPplan = std::move(pplan);
  taskContext_->newPhysicalPlan(newPplan);
  if (!instance_) {
    LOG(INFO) << "Creating the instance for the first time";
    if (taskContext_->isSpout()) {
      LOG(INFO) << "We are a spout";
      instance_ = new SpoutInstance(eventLoop_, taskContext_,
                                    dataFromExecutor_, dllHandle_);
    } else {
      LOG(INFO) << "We are a bolt";
      instance_ = new BoltInstance(eventLoop_, taskContext_,
                                   dataToExecutor_, dataFromExecutor_, dllHandle_);
    }
    if (newPplan->topology().state() == proto::api::TopologyState::RUNNING) {
      LOG(INFO) << "Starting the instance";
      instance_->Start();
      instance_->DoWork();
    } else {
      LOG(INFO) << "Not starting the instance";
    }
  } else {
    if (newPplan->topology().state() == proto::api::TopologyState::RUNNING &&
        !instance_->IsRunning()) {
      instance_->Activate();
    } else if (newPplan->topology().state() == proto::api::TopologyState::PAUSED &&
        instance_->IsRunning()) {
      instance_->Deactivate();
    }
  }
}

void Executor::HandleStMgrTuples(pool_unique_ptr<proto::system::HeronTupleSet2> tupleSet) {
  if (instance_) {
    instance_->HandleGatewayTuples(std::move(tupleSet));
  } else {
    LOG(FATAL) << "Received StMgr tuples before instance was instantiated";
  }
}

void Executor::HandleGatewayDataConsumed() {
  if (instance_) {
    instance_->DoWork();
  }
}

}  // namespace instance
}  // namespace heron
