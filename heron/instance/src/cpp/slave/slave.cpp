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

#include "slave/slave.h"
#include "proto/messages.h"
#include "network/network.h"
#include "basics/basics.h"

#include "slave/imetrics-registrar-impl.h"
#include "slave/task-context-impl.h"
#include "spoutimpl/spout-instance.h"
#include "boltimpl/bolt-instance.h"

namespace heron {
namespace instance {

Slave::Slave(int myTaskId, const std::string& topologySo)
  : myTaskId_(myTaskId), taskContext_(new TaskContextImpl(myTaskId_)),
    dataToSlave_(NULL), dataFromSlave_(NULL), metricsFromSlave_(NULL),
    instance_(NULL), eventLoop_(new EventLoopImpl()) {
  auto pplan = new proto::system::PhysicalPlan();
  pplan_typename_ = pplan->GetTypeName();
  delete pplan;
  dllHandle_ = dlopen(topologySo.c_str(), RTLD_LAZY);
  if (!dllHandle_) {
    LOG(FATAL) << "Could not dlopen " << topologySo << " failed with error "
               << dlerror();
  }
}

Slave::~Slave() {
  if (dlclose(dllHandle_) != 0) {
    LOG(FATAL) << "dlclose failed with error " << dlerror();
  }
}

void Slave::setCommunicators(NotifyingCommunicator<google::protobuf::Message*>* dataToSlave,
                             NotifyingCommunicator<google::protobuf::Message*>* dataFromSlave,
                             NotifyingCommunicator<google::protobuf::Message*>* metricsFromSlave) {
  dataToSlave_ = dataToSlave;
  dataFromSlave_ = dataFromSlave;
  metricsFromSlave_ = metricsFromSlave;
  std::shared_ptr<api::metric::IMetricsRegistrar> registrar(new IMetricsRegistrarImpl(eventLoop_,
                                                                metricsFromSlave));
  taskContext_->setMericsRegistrar(registrar);
}

void Slave::Start() {
  LOG(INFO) << "Creating slave thread";
  slaveThread_.reset(new std::thread(&Slave::InternalStart, this));
}

// This is the one thats running in the slave thread
void Slave::InternalStart() {
  LOG(INFO) << "Slave thread started up";
  eventLoop_->loop();
}

void Slave::HandleGatewayData(google::protobuf::Message* msg) {
  if (msg->GetTypeName() == pplan_typename_) {
    LOG(INFO) << "Slave Received a new pplan message from Gateway";
    auto pplan = static_cast<proto::system::PhysicalPlan*>(msg);
    HandleNewPhysicalPlan(pplan);
  } else {
    auto tupleSet = static_cast<proto::system::HeronTupleSet2*>(msg);
    HandleStMgrTuples(tupleSet);
  }
}

void Slave::HandleNewPhysicalPlan(proto::system::PhysicalPlan* pplan) {
  taskContext_->newPhysicalPlan(pplan);
  if (!instance_) {
    LOG(INFO) << "Creating the instance for the first time";
    if (taskContext_->isSpout()) {
      LOG(INFO) << "We are a spout";
      instance_ = new SpoutInstance(eventLoop_, taskContext_,
                                    dataFromSlave_, dllHandle_);
    } else {
      LOG(INFO) << "We are a bolt";
      instance_ = new BoltInstance(eventLoop_, taskContext_,
                                   dataToSlave_, dataFromSlave_, dllHandle_);
    }
    if (pplan->topology().state() == proto::api::TopologyState::RUNNING) {
      LOG(INFO) << "Starting the instance";
      instance_->Start();
      instance_->DoWork();
    } else {
      LOG(INFO) << "Not starting the instance";
    }
  } else {
    if (pplan->topology().state() == proto::api::TopologyState::RUNNING &&
        !instance_->IsRunning()) {
      instance_->Activate();
    } else if (pplan->topology().state() == proto::api::TopologyState::PAUSED &&
        instance_->IsRunning()) {
      instance_->Deactivate();
    }
  }
}

void Slave::HandleStMgrTuples(proto::system::HeronTupleSet2* tupleSet) {
  if (instance_) {
    instance_->HandleGatewayTuples(tupleSet);
  } else {
    LOG(FATAL) << "Received StMgr tuples before instance was instantiated";
  }
}

void Slave::HandleGatewayDataConsumed() {
  if (instance_) {
    instance_->DoWork();
  }
}

}  // namespace instance
}  // namespace heron
