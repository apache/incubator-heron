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

#include <string>

#include "glog/logging.h"

#include "gateway/gateway.h"
#include "proto/messages.h"
#include "network/network.h"
#include "basics/basics.h"

#include "config/heron-internals-config-reader.h"
#include "config/topology-config-helper.h"
#include "gateway/stmgr-client.h"

namespace heron {
namespace instance {

Gateway::Gateway(const std::string& topologyName,
                 const std::string& topologyId, const std::string& instanceId,
                 const std::string& componentName, int taskId, int componentIndex,
                 const std::string& stmgrId, int stmgrPort, int metricsMgrPort,
                 std::shared_ptr<EventLoop> eventLoop)
  : topologyName_(topologyName), topologyId_(topologyId), stmgrPort_(stmgrPort),
    metricsMgrPort_(metricsMgrPort), dataToExecutor_(NULL), dataFromExecutor_(NULL),
    metricsFromExecutor_(NULL), eventLoop_(eventLoop),
    maxReadBufferSize_(128), maxWriteBufferSize_(128),
    readingFromExecutor_(true) {
  maxPacketSize_ = config::HeronInternalsConfigReader::Instance()
                           ->GetHeronStreammgrNetworkOptionsMaximumPacketMb() * 1_MB;
  instanceProto_.set_instance_id(instanceId);
  instanceProto_.set_stmgr_id(stmgrId);
  instanceProto_.mutable_info()->set_task_id(taskId);
  instanceProto_.mutable_info()->set_component_index(componentIndex);
  instanceProto_.mutable_info()->set_component_name(componentName);
}

Gateway::~Gateway() { }

void Gateway::Start() {
  NetworkOptions metricsOptions;
  metricsOptions.set_host("127.0.0.1");
  metricsOptions.set_port(metricsMgrPort_);
  metricsOptions.set_max_packet_size(1_MB);
  metricsOptions.set_socket_family(PF_INET);
  metricsMgrClient_.reset(new common::MetricsMgrClient(IpUtils::getHostName(),
                          instanceProto_.info().task_id(),
                          instanceProto_.info().component_name(),
                          instanceProto_.instance_id(),
                          instanceProto_.info().component_index(),
                          eventLoop_, metricsOptions));

  gatewayMetrics_.reset(new GatewayMetrics(metricsMgrClient_, eventLoop_));

  NetworkOptions clientOptions;
  clientOptions.set_host("127.0.0.1");
  clientOptions.set_port(stmgrPort_);
  clientOptions.set_max_packet_size(config::HeronInternalsConfigReader::Instance()
                                  ->GetHeronStreammgrNetworkOptionsMaximumPacketMb() * 1_MB);
  clientOptions.set_socket_family(PF_INET);
  stmgrClient_.reset(new StMgrClient(eventLoop_, clientOptions, topologyName_, topologyId_,
                                     instanceProto_, gatewayMetrics_,
                                     std::bind(&Gateway::HandleNewPhysicalPlan, this,
                                               std::placeholders::_1),
                                     std::bind(&Gateway::HandleStMgrTuples, this,
                                               std::placeholders::_1)));
  stmgrClient_->Start();

  // Setup timer to periodically check for resumption of executor consumption
  CHECK_GT(
      eventLoop_->registerTimer(
          [this](EventLoop::Status status) { this->ResumeConsumingFromExecutorTimer(); }, true,
          10 * 1000), 0);
  eventLoop_->loop();
}

void Gateway::HandleNewPhysicalPlan(pool_unique_ptr<proto::system::PhysicalPlan> pplan) {
  LOG(INFO) << "Received a new physical plan from Stmgr";
  if (config::TopologyConfigHelper::IsComponentSpout(pplan->topology(),
                                                     instanceProto_.info().component_name())) {
    maxReadBufferSize_ = config::HeronInternalsConfigReader::Instance()
                                ->GetHeronInstanceInternalSpoutReadQueueCapacity();
    maxWriteBufferSize_ = config::HeronInternalsConfigReader::Instance()
                                ->GetHeronInstanceInternalSpoutWriteQueueCapacity();
  } else {
    maxReadBufferSize_ = config::HeronInternalsConfigReader::Instance()
                                ->GetHeronInstanceInternalBoltReadQueueCapacity();
    maxWriteBufferSize_ = config::HeronInternalsConfigReader::Instance()
                                ->GetHeronInstanceInternalBoltWriteQueueCapacity();
  }

  dataToExecutor_->enqueue(std::move(pplan));
}

void Gateway::HandleStMgrTuples(pool_unique_ptr<proto::system::HeronTupleSet2> msg) {
  dataToExecutor_->enqueue(std::move(msg));
  if (dataToExecutor_->size() > maxReadBufferSize_) {
    stmgrClient_->putBackPressure();
  }
}

void Gateway::HandleExecutorDataConsumed() {
  if (dataToExecutor_->size() < maxReadBufferSize_) {
    stmgrClient_->removeBackPressure();
  }
}

void Gateway::HandleExecutorData(google::protobuf::Message* msg) {
  auto tupleSet = static_cast<proto::system::HeronTupleSet*>(msg);
  stmgrClient_->SendTupleMessage(*tupleSet);
  delete tupleSet;
  if (stmgrClient_->getOutstandingBytes() > (maxWriteBufferSize_ * maxPacketSize_) &&
      readingFromExecutor_) {
    LOG(INFO) << "Gateway buffered too much data to be written to stmgr; "
              << "Clamping down on consumption from executor";
    dataFromExecutor_->stopConsumption();
    readingFromExecutor_ = false;
  }
}

void Gateway::HandleExecutorMetrics(google::protobuf::Message* msg) {
  auto metrics = static_cast<proto::system::MetricPublisherPublishMessage*>(msg);
  metricsMgrClient_->SendMetrics(metrics);
}

void Gateway::ResumeConsumingFromExecutorTimer() {
  if (stmgrClient_->getOutstandingBytes() < (maxWriteBufferSize_ * maxPacketSize_) &&
      !readingFromExecutor_) {
    LOG(INFO) << "Gateway buffer now under max limit; "
              << "Resuming consumption from executor";
    dataFromExecutor_->resumeConsumption();
    readingFromExecutor_ = true;
  }
}

}  // namespace instance
}  // namespace heron
