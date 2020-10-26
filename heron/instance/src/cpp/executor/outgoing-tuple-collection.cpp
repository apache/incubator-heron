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
#include "executor/outgoing-tuple-collection.h"
#include "proto/messages.h"
#include "network/network.h"
#include "basics/basics.h"

#include "config/heron-internals-config-reader.h"

namespace heron {
namespace instance {

OutgoingTupleCollection::OutgoingTupleCollection(const std::string& componentName,
                             NotifyingCommunicator<google::protobuf::Message*>* dataFromExecutor)
  : componentName_(componentName), dataFromExecutor_(dataFromExecutor),
    currentDataTuple_(NULL), currentControlTuple_(NULL),
    totalDataSizeEmitted_(0), totalDataTuplesEmitted_(0),
    totalAckTuplesEmitted_(0), totalFailTuplesEmitted_(0), currentDataTupleSize_(0) {
  maxDataTupleSize_ = config::HeronInternalsConfigReader::Instance()
                             ->GetHeronInstanceSetDataTupleSizeBytes();
  maxDataTupleSet_ = config::HeronInternalsConfigReader::Instance()
                             ->GetHeronInstanceSetDataTupleCapacity();
  maxControlTupleSet_ =
      config::HeronInternalsConfigReader::Instance()
                             ->GetHeronInstanceSetControlTupleCapacity();
}

OutgoingTupleCollection::~OutgoingTupleCollection() {
  delete currentDataTuple_;
  delete currentControlTuple_;
}

void OutgoingTupleCollection::sendOutTuples() {
  flushRemaining();
}

void OutgoingTupleCollection::addDataTuple(const std::string& streamId,
                                           proto::system::HeronDataTuple* data,
                                           int tupleSize) {
  if (!currentDataTuple_ ||
      currentDataTuple_->stream().id() != streamId ||
      currentDataTuple_->tuples_size() >= maxDataTupleSet_ ||
      currentDataTupleSize_ >= maxDataTupleSize_) {
    initNewDataTuple(streamId);
  }
  currentDataTuple_->mutable_tuples()->AddAllocated(data);
  currentDataTupleSize_ += tupleSize;
  totalDataSizeEmitted_ += tupleSize;
  totalDataTuplesEmitted_++;
}

void OutgoingTupleCollection::addAckTuple(proto::system::AckTuple* ack,
                                          int tupleSize) {
  if (!currentControlTuple_ ||
       currentControlTuple_->fails_size() > 0 ||
       currentControlTuple_->acks_size() >= maxControlTupleSet_) {
    initNewControlTuple();
  }
  currentControlTuple_->mutable_acks()->AddAllocated(ack);
  totalAckTuplesEmitted_++;
}

void OutgoingTupleCollection::addFailTuple(proto::system::AckTuple* ack,
                                          int tupleSize) {
  if (!currentControlTuple_ ||
       currentControlTuple_->acks_size() > 0 ||
       currentControlTuple_->fails_size() >= maxControlTupleSet_) {
    initNewControlTuple();
  }
  currentControlTuple_->mutable_fails()->AddAllocated(ack);
  totalFailTuplesEmitted_++;
}

void OutgoingTupleCollection::initNewDataTuple(const std::string& streamId) {
  flushRemaining();
  currentDataTupleSize_ = 0;
  currentDataTuple_ = new proto::system::HeronDataTupleSet();
  currentDataTuple_->mutable_stream()->set_id(streamId);
  currentDataTuple_->mutable_stream()->set_component_name(componentName_);
}

void OutgoingTupleCollection::initNewControlTuple() {
  flushRemaining();
  currentControlTuple_ = new proto::system::HeronControlTupleSet();
}

void OutgoingTupleCollection::flushRemaining() {
  if (currentDataTuple_) {
    auto msg = new proto::system::HeronTupleSet();
    msg->set_allocated_data(currentDataTuple_);
    dataFromExecutor_->enqueue(msg);
    currentDataTuple_ = NULL;
    currentDataTupleSize_ = 0;
  }
  if (currentControlTuple_) {
    auto msg = new proto::system::HeronTupleSet();
    msg->set_allocated_control(currentControlTuple_);
    dataFromExecutor_->enqueue(msg);
    currentControlTuple_ = NULL;
  }
}

int64_t OutgoingTupleCollection::getTotalDataSizeEmitted() const {
  return totalDataSizeEmitted_;
}

int64_t OutgoingTupleCollection::getTotalDataTuplesEmitted() const {
  return totalDataTuplesEmitted_;
}

}  // namespace instance
}  // namespace heron
