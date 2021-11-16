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

#ifndef HERON_INSTANCE_SPOUT_SPOUT_OUTPUT_COLLECTOR_IMPL_H_
#define HERON_INSTANCE_SPOUT_SPOUT_OUTPUT_COLLECTOR_IMPL_H_

#include <map>
#include <list>
#include <string>
#include <memory>
#include <random>
#include <vector>
#include "proto/messages.h"
#include "network/network.h"
#include "basics/basics.h"

#include "utils/notifying-communicator.h"
#include "spout/ispout-output-collector.h"
#include "serializer/ipluggable-serializer.h"
#include "spoutimpl/root-tuple-info.h"
#include "executor/task-context-impl.h"
#include "executor/outgoing-tuple-collection.h"

namespace heron {
namespace instance {

class SpoutOutputCollectorImpl : public api::spout::ISpoutOutputCollector {
 public:
  SpoutOutputCollectorImpl(std::shared_ptr<api::serializer::IPluggableSerializer> serializer,
                           std::shared_ptr<TaskContextImpl> taskContext,
                           NotifyingCommunicator<google::protobuf::Message*>* dataFromExecutor);
  virtual ~SpoutOutputCollectorImpl();

  virtual void reportError(std::exception& except);

  int64_t getTotalDataTuplesEmitted() const { return collector_->getTotalDataTuplesEmitted(); }
  int64_t getTotalDataBytesEmitted() const { return collector_->getTotalDataSizeEmitted(); }
  int64_t numInFlight() const { return inflightTuples_.size(); }
  int getImmediateAcksSize() const { return immediateAcks_.size(); }
  std::shared_ptr<RootTupleInfo> getImmediateAcksFront() {
    std::shared_ptr<RootTupleInfo> retval = immediateAcks_.front();
    immediateAcks_.pop_front();
    return retval;
  }
  std::shared_ptr<RootTupleInfo> retireInFlight(int64_t key);
  void retireExpired(int messageTimeout, std::list<std::shared_ptr<RootTupleInfo>>& expired);
  void sendOutTuples() { collector_->sendOutTuples(); }

 protected:
  virtual void emitInternal(const std::string& streamid,
                            const std::vector<std::string>& tup,
                            int64_t msgId);

 private:
  // The collector that actually holds the tuples
  OutgoingTupleCollection* collector_;

  // For generating random keys
  std::default_random_engine generator_;
  std::uniform_int_distribution<int64_t> distribution_;

  // map from RootId -> TupleInfo of tuples that are still being tracked
  // for their acks
  std::map<int64_t, std::shared_ptr<RootTupleInfo>> inflightTuples_;
  // map from TupleInfo -> RootId of tuples that are still being tracked
  // for their acks
  std::map<std::shared_ptr<RootTupleInfo>, int64_t> reverseInflightTuples_;
  // Tuples that need to be immediately acked
  std::list<std::shared_ptr<RootTupleInfo>> immediateAcks_;
  int taskId_;
  bool ackingEnabled_;
};

}  // namespace instance
}  // namespace heron

#endif  // HERON_INSTANCE_SPOUT_SPOUT_OUTPUT_COLLECTOR_IMPL_H_
