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

#ifndef HERON_INSTANCE_BOLT_BOLT_OUTPUT_COLLECTOR_IMPL_H_
#define HERON_INSTANCE_BOLT_BOLT_OUTPUT_COLLECTOR_IMPL_H_

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
#include "bolt/ibolt-output-collector.h"
#include "serializer/ipluggable-serializer.h"
#include "executor/task-context-impl.h"
#include "executor/outgoing-tuple-collection.h"
#include "boltimpl/bolt-metrics.h"

namespace heron {
namespace instance {

class BoltOutputCollectorImpl : public api::bolt::IBoltOutputCollector {
 public:
  BoltOutputCollectorImpl(std::shared_ptr<api::serializer::IPluggableSerializer> serializer,
                          std::shared_ptr<TaskContextImpl> taskContext,
                          NotifyingCommunicator<google::protobuf::Message*>* dataFromExecutor,
                          std::shared_ptr<BoltMetrics> metrics);
  virtual ~BoltOutputCollectorImpl();

  virtual void reportError(std::exception& except);

  void sendOutTuples() { collector_->sendOutTuples(); }

  virtual void ack(std::shared_ptr<api::tuple::Tuple> tuple);
  virtual void fail(std::shared_ptr<api::tuple::Tuple> tuple);

 protected:
  virtual void emitInternal(const std::string& streamid,
                            std::list<std::shared_ptr<api::tuple::Tuple>>& anchors,
                            const std::vector<std::string>& tup);

 private:
  // The collector that actually holds the tuples
  OutgoingTupleCollection* collector_;
  // Metrics
  std::shared_ptr<BoltMetrics> metrics_;

  // For generating random keys
  std::default_random_engine generator_;
  std::uniform_int_distribution<int64_t> distribution_;

  int taskId_;
  bool ackingEnabled_;
};

}  // namespace instance
}  // namespace heron

#endif  // HERON_INSTANCE_BOLT_BOLT_OUTPUT_COLLECTOR_IMPL_H_
