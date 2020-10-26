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

#ifndef HERON_INSTANCE_EXECUTOR_OUTGOING_TUPLE_COLLECTION_H_
#define HERON_INSTANCE_EXECUTOR_OUTGOING_TUPLE_COLLECTION_H_

#include <string>
#include "proto/messages.h"
#include "network/network.h"
#include "basics/basics.h"

#include "utils/notifying-communicator.h"

namespace heron {
namespace instance {

class OutgoingTupleCollection {
 public:
  OutgoingTupleCollection(const std::string& componentName,
                          NotifyingCommunicator<google::protobuf::Message*>* dataFromExecutor);
  ~OutgoingTupleCollection();

  void sendOutTuples();
  void addDataTuple(const std::string& streamId,
                    proto::system::HeronDataTuple* data,
                    int tupleSize);
  void addAckTuple(proto::system::AckTuple* ack,
                   int tupleSize);
  void addFailTuple(proto::system::AckTuple* ack,
                    int tupleSize);

  int64_t getTotalDataTuplesEmitted() const;
  int64_t getTotalDataSizeEmitted() const;

 private:
  void initNewDataTuple(const std::string& streamId);
  void initNewControlTuple();
  void flushRemaining();

  std::string componentName_;
  NotifyingCommunicator<google::protobuf::Message*>* dataFromExecutor_;
  proto::system::HeronDataTupleSet* currentDataTuple_;
  proto::system::HeronControlTupleSet* currentControlTuple_;
  int64_t totalDataSizeEmitted_;
  int64_t totalDataTuplesEmitted_;
  int64_t totalAckTuplesEmitted_;
  int64_t totalFailTuplesEmitted_;
  int64_t currentDataTupleSize_;
  int64_t maxDataTupleSize_;
  int64_t maxDataTupleSet_;
  int64_t maxControlTupleSet_;
};

}  // namespace instance
}  // namespace heron

#endif  // HERON_INSTANCE_EXECUTOR_OUTGOING_TUPLE_COLLECTION_H_
