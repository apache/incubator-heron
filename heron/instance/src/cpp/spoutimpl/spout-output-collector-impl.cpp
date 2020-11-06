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

#include <list>
#include <map>
#include <string>
#include <vector>

#include "spoutimpl/spout-output-collector-impl.h"
#include "proto/messages.h"
#include "network/network.h"
#include "basics/basics.h"

namespace heron {
namespace instance {

SpoutOutputCollectorImpl::SpoutOutputCollectorImpl(
                          std::shared_ptr<api::serializer::IPluggableSerializer> serializer,
                          std::shared_ptr<TaskContextImpl> taskContext,
                          NotifyingCommunicator<google::protobuf::Message*>* dataFromExecutor)
  : api::spout::ISpoutOutputCollector(serializer) {
  collector_ = new OutgoingTupleCollection(taskContext->getThisComponentName(), dataFromExecutor);
  ackingEnabled_ = taskContext->isAckingEnabled();
  taskId_ = taskContext->getThisTaskId();
}

SpoutOutputCollectorImpl::~SpoutOutputCollectorImpl() {
  delete collector_;
}

void SpoutOutputCollectorImpl::reportError(std::exception& except) {
  LOG(INFO) << "Reporting an error in topology code " << except.what();
}

void SpoutOutputCollectorImpl::emitInternal(const std::string& streamId,
                                            const std::vector<std::string>& tup,
                                            int64_t msgId) {
  auto msg = new proto::system::HeronDataTuple();
  msg->set_key(0);
  if (msgId >= 0) {
    std::shared_ptr<RootTupleInfo> tupleInfo(new RootTupleInfo(streamId, msgId));
    if (ackingEnabled_) {
      // This message is rooted
      auto root = msg->add_roots();
      root->set_taskid(taskId_);
      int64_t rootId = distribution_(generator_);
      root->set_key(rootId);
      inflightTuples_[rootId] = tupleInfo;
      reverseInflightTuples_[tupleInfo] = rootId;
    } else {
      immediateAcks_.push_back(tupleInfo);
    }
  }

  int tupleSize = 0;
  for (auto col : tup) {
    msg->add_values(col);
    tupleSize += col.size();
  }
  collector_->addDataTuple(streamId, msg, tupleSize);
}

std::shared_ptr<RootTupleInfo> SpoutOutputCollectorImpl::retireInFlight(int64_t key) {
  std::shared_ptr<RootTupleInfo> retval;
  std::map<int64_t, std::shared_ptr<RootTupleInfo>>::iterator iter;
  iter = inflightTuples_.find(key);
  if (iter == inflightTuples_.end()) {
    return retval;
  } else {
    retval = iter->second;
    inflightTuples_.erase(iter);
    reverseInflightTuples_.erase(retval);
    return retval;
  }
}

void SpoutOutputCollectorImpl::retireExpired(int messageTimeout,
                                             std::list<std::shared_ptr<RootTupleInfo>>& expired) {
  int64_t currentTimeNs =  std::chrono::duration_cast<std::chrono::nanoseconds>(
                                   std::chrono::system_clock::now().time_since_epoch()).count();
  int64_t timeout = messageTimeout * 1000 * 1000 * 1000;
  // Note:- we are going in the reverse order of the reverseInflightTuples
  // This ensures that we are getting the oldest tuples first
  auto it = reverseInflightTuples_.crbegin();
  while (it != reverseInflightTuples_.crend()) {
    if (it->first->isExpired(currentTimeNs, timeout)) {
      expired.push_back(it->first);
      inflightTuples_.erase(it->second);
      reverseInflightTuples_.erase(it->first);
      it = reverseInflightTuples_.crbegin();
    } else {
      break;
    }
  }
}

}  // namespace instance
}  // namespace heron
