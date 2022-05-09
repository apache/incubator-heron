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
#include <set>
#include <string>
#include <vector>

#include "boltimpl/bolt-output-collector-impl.h"
#include "proto/messages.h"
#include "network/network.h"
#include "basics/basics.h"

#include "boltimpl/tuple-impl.h"
#include "boltimpl/tick-tuple.h"

struct rootCompare {
  bool operator() (const heron::proto::system::RootId& lhs,
                   const heron::proto::system::RootId& rhs) const {
    return lhs.taskid() < rhs.taskid() ||
           (lhs.taskid() == rhs.taskid() && lhs.key() < rhs.key());
  }
};

namespace heron {
namespace instance {

BoltOutputCollectorImpl::BoltOutputCollectorImpl(
                          std::shared_ptr<api::serializer::IPluggableSerializer> serializer,
                          std::shared_ptr<TaskContextImpl> taskContext,
                          NotifyingCommunicator<google::protobuf::Message*>* dataFromExecutor,
                          std::shared_ptr<BoltMetrics> metrics)
  : api::bolt::IBoltOutputCollector(serializer), metrics_(metrics) {
  collector_ = new OutgoingTupleCollection(taskContext->getThisComponentName(), dataFromExecutor);
  ackingEnabled_ = taskContext->isAckingEnabled();
  taskId_ = taskContext->getThisTaskId();
}

BoltOutputCollectorImpl::~BoltOutputCollectorImpl() {
  delete collector_;
}

void BoltOutputCollectorImpl::reportError(std::exception& except) {
  LOG(INFO) << "Reporting an error in topology code " << except.what();
}

void BoltOutputCollectorImpl::emitInternal(const std::string& streamId,
                                           std::list<std::shared_ptr<api::tuple::Tuple>>& anchors,
                                           const std::vector<std::string>& tup) {
  auto msg = new proto::system::HeronDataTuple();
  msg->set_key(0);
  std::set<proto::system::RootId, rootCompare> mergedRoots;
  for (auto anchor : anchors) {
    std::shared_ptr<TupleImpl> t = std::dynamic_pointer_cast<TupleImpl>(anchor);
    if (t) {
      std::shared_ptr<const proto::system::HeronDataTuple> actualRepr = t->getDataTuple();
      for (int i = 0; i < actualRepr->roots_size(); ++i) {
        mergedRoots.insert(actualRepr->roots(i));
      }
    }
  }
  for (auto root : mergedRoots) {
    msg->add_roots()->CopyFrom(root);
  }
  int64_t totalSize = 0;
  for (auto& s : tup) {
    totalSize += s.size();
  }
  collector_->addDataTuple(streamId, msg, totalSize);
  metrics_->emittedTuple(streamId);
}

void BoltOutputCollectorImpl::ack(std::shared_ptr<api::tuple::Tuple> tup) {
  if (ackingEnabled_) {
    std::shared_ptr<TupleImpl> t = std::dynamic_pointer_cast<TupleImpl>(tup);
    if (t) {
      proto::system::AckTuple* ack = new proto::system::AckTuple();
      std::shared_ptr<const proto::system::HeronDataTuple> actualRepr = t->getDataTuple();
      ack->set_ackedtuple(actualRepr->key());
      int64_t tupSize = 0;
      for (int i = 0; i < actualRepr->roots_size(); ++i) {
        ack->add_roots()->CopyFrom(actualRepr->roots(i));
        tupSize += actualRepr->roots(i).ByteSizeLong();
      }
      collector_->addAckTuple(ack, tupSize);
      int64_t currentTime = std::chrono::duration_cast<std::chrono::nanoseconds>(
                                   std::chrono::system_clock::now().time_since_epoch()).count();
      metrics_->ackedTuple(t->getSourceStreamId(), t->getSourceComponent(),
                           currentTime - t->getCreationTimeNs());
    }
  }
}

void BoltOutputCollectorImpl::fail(std::shared_ptr<api::tuple::Tuple> tup) {
  if (ackingEnabled_) {
    std::shared_ptr<TupleImpl> t = std::dynamic_pointer_cast<TupleImpl>(tup);
    if (t) {
      proto::system::AckTuple* fl = new proto::system::AckTuple();
      std::shared_ptr<const proto::system::HeronDataTuple> actualRepr = t->getDataTuple();
      fl->set_ackedtuple(actualRepr->key());
      int64_t tupSize = 0;
      for (int i = 0; i < actualRepr->roots_size(); ++i) {
        fl->add_roots()->CopyFrom(actualRepr->roots(i));
        tupSize += actualRepr->roots(i).ByteSizeLong();
      }
      collector_->addFailTuple(fl, tupSize);
      int64_t currentTime = std::chrono::duration_cast<std::chrono::nanoseconds>(
                                   std::chrono::system_clock::now().time_since_epoch()).count();
      metrics_->failedTuple(t->getSourceStreamId(), t->getSourceComponent(),
                            currentTime - t->getCreationTimeNs());
    }
  }
}

}  // namespace instance
}  // namespace heron
