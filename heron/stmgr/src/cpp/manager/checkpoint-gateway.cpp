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

#include "manager/checkpoint-gateway.h"
#include <functional>
#include <iostream>
#include <deque>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include "util/neighbour-calculator.h"
#include "metrics/metrics.h"
#include "proto/messages.h"
#include "basics/basics.h"
#include "errors/errors.h"
#include "threads/threads.h"
#include "network/network.h"

namespace heron {
namespace stmgr {

using proto::ckptmgr::InitiateStatefulCheckpoint;

CheckpointGateway::CheckpointGateway(sp_uint64 _drain_threshold,
         shared_ptr<NeighbourCalculator> _neighbour_calculator,
         shared_ptr<common::MetricsMgrSt> const& _metrics_manager_client,
         std::function<void(sp_int32, proto::system::HeronTupleSet2*)> _tupleset_drainer,
         std::function<void(proto::stmgr::TupleStreamMessage*)> _tuplestream_drainer,
         std::function<void(sp_int32, pool_unique_ptr<InitiateStatefulCheckpoint>)> _ckpt_drainer)
  : drain_threshold_(_drain_threshold), current_size_(0),
    neighbour_calculator_(_neighbour_calculator),
    metrics_manager_client_(_metrics_manager_client), tupleset_drainer_(_tupleset_drainer),
    tuplestream_drainer_(_tuplestream_drainer), ckpt_drainer_(_ckpt_drainer) {
  size_metric_ = std::make_shared<common::AssignableMetric>(current_size_);
  metrics_manager_client_->register_metric("__stateful_gateway_size", size_metric_);
}

CheckpointGateway::~CheckpointGateway() {
  pending_tuples_.erase(pending_tuples_.begin(), pending_tuples_.end());
  metrics_manager_client_->unregister_metric("__stateful_gateway_size");
}

void CheckpointGateway::SendToInstance(sp_int32 _task_id,
                                       proto::system::HeronTupleSet2* _message) {
  if (current_size_ > drain_threshold_) {
    ForceDrain();
  }
  CheckpointInfo& info = get_info(_task_id);
  sp_uint64 size = _message->GetCachedSize();
  _message = info.SendToInstance(_message, size);
  if (!_message) {
    current_size_ += size;
  } else {
    tupleset_drainer_(_task_id, _message);
  }
  size_metric_->SetValue(current_size_);
}

void CheckpointGateway::SendToInstance(pool_unique_ptr<proto::stmgr::TupleStreamMessage> _message) {
  if (current_size_ > drain_threshold_) {
    ForceDrain();
  }

  sp_int32 task_id = _message->task_id();
  sp_uint64 size = _message->set().size();
  CheckpointInfo& info = get_info(task_id);

  proto::stmgr::TupleStreamMessage *raw_message = info.SendToInstance(_message.release(), size);

  if (!raw_message) {
    current_size_ += size;
  } else {
    tuplestream_drainer_(raw_message);
  }

  size_metric_->SetValue(current_size_);
}

void CheckpointGateway::HandleUpstreamMarker(sp_int32 _src_task_id, sp_int32 _destination_task_id,
                                             const sp_string& _checkpoint_id) {
  LOG(INFO) << "Got checkpoint marker for triplet "
            << _checkpoint_id << " " << _src_task_id << " " << _destination_task_id;
  CheckpointInfo& info = get_info(_destination_task_id);
  sp_uint64 size = 0;
  std::deque<Tuple> tuples = info.HandleUpstreamMarker(_src_task_id, _checkpoint_id, &size);
  for (auto &tupl : tuples) {
    DrainTuple(_destination_task_id, tupl);
  }
  current_size_ -= size;
  size_metric_->SetValue(current_size_);
}

void CheckpointGateway::DrainTuple(sp_int32 _dest, Tuple& _tuple) {
  if (std::get<0>(_tuple)) {
    tupleset_drainer_(_dest, std::get<0>(_tuple));
  } else if (std::get<1>(_tuple)) {
    tuplestream_drainer_(std::get<1>(_tuple));
  } else {
    ckpt_drainer_(_dest, std::move(std::get<2>(_tuple)));
  }
}

void CheckpointGateway::ForceDrain() {
  for (auto &kv : pending_tuples_) {
    std::deque<Tuple> tuples = kv.second->ForceDrain();
    for (auto &tupl : tuples) {
      DrainTuple(kv.first, tupl);
    }
  }
  current_size_ = 0;
  size_metric_->SetValue(current_size_);
}

CheckpointGateway::CheckpointInfo& CheckpointGateway::get_info(sp_int32 _task_id) {
  auto iter = pending_tuples_.find(_task_id);
  if (iter == pending_tuples_.end()) {
    auto info =
        make_unique<CheckpointInfo>(_task_id, neighbour_calculator_->get_upstreamers(_task_id));
    pending_tuples_[_task_id] = std::move(info);
    return *pending_tuples_[_task_id];
  } else {
    return *(iter->second);
  }
}

void CheckpointGateway::Clear() {
  for (auto &kv : pending_tuples_) {
    kv.second->Clear();
    pending_tuples_.erase(kv.first);
  }

  pending_tuples_.clear();
  current_size_ = 0;
  size_metric_->SetValue(current_size_);
}

CheckpointGateway::CheckpointInfo::CheckpointInfo(sp_int32 _this_task_id,
               const std::unordered_set<sp_int32>& _all_upstream_dependencies) {
  checkpoint_id_ = "";
  all_upstream_dependencies_ = _all_upstream_dependencies;
  pending_upstream_dependencies_ = all_upstream_dependencies_;
  current_size_ = 0;
  this_task_id_ = _this_task_id;
}

CheckpointGateway::CheckpointInfo::~CheckpointInfo() {
  CHECK(pending_tuples_.empty());
  CHECK_EQ(current_size_, 0);
}

proto::system::HeronTupleSet2*
CheckpointGateway::CheckpointInfo::SendToInstance(proto::system::HeronTupleSet2* _tuple,
                                                  sp_uint64 _size) {
  if (checkpoint_id_.empty()) {
    return _tuple;
  } else {
    if (pending_upstream_dependencies_.find(_tuple->src_task_id()) !=
        pending_upstream_dependencies_.end()) {
      // This means that we still are expecting a checkpoint marker from this src task id
      return _tuple;
    } else {
      auto tp = std::make_tuple(_tuple,
                              (proto::stmgr::TupleStreamMessage*)nullptr,
                              (pool_unique_ptr<proto::ckptmgr::InitiateStatefulCheckpoint>)nullptr);
      add(tp, _size);
      return nullptr;
    }
  }
}

proto::stmgr::TupleStreamMessage*
CheckpointGateway::CheckpointInfo::SendToInstance(proto::stmgr::TupleStreamMessage* _tuple,
                                                  sp_uint64 _size) {
  if (checkpoint_id_.empty()) {
    return _tuple;
  } else {
    if (pending_upstream_dependencies_.find(_tuple->src_task_id()) !=
        pending_upstream_dependencies_.end()) {
      // This means that we still are expecting a checkpoint marker from this src task id
      return _tuple;
    } else {
      auto tp = std::make_tuple((proto::system::HeronTupleSet2*)nullptr,
                              _tuple,
                              (pool_unique_ptr<proto::ckptmgr::InitiateStatefulCheckpoint>)nullptr);
      add(tp, _size);
      return nullptr;
    }
  }
}

std::deque<CheckpointGateway::Tuple>
CheckpointGateway::CheckpointInfo::HandleUpstreamMarker(sp_int32 _src_task_id,
                                                        const sp_string& _checkpoint_id,
                                                        sp_uint64* _size) {
  if (_checkpoint_id == checkpoint_id_) {
    pending_upstream_dependencies_.erase(_src_task_id);
  } else if (checkpoint_id_.empty()) {
    LOG(INFO) << "TaskId: " << this_task_id_
              << " Seeing the checkpoint marker " << _checkpoint_id
              << " for the first time";
    checkpoint_id_ = _checkpoint_id;
    pending_upstream_dependencies_.erase(_src_task_id);
  } else if (_checkpoint_id > checkpoint_id_) {
    LOG(INFO) << "TaskId: " << this_task_id_
              << " Seeing the checkpoint marker " << _checkpoint_id
              << " while we were already amidst " << checkpoint_id_
              << " ..resetting";
    checkpoint_id_ = _checkpoint_id;
    pending_upstream_dependencies_ = all_upstream_dependencies_;
    pending_upstream_dependencies_.erase(_src_task_id);
  } else {
    LOG(WARNING) << "TaskId: " << this_task_id_
                 << " Discarding older checkpoint_id message "
                 << _checkpoint_id << " from upstream task "
                 << _src_task_id;
  }
  if (pending_upstream_dependencies_.empty()) {
    LOG(INFO) << "TaskId: " << this_task_id_
              << " All checkpoint markers received for checkpoint "
                 << _checkpoint_id;
    // We need to add Initiate Checkpoint message before the current set
    auto message = make_unique_from_protobuf_pool<proto::ckptmgr::InitiateStatefulCheckpoint>();
    message->set_checkpoint_id(_checkpoint_id);
    int cache_size = message->GetCachedSize();
    auto new_tuple = std::make_tuple(
            (proto::system::HeronTupleSet2*)nullptr,
            (proto::stmgr::TupleStreamMessage*)nullptr, std::move(message));
    add_front(new_tuple, cache_size);
    return ForceDrain();
  } else {
    std::deque<Tuple> dummy;
    return dummy;
  }
}

std::deque<CheckpointGateway::Tuple>
CheckpointGateway::CheckpointInfo::ForceDrain() {
  checkpoint_id_ = "";
  current_size_ = 0;
  std::deque<Tuple> tmp;

  for (auto it = pending_tuples_.begin(); it != pending_tuples_.end(); ++it) {
    auto m1 = std::get<0>(*it);
    auto m2 = std::get<1>(*it);
    auto m3 = std::move(std::get<2>(*it));
    tmp.push_back(std::make_tuple(m1, m2, std::move(m3)));
  }

  pending_tuples_.clear();

  pending_upstream_dependencies_ = all_upstream_dependencies_;

  return tmp;
}

void CheckpointGateway::CheckpointInfo::add(Tuple& _tuple, sp_uint64 _size) {
  auto m1 = std::get<0>(_tuple);
  auto m2 = std::get<1>(_tuple);
  auto m3 = std::move(std::get<2>(_tuple));
  pending_tuples_.push_back(std::make_tuple(m1, m2, std::move(m3)));
  current_size_ += _size;
}

void CheckpointGateway::CheckpointInfo::add_front(Tuple& _tuple, sp_uint64 _size) {
  auto m1 = std::get<0>(_tuple);
  auto m2 = std::get<1>(_tuple);
  auto m3 = std::move(std::get<2>(_tuple));
  pending_tuples_.push_front(std::make_tuple(m1, m2, std::move(m3)));
  current_size_ += _size;
}

void CheckpointGateway::CheckpointInfo::Clear() {
  for (auto &tupl : pending_tuples_) {
    if (std::get<0>(tupl)) {
      __global_protobuf_pool_release__(std::get<0>(tupl));
    } else if (std::get<1>(tupl)) {
      __global_protobuf_pool_release__(std::get<1>(tupl));
    } else {
      auto message = std::move(std::get<2>(tupl));
    }
  }

  pending_tuples_.clear();
  current_size_ = 0;
  checkpoint_id_ = "";
}
}  // namespace stmgr
}  // namespace heron
