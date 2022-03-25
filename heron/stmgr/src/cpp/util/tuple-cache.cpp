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

#include "util/tuple-cache.h"
#include <iostream>
#include <map>
#include <string>
#include "proto/messages.h"
#include "basics/basics.h"
#include "errors/errors.h"
#include "threads/threads.h"
#include "network/network.h"
#include "config/heron-internals-config-reader.h"

namespace heron {
namespace stmgr {

TupleCache::TupleCache(std::shared_ptr<EventLoop> eventLoop, sp_uint32 _drain_threshold)
    : eventLoop_(eventLoop), drain_threshold_bytes_(_drain_threshold) {
  cache_drain_frequency_ms_ =
      config::HeronInternalsConfigReader::Instance()->GetHeronStreammgrCacheDrainFrequencyMs();
  // leave something for the headers
  tuples_cache_max_tuple_size_ =
    config::HeronInternalsConfigReader::Instance()->GetHeronStreammgrNetworkOptionsMaximumPacketMb()
      * 1_MB - 1024;

  total_size_ = 0;
  auto drain_cb = [this](EventLoop::Status status) { this->drain(status); };
  eventLoop_->registerTimer(std::move(drain_cb), true, cache_drain_frequency_ms_ * 1000);
}

TupleCache::~TupleCache() {
  // Clear the cache first
  clear();

  for (auto iter = cache_.begin(); iter != cache_.end(); ++iter) {
    delete iter->second;
  }
}

sp_int64 TupleCache::add_data_tuple(sp_int32 _src_task_id,
                                    sp_int32 _task_id, const proto::api::StreamId& _streamid,
                                    proto::system::HeronDataTuple* _tuple) {
  if (total_size_ >= drain_threshold_bytes_) drain_impl();
  TupleList* l = get(_task_id);
  return l->add_data_tuple(_src_task_id, _streamid, _tuple, &total_size_,
                           &tuples_cache_max_tuple_size_);
}

void TupleCache::add_ack_tuple(sp_int32 _src_task_id,
                               sp_int32 _task_id, const proto::system::AckTuple& _tuple) {
  if (total_size_ >= drain_threshold_bytes_) drain_impl();
  TupleList* l = get(_task_id);
  return l->add_ack_tuple(_src_task_id, _tuple, &total_size_);
}

void TupleCache::add_fail_tuple(sp_int32 _src_task_id,
                                sp_int32 _task_id, const proto::system::AckTuple& _tuple) {
  if (total_size_ >= drain_threshold_bytes_) drain_impl();
  TupleList* l = get(_task_id);
  return l->add_fail_tuple(_src_task_id, _tuple, &total_size_);
}

void TupleCache::add_emit_tuple(sp_int32 _src_task_id,
                                sp_int32 _task_id, const proto::system::AckTuple& _tuple) {
  if (total_size_ >= drain_threshold_bytes_) drain_impl();
  TupleList* l = get(_task_id);
  return l->add_emit_tuple(_src_task_id, _tuple, &total_size_);
}

void TupleCache::add_checkpoint_tuple(sp_int32 _task_id,
                                      proto::ckptmgr::DownstreamStatefulCheckpoint* _message) {
  if (total_size_ >= drain_threshold_bytes_) drain_impl();
  TupleList* l = get(_task_id);
  return l->add_checkpoint_tuple(_message, &total_size_);
}

TupleCache::TupleList* TupleCache::get(sp_int32 _task_id) {
  TupleList* l = NULL;
  auto iter = cache_.find(_task_id);
  if (iter == cache_.end()) {
    l = new TupleList();
    cache_[_task_id] = l;
  } else {
    l = iter->second;
  }
  return l;
}

void TupleCache::clear() {
  for (auto kv : cache_) {
    kv.second->clear();
    delete kv.second;
  }
  cache_.clear();
  total_size_ = 0;
}

void TupleCache::drain(EventLoop::Status) { drain_impl(); }

void TupleCache::drain_impl() {
  for (auto iter = cache_.begin(); iter != cache_.end(); ++iter) {
    iter->second->drain(iter->first, tuple_drainer_, checkpoint_drainer_);
  }
  total_size_ = 0;
}

TupleCache::TupleList::TupleList() {
  current_ = NULL;
  current_size_ = 0;
  last_drained_count_ = 0;
}

TupleCache::TupleList::~TupleList() {
  CHECK(tuples_.empty());
  CHECK(!current_);
}

void TupleCache::TupleList::clear() {
  if (current_) {
    __global_protobuf_pool_release__(current_);
    current_ = NULL;
  }
  while (!tuples_.empty()) {
    __global_protobuf_pool_release__(tuples_.front());
    tuples_.pop_front();
  }
  current_size_ = 0;
  last_drained_count_ = 0;
}

sp_int64 TupleCache::TupleList::add_data_tuple(sp_int32 _src_task_id,
                                               const proto::api::StreamId& _streamid,
                                               proto::system::HeronDataTuple* _tuple,
                                               sp_uint64* _total_size,
                                               sp_uint64* _tuples_cache_max_tuple_size) {
  if (!current_ || current_->has_control() || current_->src_task_id() != _src_task_id ||
      current_->data().stream().id() != _streamid.id() ||
      current_->data().stream().component_name() != _streamid.component_name() ||
      current_size_ > *_tuples_cache_max_tuple_size) {
    if (current_) {
      tuples_.push_front(current_);
    }
    current_ = acquire_clean_set();
    current_->mutable_data()->mutable_stream()->MergeFrom(_streamid);
    current_->set_src_task_id(_src_task_id);
    current_size_ = 0;
  }

  sp_int64 tuple_key = 0;
  if (_tuple->roots_size() > 0) {
     tuple_key = RandUtils::lrand();
  }
  // Override in place
  _tuple->set_key(tuple_key);

  std::string* added_tuple = current_->mutable_data()->add_tuples();
  _tuple->SerializePartialToString(added_tuple);

  sp_int64 tuple_size = _tuple->GetCachedSize();
  current_size_ += tuple_size;
  *_total_size += tuple_size;
  return tuple_key;
}

void TupleCache::TupleList::add_ack_tuple(sp_int32 _src_task_id,
                                          const proto::system::AckTuple& _tuple,
                                          sp_uint64* _total_size) {
  if (!current_ || current_->src_task_id() != _src_task_id ||
      current_->has_data() || current_->control().emits_size() > 0) {
    if (current_) {
      tuples_.push_front(current_);
    }
    current_ = acquire_clean_set();
    current_->set_src_task_id(_src_task_id);
    current_size_ = 0;
  }
  sp_int64 tuple_size = _tuple.ByteSizeLong();
  current_size_ += tuple_size;
  *_total_size += tuple_size;
  current_->mutable_control()->add_acks()->CopyFrom(_tuple);
}

void TupleCache::TupleList::add_fail_tuple(sp_int32 _src_task_id,
                                           const proto::system::AckTuple& _tuple,
                                           sp_uint64* _total_size) {
  if (!current_ || current_->src_task_id() != _src_task_id ||
      current_->has_data() || current_->control().emits_size() > 0) {
    if (current_) {
      tuples_.push_front(current_);
    }
    current_ = acquire_clean_set();
    current_->set_src_task_id(_src_task_id);
    current_size_ = 0;
  }
  sp_int64 tuple_size = _tuple.ByteSizeLong();
  current_size_ += tuple_size;
  *_total_size += tuple_size;
  current_->mutable_control()->add_fails()->CopyFrom(_tuple);
}

void TupleCache::TupleList::add_emit_tuple(sp_int32 _src_task_id,
                                           const proto::system::AckTuple& _tuple,
                                           sp_uint64* _total_size) {
  if (!current_ || current_->src_task_id() != _src_task_id ||
      current_->has_data() || current_->control().acks_size() > 0 ||
      current_->control().fails_size() > 0) {
    if (current_) {
      tuples_.push_front(current_);
    }
    current_ = acquire_clean_set();
    current_->set_src_task_id(_src_task_id);
    current_size_ = 0;
  }
  sp_int64 tuple_size = _tuple.ByteSizeLong();
  current_size_ += tuple_size;
  *_total_size += tuple_size;
  current_->mutable_control()->add_emits()->CopyFrom(_tuple);
}

void TupleCache::TupleList::add_checkpoint_tuple(
                 proto::ckptmgr::DownstreamStatefulCheckpoint* _message,
                 sp_uint64* _total_size) {
  if (current_) {
    tuples_.push_front(current_);
    current_ = NULL;
    current_size_ = 0;
  }
  sp_int64 tuple_size = _message->ByteSizeLong();
  *_total_size += tuple_size;
  tuples_.push_front(_message);
}

void TupleCache::TupleList::drain(
    sp_int32 _task_id, std::function<void(sp_int32, proto::system::HeronTupleSet2*)> _tuple_drainer,
    std::function<void(sp_int32, proto::ckptmgr::DownstreamStatefulCheckpoint*)>
     _checkpoint_drainer) {
  sp_int32 drained = 0;
  // we have to drain from back
  while (!tuples_.empty()) {
    if (tuples_.back()->GetTypeName() == "heron.proto.system.HeronTupleSet2") {
      auto t = static_cast<proto::system::HeronTupleSet2*>(tuples_.back());
      _tuple_drainer(_task_id, t);  // Drain cleans up the structure
    } else {
      auto t = static_cast<proto::ckptmgr::DownstreamStatefulCheckpoint*>(tuples_.back());
      _checkpoint_drainer(_task_id, t);  // Drain cleans up the structure
    }
    tuples_.pop_back();
    drained++;
  }
  if (current_ && drained == 0 && last_drained_count_ == 0) {
    // We didn;t drain anything last time. Better do it now
    // TODO(vikasr) : Add metric
    _tuple_drainer(_task_id, current_);
    drained++;
    current_ = NULL;
    current_size_ = 0;
  }
  last_drained_count_ = drained;
}
}  // namespace stmgr
}  // namespace heron
