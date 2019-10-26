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

#ifndef SRC_CPP_SVCS_STMGR_SRC_UTIL_TUPLE_CACHE_H_
#define SRC_CPP_SVCS_STMGR_SRC_UTIL_TUPLE_CACHE_H_

#include <tsl/hopscotch_map.h>
#include <deque>
#include <vector>
#include <map>
#include "proto/messages.h"
#include "basics/basics.h"
#include "network/network.h"

namespace heron {
namespace stmgr {

class StMgr;

class TupleCache {
 public:
  TupleCache(std::shared_ptr<EventLoop> eventLoop, sp_uint32 _drain_threshold);
  virtual ~TupleCache();

  template <class T>
  void RegisterDrainer(void (T::*method)(sp_int32, proto::system::HeronTupleSet2*), T* _t) {
    tuple_drainer_ = std::bind(method, _t, std::placeholders::_1, std::placeholders::_2);
  }

  template <class T>
  void RegisterCheckpointDrainer(void (T::*method)(sp_int32,
             proto::ckptmgr::DownstreamStatefulCheckpoint*), T* _t) {
    checkpoint_drainer_ = std::bind(method, _t, std::placeholders::_1, std::placeholders::_2);
  }

  // returns tuple key
  sp_int64 add_data_tuple(sp_int32 _src_task_id,
                          sp_int32 _task_id, const proto::api::StreamId& _streamid,
                          proto::system::HeronDataTuple* _tuple);
  void add_ack_tuple(sp_int32 _src_task_id,
                     sp_int32 _task_id, const proto::system::AckTuple& _tuple);
  void add_fail_tuple(sp_int32 _src_task_id,
                      sp_int32 _task_id, const proto::system::AckTuple& _tuple);
  void add_emit_tuple(sp_int32 _src_task_id,
                      sp_int32 _task_id, const proto::system::AckTuple& _tuple);
  void add_checkpoint_tuple(sp_int32 _task_id,
                            proto::ckptmgr::DownstreamStatefulCheckpoint* _message);

  // Clear all data of all task_ids
  // This is different from the drain because while drain clears the messages
  // calling the drainer functions, this one just deletes the messages.
  virtual void clear();

 private:
  void drain(EventLoop::Status);
  void drain_impl();

  class TupleList {
    // not accessible to anyone else
    friend class TupleCache;

   private:
    TupleList();
    ~TupleList();

    sp_int64 add_data_tuple(sp_int32 _src_task_id,
                            const proto::api::StreamId& _streamid,
                            proto::system::HeronDataTuple* _tuple, sp_uint64* total_size_,
                            sp_uint64* _tuples_cache_max_tuple_size);
    void add_ack_tuple(sp_int32 _src_task_id,
                       const proto::system::AckTuple& _tuple, sp_uint64* total_size_);
    void add_fail_tuple(sp_int32 _src_task_id,
                        const proto::system::AckTuple& _tuple, sp_uint64* total_size_);
    void add_emit_tuple(sp_int32 _src_task_id,
                        const proto::system::AckTuple& _tuple, sp_uint64* total_size_);
    void add_checkpoint_tuple(proto::ckptmgr::DownstreamStatefulCheckpoint* _message,
                              sp_uint64* total_size_);

    void drain(sp_int32 _task_id,
               std::function<void(sp_int32, proto::system::HeronTupleSet2*)> _tuple_drainer,
               std::function<void(sp_int32,
               proto::ckptmgr::DownstreamStatefulCheckpoint*)> _checkpoint_drainer);

    proto::system::HeronTupleSet2* acquire_clean_set() {
     proto::system::HeronTupleSet2* set = nullptr;
     set = __global_protobuf_pool_acquire__(set);
     return set;
    }

    void clear();

   private:
    std::deque<google::protobuf::Message*> tuples_;
    proto::system::HeronTupleSet2* current_;
    sp_uint64 current_size_;
    sp_int32 last_drained_count_;
  };

  TupleList* get(sp_int32 _task_id);

  // map from task_id to the TupleList
  tsl::hopscotch_map<sp_int32, TupleList*> cache_;
  std::shared_ptr<EventLoop> eventLoop_;
  std::function<void(sp_int32, proto::system::HeronTupleSet2*)> tuple_drainer_;
  std::function<void(sp_int32, proto::ckptmgr::DownstreamStatefulCheckpoint*)>
                                  checkpoint_drainer_;
  sp_uint64 total_size_;
  sp_uint32 drain_threshold_bytes_;

  // Configs to be read
  sp_int32 cache_drain_frequency_ms_;
  sp_uint64 tuples_cache_max_tuple_size_;
};

}  // namespace stmgr
}  // namespace heron

#endif  // SRC_CPP_SVCS_STMGR_SRC_UTIL_TUPLE_CACHE_H_
