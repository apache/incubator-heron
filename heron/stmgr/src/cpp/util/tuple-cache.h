/*
 * Copyright 2015 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef SRC_CPP_SVCS_STMGR_SRC_UTIL_TUPLE_CACHE_H_
#define SRC_CPP_SVCS_STMGR_SRC_UTIL_TUPLE_CACHE_H_

#include <deque>
#include <vector>
#include <map>
#include "proto/messages.h"
#include "basics/basics.h"
#include "network/network.h"
#include "network/mempool.h"

namespace heron {
namespace stmgr {

class StMgr;

class TupleCache {
 public:
  TupleCache(EventLoop* eventLoop, sp_uint32 _drain_threshold);
  virtual ~TupleCache();

  template <class T>
  void RegisterDrainer(void (T::*method)(sp_int32, proto::system::HeronTupleSet2*), T* _t) {
    drainer_ = std::bind(method, _t, std::placeholders::_1, std::placeholders::_2);
  }

  // returns tuple key
  sp_int64 add_data_tuple(sp_int32 _task_id, const proto::api::StreamId& _streamid,
                          proto::system::HeronDataTuple* _tuple);
  void add_ack_tuple(sp_int32 _task_id, const proto::system::AckTuple& _tuple);
  void add_fail_tuple(sp_int32 _task_id, const proto::system::AckTuple& _tuple);
  void add_emit_tuple(sp_int32 _task_id, const proto::system::AckTuple& _tuple);

  void release(sp_int32 _task_id, proto::system::HeronTupleSet2* set) {
    get(_task_id)->release(set);
  }

 private:
  void drain(EventLoop::Status);
  void drain_impl();

  class TupleList {
    // not accessible to anyone else
    friend class TupleCache;

   private:
    TupleList();
    ~TupleList();

    sp_int64 add_data_tuple(const proto::api::StreamId& _streamid,
                            proto::system::HeronDataTuple* _tuple, sp_uint64* total_size_,
                            sp_uint64* _tuples_cache_max_tuple_size);
    void add_ack_tuple(const proto::system::AckTuple& _tuple, sp_uint64* total_size_);
    void add_fail_tuple(const proto::system::AckTuple& _tuple, sp_uint64* total_size_);
    void add_emit_tuple(const proto::system::AckTuple& _tuple, sp_uint64* total_size_);

    void drain(sp_int32 _task_id,
               std::function<void(sp_int32, proto::system::HeronTupleSet2*)> _drainer);

    proto::system::HeronTupleSet2* acquire() {
      return heron_tuple_set_pool_.acquire();
    }

    proto::system::HeronTupleSet2* acquire_clean_set() {
     proto::system::HeronTupleSet2* set = acquire();
     set->Clear();
     return set;
    }

    void release(proto::system::HeronTupleSet2* set) {
      heron_tuple_set_pool_.release(set);
    }

   private:
    BaseMemPool<proto::system::HeronTupleSet2> heron_tuple_set_pool_;
    std::deque<proto::system::HeronTupleSet2*> tuples_;
    proto::system::HeronTupleSet2* current_;
    sp_uint64 current_size_;
    sp_int32 last_drained_count_;
  };

  TupleList* get(sp_int32 _task_id);

  // map from task_id to the TupleList
  std::map<sp_int32, TupleList*> cache_;
  EventLoop* eventLoop_;
  std::function<void(sp_int32, proto::system::HeronTupleSet2*)> drainer_;
  sp_uint64 total_size_;
  sp_uint32 drain_threshold_bytes_;

  // Configs to be read
  sp_int32 cache_drain_frequency_ms_;
  sp_uint64 tuples_cache_max_tuple_size_;
};

}  // namespace stmgr
}  // namespace heron

#endif  // SRC_CPP_SVCS_STMGR_SRC_UTIL_TUPLE_CACHE_H_
