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

#ifndef MEM_POOL_H
#define MEM_POOL_H

#include <google/protobuf/message.h>
#include <vector>
#include <unordered_map>
#include <mutex>
#include <typeindex>
#include "basics/basics.h"

template<typename B>
class MemPool {
 public:
  MemPool() {
  }

  explicit MemPool(sp_int32 _pool_limit) :
    pool_limit_(_pool_limit) {
  }

  // TODO(cwang): we have a memory leak here.
  ~MemPool() {
    for (auto& map_iter : mem_pool_map_) {
      for (auto& mem_pool : map_iter.second) {
        delete mem_pool;
      }
      map_iter.second.clear();
    }
    mem_pool_map_.clear();
  }

  template<typename M>
  M* acquire(M* m) {
    std::type_index type = typeid(M);
    std::vector<B*>& pool = mem_pool_map_[type];

    if (pool.empty()) {
      return new M();
    }
    B* t = pool.back();
    pool.pop_back();
    return static_cast<M*>(t);
  }

  template<typename M>
  void release(M* ptr) {
    std::type_index type = typeid(M);
    sp_int32 size = mem_pool_map_[type].size();
    // if pool size reaches the limit, release the memory
    // otherwise put the memory into pool
    if (size >= pool_limit_) {
      delete ptr;
    } else {
      mem_pool_map_[type].push_back(static_cast<B*>(ptr));
    }
  }

  void set_pool_max_number_of_messages(sp_int32 _pool_limit) {
    pool_limit_ = _pool_limit;
  }

 private:
  // each type has its own separate mem pool entry
  std::unordered_map<std::type_index, std::vector<B*>> mem_pool_map_;
  // number of message in each mem pool should not exceed the pool_limit_
  sp_int32 pool_limit_;
};

extern MemPool<google::protobuf::Message>* __global_protobuf_pool__;
extern std::mutex __global_protobuf_pool_mutex__;

template<typename T>
T* __global_protobuf_pool_acquire__(T* _m) {
  std::lock_guard<std::mutex> guard(__global_protobuf_pool_mutex__);
  T* t = __global_protobuf_pool__->acquire(_m);
  t->Clear();
  return t;
}

template<typename T>
void __global_protobuf_pool_release__(T* _m) {
  std::lock_guard<std::mutex> guard(__global_protobuf_pool_mutex__);
  __global_protobuf_pool__->release(_m);
}

void __global_protobuf_pool_set_pool_max_number_of_messages__(sp_int32 _pool_limit);

#endif

