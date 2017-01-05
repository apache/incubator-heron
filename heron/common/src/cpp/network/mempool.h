/*
 * Copyright 2016 Twitter, Inc.
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
#ifndef MEM_POOL_H
#define MEM_POOL_H

#include <deque>
#include <typeindex>
#include <unordered_map>
#include "basics/sptypes.h"
#include "glog/logging.h"

template<typename T>
class BaseMemPool {
 public:
  static void set_limit(sp_int32 limit) {
    if (limit_ == 0)
      limit_ = limit;
  }

  BaseMemPool() {
  }

  ~BaseMemPool() {
    for (auto& p : pool_) {
      delete p;
    }
    pool_.clear();
  }

  template<typename S>
  S* acquire(S* unused) {
    LOG(INFO) << "acquire: size = " << size_;
    if (pool_.empty()) {
      return new S();
    }
    S* t = static_cast<S*>(pool_.back());
    pool_.pop_back();
    size_ -= sizeof(S);
    return t;
  }

  template<typename S>
  void release(S* t) {
    LOG(INFO) << "release: size = " << size_;
    if (limit_ == 0) {
      delete t;
      return;
    }
    if (size_ > limit_) {
      auto first = pool_.front();
      pool_.pop_front();
      size_ -= sizeof(S);
      delete first;
    }
    pool_.push_back(t);
    size_ += sizeof(S);
  }

 private:
  static sp_int32 limit_;
  static sp_int32 size_;
  std::deque<T*> pool_;
};


template<typename ValueType>
class MemPool {
 public:
  MemPool() {
  }

  ~MemPool() {
    map_.clear();
  }

  template<typename KeyType>
  KeyType* acquire(KeyType* m) {
    std::type_index type = typeid(KeyType);
    auto& pool = map_[type];
    return pool.acquire(m);
  }

  template<typename KeyType>
  void release(KeyType* ptr) {
    std::type_index type = typeid(KeyType);
    auto& pool = map_[type];
    pool.release(ptr);
  }

 private:
  std::unordered_map<std::type_index, BaseMemPool<ValueType>> map_;
};

#endif

