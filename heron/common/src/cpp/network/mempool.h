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

#include <list>
#include <vector>
#include <unordered_map>
#include <typeindex>

template<typename T>
class BaseMemPool {
 public:
  template<class... Args>
  T* acquire(Args&&... args) {
    if (pool_.empty()) {
      return new T(std::forward<Args>(args)...);
    }
    T* t = pool_.back();
    pool_.pop_back();
    return t;
  }
  void release(T* t) {
    pool_.push_back(t);
  }
  BaseMemPool() {
  }
  ~BaseMemPool() {
      for (auto& p : pool_) {
        delete p;
      }
      pool_.clear();
  }
 private:
  std::vector<T*> pool_;
};


template<typename B>
class MemPool {
 public:
  MemPool() : size_(0), size_limit_(0) {
  }

  // TODO(cwang): we have a memory leak here.
  ~MemPool() {
    for (auto& m : map_) {
      for (auto& n : m.second) {
        delete n;
      }
      m.second.clear();
    }
    map_.clear();
  }

  void set_limit(sp_int32 limit) {
    size_limit_ = limit;
  }

  template<typename M>
  M* acquire(M* m) {
    std::type_index type = typeid(M);
    auto& pool = map_[type];

    if (pool.empty()) {
      return new M();
    }
    B* t = pool.back();
    pool.pop_back();
    size_ -= sizeof(M);
    return static_cast<M*>(t);
  }

  template<typename M>
  void release(M* ptr) {
    if (size_limit_ == 0) {
      delete ptr;
      return;
    }

    std::type_index type = typeid(M);
    auto& pool = map_[type];
    if (size_ > size_limit_) {
      auto first = pool.front();
      pool.pop_front();
      size_ -= sizeof(*first);
      delete first;
    }
    pool.push_back(static_cast<B*>(ptr));
    size_ += sizeof(M);
  }

 private:
  sp_int32 size_;
  sp_int32 size_limit_;
  std::unordered_map<std::type_index, std::list<B*>> map_;
};

#endif

