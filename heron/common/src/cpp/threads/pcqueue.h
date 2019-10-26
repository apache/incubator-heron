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

////////////////////////////////////////////////////////////////////
//
// This file consists of class definition of a producer consumer queue
//
/////////////////////////////////////////////////////////////////////

#if !defined(__PC_QUEUE_H)
#define __PC_QUEUE_H

#include <queue>
#include <vector>
#include <mutex>
#include <condition_variable>
#include "basics/sptypes.h"

template <typename T>
class PCQueue {
 public:
  PCQueue() {}
  virtual ~PCQueue() throw() {}

  PCQueue(const PCQueue& pcqueue) = delete;
  PCQueue& operator=(const PCQueue& pcqueue) = delete;

  void enqueue(T _item);
  void enqueue_all(T _item, sp_int32 _ntimes);

  T dequeue(void);
  T trydequeue(bool&);
  sp_uint32 trydequeuen(sp_uint32 _n, std::vector<T>& _queue);

  sp_int32 size(void);

 protected:
  std::queue<T> queue_;

  std::condition_variable cond_;
  std::mutex mutex_;
};

template <typename T>
void PCQueue<T>::enqueue(T _item) {
  std::unique_lock<std::mutex> m(mutex_);
  queue_.push(std::move(_item));

  cond_.notify_one();
}

template <typename T>
void PCQueue<T>::enqueue_all(T _item, sp_int32 _ntimes) {
  std::unique_lock<std::mutex> m(mutex_);

  for (sp_int32 i = 0; i < _ntimes; i++) {
    queue_.push(std::move(_item));
  }

  cond_.notify_one();
}

template <typename T>
T PCQueue<T>::dequeue() {
  std::unique_lock<std::mutex> m(mutex_);

  while (queue_.empty()) cond_.wait(m);

  T item = std::move(queue_.front());
  queue_.pop();
  return item;
}

template <typename T>
T PCQueue<T>::trydequeue(bool& _dequeued) {
  std::unique_lock<std::mutex> m(mutex_);

  if (queue_.empty()) {
    _dequeued = false;
    return nullptr;
  }

  T item = std::move(queue_.front());

  queue_.pop();
  _dequeued = true;

  return item;
}

template <typename T>
sp_uint32 PCQueue<T>::trydequeuen(sp_uint32 _ntodequeue, std::vector<T>& _retval) {
  std::unique_lock<std::mutex> m(mutex_);

  sp_uint32 dequeued = 0;

  while (!queue_.empty() && dequeued < _ntodequeue) {
    T item = std::move(queue_.front());
    queue_.pop();
    _retval.push_back(std::move(item));
    dequeued++;
  }

  return dequeued;
}

template <typename T>
sp_int32 PCQueue<T>::size(void) {
  std::unique_lock<std::mutex> m(mutex_);
  sp_int32 retval = queue_.size();
  return retval;
}

#endif
