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

////////////////////////////////////////////////////////////////////
//
// This file consists of the implementation of a producer consumer
// queue.
/////////////////////////////////////////////////////////////////////
#include "threads/pcqueue.h"
#include <vector>

void PCQueue::enqueue(void* _item) {
  std::unique_lock<std::mutex> m(mutex_);
  queue_.push(_item);

  cond_.notify_one();
}

void PCQueue::enqueue_all(void* _item, sp_int32 _ntimes) {
  std::unique_lock<std::mutex> m(mutex_);

  for (sp_int32 i = 0; i < _ntimes; i++) {
    queue_.push(_item);
  }

  cond_.notify_one();
}

void* PCQueue::dequeue() {
  std::unique_lock<std::mutex> m(mutex_);

  while (queue_.empty()) cond_.wait(m);

  void* item = queue_.front();
  queue_.pop();
  return item;
}

void* PCQueue::trydequeue(bool& _dequeued) {
  std::unique_lock<std::mutex> m(mutex_);
  if (queue_.empty()) {
    _dequeued = false;
    return NULL;
  }
  void* item = queue_.front();
  queue_.pop();
  _dequeued = true;
  return item;
}

sp_uint32 PCQueue::trydequeuen(sp_uint32 _ntodequeue, std::vector<void*>& _retval) {
  std::unique_lock<std::mutex> m(mutex_);

  sp_uint32 dequeued = 0;
  while (!queue_.empty() && dequeued < _ntodequeue) {
    void* item = queue_.front();
    queue_.pop();
    _retval.push_back(item);
    dequeued++;
  }
  return dequeued;
}

sp_int32 PCQueue::size(void) {
  std::unique_lock<std::mutex> m(mutex_);
  sp_int32 retval = queue_.size();
  return retval;
}
