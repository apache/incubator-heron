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
// This file consists of ciass definition of a producer consumer queue
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

#endif
