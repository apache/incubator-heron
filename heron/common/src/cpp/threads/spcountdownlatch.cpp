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

#include "threads/spcountdownlatch.h"

CountDownLatch::CountDownLatch(sp_uint32 count) : count_(count) {}

CountDownLatch::~CountDownLatch() {}

void CountDownLatch::wait(sp_uint32 target) {
  std::unique_lock<std::mutex> m(mutex_);
  // If count is greater than target, then wait until it is target.
  // Else return immediately.
  while (count_ > target) {
    cond_.wait(m);
  }
}

void CountDownLatch::countDown() {
  std::unique_lock<std::mutex> m(mutex_);

  // Nothing to do if count is already 0
  if (count_ == 0) return;

  // Decrement count.
  count_--;
  //  Notify all blocked threads: give them a chance to compare (count_ and target)
  cond_.notify_all();
}

sp_uint32 CountDownLatch::getCount() {
  std::unique_lock<std::mutex> m(mutex_);
  return count_;
}
