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

#include <mutex>
#include <condition_variable>
#include <chrono>
#include "basics/sptypes.h"

#ifndef SRC_CPP_CORE_THREADS_PUBLIC_SPCOUNTDOWNLATCH_H_
#define SRC_CPP_CORE_THREADS_PUBLIC_SPCOUNTDOWNLATCH_H_

/*
 * CountDownLatch is similar to java.util.concurrent.CountDownLatch.
 * It is a synchronization aid that allows one or more threads
 * to wait until a set of operations being performed in other threads completes
 *
 * A CountDownLatch is initialized with a given count. The await methods block
 * until the current count reaches zero due to invocations of the countDown()
 * method, after which all waiting threads are released and any subsequent
 * invocations of await return immediately. This is a one-shot phenomenon,
 * the count cannot be reset.
 */
class CountDownLatch {
 public:
  explicit CountDownLatch(sp_uint32 count);
  virtual ~CountDownLatch();

  CountDownLatch(const CountDownLatch& latch) = delete;
  CountDownLatch& operator=(const CountDownLatch& latch) = delete;

  // Causes the current thread to wait until the latch has counted down to target
  bool wait(sp_uint32 target = 0, const std::chrono::seconds& d = std::chrono::seconds::zero());

  // Decrements the count of the latch, releasing all waiting threads if the
  // count reaches zero
  void countDown();

  // returns the current count
  sp_uint32 getCount();

 private:
  std::mutex mutex_;
  std::condition_variable cond_;
  sp_uint32 count_;
};

#endif  // SRC_CPP_CORE_THREADS_PUBLIC_SPCOUNTDOWNLATCH_H_
