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

#include "threads/spcountdownlatch.h"

#include <iostream>
#include <string>
#include <thread>

#include "gtest/gtest.h"
#include "basics/basics.h"
#include "errors/errors.h"

#include "basics/modinit.h"
#include "errors/modinit.h"

void testCountDownFunc(CountDownLatch& latch) { latch.countDown(); }

void testWaitFunc(CountDownLatch& latch) { latch.wait(); }
void testWaitFuncTrue(CountDownLatch& latch) {
  EXPECT_TRUE(latch.wait(1, std::chrono::seconds(2)));
}
void testWaitFuncFalse(CountDownLatch& latch) {
  EXPECT_FALSE(latch.wait(1, std::chrono::seconds(1)));
}

TEST(CountDownLatchTest, testCountDownMethod) {
  CountDownLatch latch(2);
  latch.countDown();

  // countDown should decrement the value
  EXPECT_EQ(latch.getCount(), static_cast<sp_uint32>(1));
}

TEST(CountDownLatchTest, testWaitMethod) {
  CountDownLatch latch(1);

  std::thread t1(testWaitFunc, std::ref(latch));
  latch.countDown();

  // the wait method should see the countDown and release the blocked thread
  t1.join();
  EXPECT_EQ(latch.getCount(), static_cast<sp_uint32>(0));
}

TEST(CountDownLatchTest, testWaitMethod1) {
  CountDownLatch latch(2);

  std::thread t1(testWaitFuncTrue, std::ref(latch));
  latch.countDown();

  // the wait method should see the countDown and release the blocked thread
  t1.join();
  EXPECT_EQ(latch.getCount(), static_cast<sp_uint32>(1));
}

TEST(CountDownLatchTest, testWaitMethod2) {
  CountDownLatch latch(2);

  std::thread t1(testWaitFuncFalse, std::ref(latch));
  std::this_thread::sleep_for(std::chrono::seconds(2));
  latch.countDown();

  // the wait method should see the countDown and release the blocked thread
  t1.join();
  EXPECT_EQ(latch.getCount(), static_cast<sp_uint32>(1));
}

TEST(CountDownLatchTest, testWaitNotifyWithTwoThreads) {
  CountDownLatch latch(1);

  std::thread t1(testWaitFunc, std::ref(latch));
  std::thread t2(testCountDownFunc, std::ref(latch));

  t2.join();
  t1.join();

  // If the test reached here, then it has passed
  EXPECT_EQ(latch.getCount(), static_cast<sp_uint32>(0));
}

// Same test as above, but coundDown is called before wait
TEST(CountDownLatchTest, testWaitNotifyWithTwoThreads2) {
  CountDownLatch latch(1);
  std::thread t2(testCountDownFunc, std::ref(latch));

  // ensures that countdown is called first
  t2.join();

  // the wait thread should not block at this point
  std::thread t1(testWaitFunc, std::ref(latch));
  t1.join();

  // If the test reached here, then it has passed
  EXPECT_EQ(latch.getCount(), static_cast<sp_uint32>(0));
}

TEST(CountDownLatchTest, testWithMultipleThreads) {
  CountDownLatch latch(2);

  std::thread t1(testWaitFunc, std::ref(latch));
  std::thread t2(testWaitFunc, std::ref(latch));

  std::thread t3(testCountDownFunc, std::ref(latch));
  std::thread t4(testCountDownFunc, std::ref(latch));

  // wait for both threads to call countdown
  t3.join();
  t4.join();

  // the two waiting threads should be unblocked now
  t1.join();
  t2.join();

  // If the test reached here, then it has passed
  EXPECT_EQ(latch.getCount(), static_cast<sp_uint32>(0));
}

int main(int argc, char** argv) {
  heron::common::Initialize(argv[0]);
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
