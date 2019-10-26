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

#include <thread>
#include "gtest/gtest.h"
#include "basics/basics.h"
#include "errors/errors.h"
#include "threads/threads.h"
#include "network/network.h"
#include "basics/modinit.h"
#include "errors/modinit.h"
#include "threads/modinit.h"
#include "network/modinit.h"

struct Resource {
  sp_int32 count;
};

void RunningEventLoop(std::shared_ptr<EventLoopImpl> ss) {
  ss->registerTimer([](EventLoop::Status status) { /* do nothing */ }, true, 1 * 1000 * 1000);
  ss->loop();
}

TEST(PiperTest, test_piper) {
  auto eventLoop = std::make_shared<EventLoopImpl>();
  std::thread* thread = new std::thread(RunningEventLoop, eventLoop);

  Piper* piper = new Piper(eventLoop);
  Resource* resource = new Resource();
  resource->count = 0;

  auto cb = [resource](){ resource->count++; };

  piper->ExecuteInEventLoop(cb);
  sleep(1);
  EXPECT_EQ(resource->count, 1);

  piper->ExecuteInEventLoop(cb);
  sleep(1);
  EXPECT_EQ(resource->count, 2);

  eventLoop->loopExit();

  // Wait for the thread to terminate
  thread->join();

  delete piper;
  delete resource;
  delete thread;
}

int main(int argc, char **argv) {
  heron::common::Initialize(argv[0]);
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
