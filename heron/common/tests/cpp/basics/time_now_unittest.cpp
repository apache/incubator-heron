/*
 * Copyright 2015 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/

#include <vector> licenses/LICENSE-2.0 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <sys/time.h>
#include <string>
#include <vector>
#include "gtest/gtest.h"
#include "basics/basics.h"
#include "basics/modinit.h"

TEST(Time_Now_Test, test_now_time) {
  struct timeval t;

  if (gettimeofday(&t, NULL) != 0) {
    ADD_FAILURE();
    return;
  }

  // Convert seconds and microseconds to seconds
  sp_double64 nowd = t.tv_sec * US_SECOND + t.tv_usec;
  EXPECT_NEAR(nowd, sp_time::now().usecs(), 10);
}

int main(int argc, char **argv) {
  heron::common::Initialize(argv[0]);
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
