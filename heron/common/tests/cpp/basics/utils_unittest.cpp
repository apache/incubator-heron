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

#include <string>
#include <vector>
#include "gtest/gtest.h"
#include "basics/basics.h"
#include "basics/modinit.h"

TEST(UtilTest, itos_int) {
  std::string value = std::to_string(1234567890);
  EXPECT_STREQ("1234567890", value.c_str());

  value = std::to_string(-1234567890);
  EXPECT_STREQ("-1234567890", value.c_str());
}

TEST(UtilTest, itos_uint) {
  std::string value = std::to_string(1234567890);
  EXPECT_STREQ("1234567890", value.c_str());

  value = std::to_string(-1234567890);
  EXPECT_STREQ("-1234567890", value.c_str());
}

int main(int argc, char **argv) {
  heron::common::Initialize(argv[0]);
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
