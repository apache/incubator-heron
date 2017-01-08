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
#include "config/parameters.h"

TEST(ParamsTest, builder) {
  heron::config::Parameters::Builder builder;
  builder.putstr("x", "y");

  heron::config::Parameters params = builder.build();
  EXPECT_EQ(params.getstr("x"), "y");
}

TEST(ParamsTest, boolean) {
  bool a = true;
  bool b = false;
  auto params = heron::config::Parameters::Builder()
      .putbool("a", a)
      .putbool("b", b)
      .build();
  EXPECT_EQ(params.getbool("a"), a);
  EXPECT_EQ(params.getbool("b"), b);
}

TEST(ParamsTest, integer64) {
  int64_t a = 123456789012345;
  auto params = heron::config::Parameters::Builder().putint64("a", a).build();
  EXPECT_EQ(params.getint64("a"), a);
}

TEST(ParamsTest, integer32) {
  int32_t a = 1234567890;
  auto params = heron::config::Parameters::Builder().putint32("a", a).build();
  EXPECT_EQ(params.getint32("a"), a);
}

TEST(ParamsTest, doublevalue) {
  double a = 1234567890.123450;
  auto params = heron::config::Parameters::Builder().putdouble("a", a).build();
  EXPECT_DOUBLE_EQ(params.getdouble("a"), a);
}

TEST(ParamsTest, pointer) {
  int32_t a = 34;
  auto params = heron::config::Parameters::Builder().putptr("a", &a).build();

  EXPECT_EQ(params.getptr("a"), &a);

  int32_t b = *reinterpret_cast<int *>(params.getptr("a"));
  EXPECT_EQ(a, b);
}

int main(int argc, char **argv) {
  heron::common::Initialize(argv[0]);
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
