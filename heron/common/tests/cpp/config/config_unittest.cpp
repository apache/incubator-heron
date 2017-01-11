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
#include "config/config.h"

TEST(ConfigTest, builder) {
  heron::config::Config::Builder builder;
  builder.putstr("x", "y");

  heron::config::Config config = builder.build();
  EXPECT_EQ(config.getstr("x"), "y");
}

TEST(ConfigTest, boolean) {
  bool a = true;
  bool b = false;
  auto config = heron::config::Config::Builder()
      .putbool("a", a)
      .putbool("b", b)
      .build();
  EXPECT_EQ(config.getbool("a"), a);
  EXPECT_EQ(config.getbool("b"), b);
}

TEST(ConfigTest, integer64) {
  int64_t a = 123456789012345;
  auto config = heron::config::Config::Builder().putint64("a", a).build();
  EXPECT_EQ(config.getint64("a"), a);
}

TEST(ConfigTest, integer32) {
  int32_t a = 1234567890;
  auto config = heron::config::Config::Builder().putint32("a", a).build();
  EXPECT_EQ(config.getint32("a"), a);
}

TEST(ConfigTest, doublevalue) {
  double a = 1234567890.123450;
  auto config = heron::config::Config::Builder().putdouble("a", a).build();
  EXPECT_DOUBLE_EQ(config.getdouble("a"), a);
}

TEST(ConfigTest, expand) {
  heron::config::Config::Builder builder;
  builder.putstr(heron::config::CommonConfigVars::CLUSTER, "cluster1");
  builder.putstr(heron::config::CommonConfigVars::ROLE, "role1");
  builder.putstr(heron::config::CommonConfigVars::ENVIRON, "environ1");
  builder.putstr(heron::config::CommonConfigVars::TOPOLOGY_NAME, "topology1");
  builder.putstr("base_dir", ".herondata/${CLUSTER}/${ROLE}/${TOPOLOGY}");
  builder.putstr("another_dir", "/${CLUSTER}/${ROLE}/${TOPOLOGY}");
  builder.putstr("common_dir", "${CLUSTER}");
  builder.putstr("a", "a");
  builder.putint64("b", 123456);

  heron::config::Config config = builder.build().expand();

  EXPECT_EQ(config.getstr("base_dir"), ".herondata/cluster1/role1/topology1");
  EXPECT_EQ(config.getstr("another_dir"), "/cluster1/role1/topology1");
  EXPECT_EQ(config.getstr("common_dir"), "cluster1");
  EXPECT_EQ(config.getstr("a"), "a");
  EXPECT_EQ(config.getint64("b"), 123456);
}

int main(int argc, char **argv) {
  heron::common::Initialize(argv[0]);
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
