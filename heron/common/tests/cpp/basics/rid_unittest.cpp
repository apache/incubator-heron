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

#include <string>
#include <vector>
#include "gtest/gtest.h"
#include "basics/basics.h"
#include "basics/modinit.h"

TEST(RIDTest, zero_rid) {
  REQID a;
  std::string v(REQID_size, 0);
  EXPECT_EQ(a.str(), v);
}

TEST(RIDTest, itos_int) {
  REQID a, b;
  REQID_Generator g;
  std::string v(REQID_size, 0);

  EXPECT_EQ(REQID_size, a.length());
  EXPECT_EQ(a.str(), v);

  // check generation
  b = g.generate();
  EXPECT_EQ(REQID_size, b.length());
  EXPECT_NE(v, b.str());

  // check assignment
  a = b;
  EXPECT_STREQ(b.c_str(), a.c_str());

  // check equality
  ASSERT_TRUE(a == b);

  // check assign function
  std::string d(REQID_size, 'a');
  a.assign(std::string(REQID_size, 'a'));
  EXPECT_EQ(d, a.str());

  // check non equality
  ASSERT_TRUE(a != b);
}

int main(int argc, char **argv) {
  heron::common::Initialize(argv[0]);
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
