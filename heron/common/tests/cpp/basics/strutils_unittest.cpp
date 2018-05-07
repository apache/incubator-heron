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
#include "basics/strutils.h"
#include "basics/modinit.h"

TEST(StrUtilsTest, split) {
  std::vector<std::string> tokens;

  const std::string s1("how are you");
  tokens = StrUtils::split(s1, " ");
  EXPECT_EQ(static_cast<size_t>(3), tokens.size());
  EXPECT_EQ("how", tokens.at(0));
  EXPECT_EQ("are", tokens.at(1));
  EXPECT_EQ("you", tokens.at(2));

  const std::string s2("won\'t you be there");
  tokens = StrUtils::split(s2, " ");
  EXPECT_EQ(static_cast<size_t>(4), tokens.size());
  EXPECT_EQ("won\'t", tokens.at(0));
  EXPECT_EQ("you", tokens.at(1));
  EXPECT_EQ("be", tokens.at(2));
  EXPECT_EQ("there", tokens.at(3));

  const std::string s3("how");
  tokens = StrUtils::split(s3, " ");
  EXPECT_EQ(static_cast<size_t>(1), tokens.size());
  EXPECT_EQ("how", tokens.at(0));

  const std::string s4("");
  tokens = StrUtils::split(s4, " ");
  EXPECT_EQ(static_cast<size_t>(0), tokens.size());
}

int main(int argc, char **argv) {
  heron::common::Initialize(argv[0]);
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
