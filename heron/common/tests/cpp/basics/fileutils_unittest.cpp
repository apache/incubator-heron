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
#include "basics/fileutils.h"
#include "basics/modinit.h"

TEST(FileUtilsTest, basename) {
  const std::string file1("/users/newuser/temp.file");
  EXPECT_EQ("temp.file", FileUtils::baseName(file1));

  const std::string file2("temp.file");
  EXPECT_EQ("temp.file", FileUtils::baseName(file2));

  const std::string file3("/users/newuser/tempfile");
  EXPECT_EQ("tempfile", FileUtils::baseName(file3));

  const std::string file4("tempfile");
  EXPECT_EQ("tempfile", FileUtils::baseName(file4));

  const std::string file5("newuser/tempfile");
  EXPECT_EQ("tempfile", FileUtils::baseName(file5));

  const std::string file6("");
  EXPECT_EQ("", FileUtils::baseName(file6));
}

int main(int argc, char **argv) {
  heron::common::Initialize(argv[0]);
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
