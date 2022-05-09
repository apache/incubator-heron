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

#include <map>
#include <sstream>
#include <thread>
#include <vector>
#include "gtest/gtest.h"
#include "basics/basics.h"
#include "manager/tcontroller.h"


// Test to make sure that ParseRuntimeConfig works correctly
TEST(TController, test_parse_runtime_config) {
  std::vector<sp_string> parameters;
  parameters.push_back("test:1");
  parameters.push_back("testabc:abc");
  parameters.push_back("spout:test:2");
  parameters.push_back("bolt:test:3");

  std::map<sp_string, std::map<sp_string, sp_string>> conf;
  heron::tmanager::TController::ParseRuntimeConfig(parameters, conf);

  EXPECT_EQ(conf.size(), 3);
  EXPECT_EQ(conf["_topology_"]["test"], "1");
  EXPECT_EQ(conf["_topology_"]["testabc"], "abc");
  EXPECT_EQ(conf["spout"]["test"], "2");
  EXPECT_EQ(conf["bolt"]["test"], "3");
}

int main(int argc, char** argv) {
  heron::common::Initialize(argv[0]);
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
