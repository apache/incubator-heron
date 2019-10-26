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

#include <vector>
#include "gtest/gtest.h"
#include "proto/messages.h"
#include "basics/basics.h"
#include "errors/errors.h"
#include "threads/threads.h"
#include "network/network.h"
#include "basics/modinit.h"
#include "errors/modinit.h"
#include "threads/modinit.h"
#include "network/modinit.h"
#include "config/heron-internals-config-reader.h"
#include "util/xor-manager.h"

sp_string heron_internals_config_filename =
    "../../../../../../../../heron/config/heron_internals.yaml";

std::shared_ptr<EventLoopImpl> ss = std::make_shared<EventLoopImpl>();

void OneTimer(heron::stmgr::XorManager* _g, std::vector<sp_int64>* _task_ids,
              EventLoopImpl::Status) {
  for (sp_int32 j = 0; j < 99; ++j) {
    EXPECT_EQ(_g->anchor(1, 1, (*_task_ids)[j]), false);
  }

  EXPECT_EQ(_g->anchor(1, 1, (*_task_ids)[99]), true);
  EXPECT_EQ(_g->remove(1, 1), true);

  delete _task_ids;
}

void TwoTimer(heron::stmgr::XorManager* _g, std::vector<sp_int64>* _task_ids,
              EventLoopImpl::Status) {
  for (sp_int32 j = 0; j < 100; ++j) {
    EXPECT_EQ(_g->anchor(2, 1, (*_task_ids)[j]), false);
  }

  EXPECT_EQ(_g->remove(2, 1), false);

  delete _task_ids;
  ss->loopExit();
}

// Test the anchoring xoring logic and removes
TEST(XorManager, test_all) {
  std::vector<sp_int32> task_ids;
  task_ids.push_back(1);
  task_ids.push_back(2);
  heron::stmgr::XorManager* g = new heron::stmgr::XorManager(ss, 1, task_ids);

  // Create some items
  for (sp_int32 i = 0; i < 100; ++i) {
    g->create(1, i, 1);
  }

  // basic Anchor works
  for (sp_int32 i = 0; i < 100; ++i) {
    EXPECT_EQ(g->anchor(1, i, 1), true);
    EXPECT_EQ(g->remove(1, i), true);
  }

  // layered anchoring
  std::vector<sp_int64> things_added;
  sp_int64 first_key = RandUtils::lrand();
  g->create(1, 1, first_key);
  things_added.push_back(first_key);
  for (sp_int32 j = 1; j < 100; ++j) {
    sp_int64 key = RandUtils::lrand();
    things_added.push_back(key);
    EXPECT_EQ(g->anchor(1, 1, key), false);
  }

  // xor ing works
  for (sp_int32 j = 0; j < 99; ++j) {
    EXPECT_EQ(g->anchor(1, 1, things_added[j]), false);
  }

  EXPECT_EQ(g->anchor(1, 1, things_added[99]), true);
  EXPECT_EQ(g->remove(1, 1), true);

  // Same test with some rotation
  std::vector<sp_int64>* one_added = new std::vector<sp_int64>();
  first_key = RandUtils::lrand();
  g->create(1, 1, first_key);
  one_added->push_back(first_key);
  for (sp_int32 j = 1; j < 100; ++j) {
    sp_int64 key = RandUtils::lrand();
    one_added->push_back(key);
    EXPECT_EQ(g->anchor(1, 1, key), false);
  }

  auto cb1 = [g, one_added](EventLoopImpl::Status status) {
    OneTimer(g, one_added, status);
  };

  // register timer after 1.5 seconds to allow rotation to happen
  ss->registerTimer(std::move(cb1), false, 1500000);

  // Same test with too much rotation
  std::vector<sp_int64>* two_added = new std::vector<sp_int64>();
  first_key = RandUtils::lrand();
  g->create(2, 1, first_key);
  two_added->push_back(first_key);
  for (sp_int32 j = 1; j < 100; ++j) {
    sp_int64 key = RandUtils::lrand();
    two_added->push_back(key);
    EXPECT_EQ(g->anchor(2, 1, key), false);
  }

  auto cb2 = [g, two_added](EventLoopImpl::Status status) {
    TwoTimer(g, two_added, status);
  };

  // register timer after 5 seconds to allow
  // multiple rotation to happen
  ss->registerTimer(std::move(cb2), false, 4000000);
  ss->loop();

  delete g;
}

int main(int argc, char** argv) {
  heron::common::Initialize(argv[0]);
  testing::InitGoogleTest(&argc, argv);
  if (argc > 1) {
    std::cerr << "Using config file " << argv[1] << std::endl;
    heron_internals_config_filename = argv[1];
  }

  // Create the sington for heron_internals_config_reader, if it does not exist
  if (!heron::config::HeronInternalsConfigReader::Exists()) {
    heron::config::HeronInternalsConfigReader::Create(heron_internals_config_filename, "");
  }
  return RUN_ALL_TESTS();
}
